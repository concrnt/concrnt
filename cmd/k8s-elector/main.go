// k8s-elector is a sidecar that provides cluster coordination to concrnt on
// Kubernetes: leader election via a coordination.k8s.io Lease and peer
// discovery via a headless service. concrnt itself stays free of any
// Kubernetes dependency by only speaking this sidecar's small HTTP protocol:
//
//	GET /status -> {"isLeader": bool, "leaderUrl": string, "peers": [string]}
//	GET /health -> 200
//
// Any service that implements the same protocol (e.g. backed by consul, etcd
// or redis) can replace this sidecar.
//
// Configuration (environment variables):
//
//	ADVERTISE_URL    (required) this replica's concrnt coordination base URL,
//	                 e.g. http://$(POD_IP):8003 — stored as the lease holder
//	                 identity so peers learn the leader's address
//	HEADLESS_SERVICE (required) DNS name resolving to every replica
//	PEER_PORT        concrnt coordination port for peer URLs (default 8003)
//	LEASE_NAME       lease object name (default concrnt-leader)
//	LISTEN           listen address (default :8002)
//	POD_NAMESPACE    lease namespace (default: serviceaccount namespace file)
//	POD_NAME         used for logging only (default: hostname)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/concrnt/concrnt/internal/infra/cluster"
)

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 2 * time.Second

	// how long a stale peer list may be served while DNS resolution fails;
	// past this, /status returns 503 so consumers keep their own last-known
	// state instead of acting on a false "zero replicas"
	peerResolveGrace = 30 * time.Second
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	advertiseURL := os.Getenv("ADVERTISE_URL")
	if advertiseURL == "" {
		slog.Error("ADVERTISE_URL is not set")
		os.Exit(1)
	}

	headlessService := os.Getenv("HEADLESS_SERVICE")
	if headlessService == "" {
		slog.Error("HEADLESS_SERVICE is not set")
		os.Exit(1)
	}

	peerPort, err := strconv.Atoi(getenv("PEER_PORT", "8003"))
	if err != nil {
		slog.Error("invalid PEER_PORT", slog.String("error", err.Error()))
		os.Exit(1)
	}

	leaseName := getenv("LEASE_NAME", "concrnt-leader")
	listen := getenv("LISTEN", ":8002")

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err != nil {
			slog.Error("POD_NAMESPACE is not set and namespace file is unreadable", slog.String("error", err.Error()))
			os.Exit(1)
		}
		namespace = strings.TrimSpace(string(data))
	}

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		podName, _ = os.Hostname()
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		slog.Error("failed to load in-cluster config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("failed to create kubernetes client", slog.String("error", err.Error()))
		os.Exit(1)
	}

	slog.Info(
		"k8s-elector starting",
		slog.String("pod", podName),
		slog.String("namespace", namespace),
		slog.String("lease", leaseName),
		slog.String("advertise", advertiseURL),
	)

	var elector atomic.Pointer[leaderelection.LeaderElector]

	electionDone := make(chan struct{})
	go func() {
		defer close(electionDone)
		// returns on ctx cancellation or when leadership is lost; loop to
		// re-candidate
		for ctx.Err() == nil {
			lock := &resourcelock.LeaseLock{
				LeaseMeta: metav1.ObjectMeta{
					Name:      leaseName,
					Namespace: namespace,
				},
				Client: clientset.CoordinationV1(),
				LockConfig: resourcelock.ResourceLockConfig{
					Identity: advertiseURL,
				},
			}

			le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
				Lock:            lock,
				LeaseDuration:   leaseDuration,
				RenewDeadline:   renewDeadline,
				RetryPeriod:     retryPeriod,
				ReleaseOnCancel: true,
				Callbacks: leaderelection.LeaderCallbacks{
					OnStartedLeading: func(context.Context) {
						slog.Info("acquired leadership", slog.String("pod", podName))
					},
					OnStoppedLeading: func() {
						slog.Info("lost leadership", slog.String("pod", podName))
					},
					OnNewLeader: func(identity string) {
						slog.Info("leader observed", slog.String("leader", identity))
					},
				},
			})
			if err != nil {
				slog.Error("failed to create leader elector", slog.String("error", err.Error()))
				os.Exit(1)
			}

			elector.Store(le)
			le.Run(ctx)
		}
	}()

	// last successful headless-service resolution, served during brief DNS
	// blips so a flaky resolver doesn't read as a shrinking cluster
	var peersMu sync.Mutex
	var lastPeers []string
	var lastPeersAt time.Time

	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		// cluster.ElectorStatus is the wire contract HTTPElector decodes;
		// sharing the type keeps the two sides in lockstep
		status := cluster.ElectorStatus{Peers: []string{}}

		if le := elector.Load(); le != nil {
			status.IsLeader = le.IsLeader()
			status.LeaderURL = le.GetLeader()
		}

		addrs, err := net.DefaultResolver.LookupIPAddr(r.Context(), headlessService)
		if err != nil {
			// an empty peer list with 200 would be trusted as "zero replicas"
			// and tear down every cross-replica subscription; serve the last
			// known list briefly, then fail the request outright
			peersMu.Lock()
			stalePeers := lastPeers
			staleAt := lastPeersAt
			peersMu.Unlock()

			if stalePeers == nil || time.Since(staleAt) > peerResolveGrace {
				slog.Error("failed to resolve headless service", slog.String("error", err.Error()))
				http.Error(w, "failed to resolve headless service", http.StatusServiceUnavailable)
				return
			}

			slog.Warn("failed to resolve headless service, serving last known peers", slog.String("error", err.Error()))
			status.Peers = stalePeers
		} else {
			for _, addr := range addrs {
				status.Peers = append(status.Peers, "http://"+net.JoinHostPort(addr.IP.String(), strconv.Itoa(peerPort)))
			}
			peersMu.Lock()
			lastPeers = status.Peers
			lastPeersAt = time.Now()
			peersMu.Unlock()
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})

	server := &http.Server{Addr: listen, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server stopped unexpectedly", slog.String("error", err.Error()))
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(shutdownCtx)

	// wait for ReleaseOnCancel to give up the lease so the next leader takes
	// over immediately
	select {
	case <-electionDone:
	case <-time.After(5 * time.Second):
	}
}

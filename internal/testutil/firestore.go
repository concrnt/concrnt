package testutil

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// FirestoreEmulatorImage is the official Cloud SDK image with the emulators
// (and the JRE they need) preinstalled.
const (
	FirestoreEmulatorImage = "gcr.io/google.com/cloudsdktool/google-cloud-cli"
	FirestoreEmulatorTag   = "emulators"
	firestoreEmulatorBoot  = 90 * time.Second
)

var (
	firestoreOnce       sync.Once
	firestoreHost       string
	firestoreResource   *dockertest.Resource
	firestoreProjectSeq atomic.Int64
)

// firestoreEmulator returns the host:port of a Firestore emulator: the one
// named by CONCRNT_TEST_FIRESTORE_HOST, or one started here once per test
// process (one container; every CreateFirestore call gets its own project).
func firestoreEmulator(t *testing.T) string {
	t.Helper()
	if os.Getenv("CONCRNT_TEST_FIRESTORE") == "0" {
		t.Skip("CONCRNT_TEST_FIRESTORE=0")
	}
	firestoreOnce.Do(func() {
		if host := os.Getenv("CONCRNT_TEST_FIRESTORE_HOST"); host != "" {
			firestoreHost = host
			return
		}
		pool := getPool()
		resource, err := pool.RunWithOptions(&dockertest.RunOptions{
			Name:         "concrnt_firestore_" + getSuffix() + fmt.Sprintf("_%d", os.Getpid()),
			Repository:   FirestoreEmulatorImage,
			Tag:          FirestoreEmulatorTag,
			Cmd:          []string{"gcloud", "emulators", "firestore", "start", "--host-port=0.0.0.0:8080"},
			ExposedPorts: []string{"8080/tcp"},
		}, func(hc *dc.HostConfig) {
			hc.AutoRemove = true
		})
		if err != nil {
			log.Fatalf("Could not start firestore emulator: %s", err)
		}
		// the container is removed when it stops; the expiry is the safety
		// net for a test process killed before it could purge
		_ = resource.Expire(3600)
		firestoreResource = resource
		firestoreHost = "localhost:" + resource.GetPort("8080/tcp")
		log.Printf("Firestore emulator running on %s\n", firestoreHost)

		// the emulator answers "Ok" on its root once it accepts connections
		deadline := time.Now().Add(firestoreEmulatorBoot)
		for {
			resp, err := http.Get("http://" + firestoreHost + "/")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					break
				}
			}
			if time.Now().After(deadline) {
				PurgeFirestore()
				log.Fatalf("Firestore emulator did not become ready: %v", err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	})
	return firestoreHost
}

// PurgeFirestore stops the emulator container this process started, if
// any. Test packages call it from TestMain after m.Run.
func PurgeFirestore() {
	if firestoreResource != nil {
		_ = getPool().Purge(firestoreResource)
		firestoreResource = nil
	}
}

// CreateFirestore returns a client on an empty database: every call
// targets a fresh project id, and the emulator namespaces data per project,
// so tests share the (slow to boot) container without cleaning up after
// each other.
func CreateFirestore(t *testing.T) *firestore.Client {
	t.Helper()
	host := firestoreEmulator(t)

	// Dial options instead of FIRESTORE_EMULATOR_HOST keep the process
	// environment untouched across packages.
	project := fmt.Sprintf("test-%s-%d", sanitizeProject(t.Name()), firestoreProjectSeq.Add(1))
	conn, err := grpc.NewClient(host,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(emulatorCreds{}),
	)
	if err != nil {
		t.Fatalf("failed to dial firestore emulator: %v", err)
	}
	client, err := firestore.NewClient(context.Background(), project, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("failed to create firestore client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

// emulatorCreds is what the SDK attaches when FIRESTORE_EMULATOR_HOST is set:
// the emulator treats "Bearer owner" as an admin, which batch writes require.
type emulatorCreds struct{}

func (emulatorCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer owner"}, nil
}

func (emulatorCreds) RequireTransportSecurity() bool { return false }

func sanitizeProject(name string) string {
	name = strings.ToLower(name)
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	s := b.String()
	if len(s) > 24 {
		s = s[:24]
	}
	return s
}

package testutil

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/ory/dockertest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// firestoreEmulatorImage is the official Cloud SDK image with the emulators
// (and the JRE they need) preinstalled.
const (
	firestoreEmulatorImage = "gcr.io/google.com/cloudsdktool/google-cloud-cli"
	firestoreEmulatorTag   = "emulators"
	firestoreEmulatorBoot  = 90 * time.Second
)

var firestoreProjectSeq atomic.Int64

// CreateFirestore starts one Firestore emulator container and returns a
// factory handing out isolated clients: every call targets a fresh project
// id, and the emulator namespaces data per project, so tests share the (slow
// to boot) container without cleaning up after each other. Call it once per
// package from TestMain.
func CreateFirestore() (newClient func(t testing.TB) *firestore.Client, cleanup func()) {
	pool := getPool()

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         getCallerName() + "_firestore_" + getSuffix(),
		Repository:   firestoreEmulatorImage,
		Tag:          firestoreEmulatorTag,
		Cmd:          []string{"gcloud", "emulators", "firestore", "start", "--host-port=0.0.0.0:8080"},
		ExposedPorts: []string{"8080/tcp"},
	})
	if err != nil {
		log.Fatalf("Could not start firestore emulator: %s", err)
	}
	cleanup = func() {
		closeContainer(pool, resource)
	}

	host := "localhost:" + resource.GetPort("8080/tcp")
	log.Printf("Firestore emulator running on %s\n", host)

	// the emulator answers "Ok" on its root once it accepts connections
	deadline := time.Now().Add(firestoreEmulatorBoot)
	for {
		resp, err := http.Get("http://" + host + "/")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			cleanup()
			log.Fatalf("Firestore emulator did not become ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	newClient = func(t testing.TB) *firestore.Client {
		t.Helper()
		// per-test project: the emulator keeps each project's data separate.
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
	return newClient, cleanup
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

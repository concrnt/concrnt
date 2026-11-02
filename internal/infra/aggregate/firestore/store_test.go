package firestore_test

import (
	"os"
	"testing"

	"github.com/concrnt/concrnt/internal/infra/aggregate/firestore"
	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.PurgeFirestore()
	os.Exit(code)
}

func TestFirestoreStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) aggregate.Store {
		return firestore.New(testutil.CreateFirestore(t))
	})
}

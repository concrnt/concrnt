package storetest_test

import (
	"testing"

	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
)

func TestInMemoryStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) aggregate.Store { return storetest.NewInMemory() })
}

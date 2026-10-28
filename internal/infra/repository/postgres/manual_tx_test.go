package postgres

import (
	"context"
	"errors"

	"github.com/concrnt/concrnt/internal/usecase/record"
)

// manualTx drives one RunInTx step by step so the pre-RunInTx tests keep
// their begin / commit / rollback shape until repotest replaces them.
type manualTx struct {
	tx     record.RepositoryTx
	finish chan error
	result chan error
}

var errManualRollback = errors.New("manual rollback")

func beginTx(ctx context.Context, repo record.Repository) (*manualTx, error) {
	m := &manualTx{finish: make(chan error, 1), result: make(chan error, 1)}
	ready := make(chan struct{}, 1)
	go func() {
		m.result <- repo.RunInTx(ctx, func(tx record.RepositoryTx) error {
			m.tx = tx
			ready <- struct{}{}
			return <-m.finish
		})
	}()
	select {
	case <-ready:
		return m, nil
	case err := <-m.result:
		return nil, err
	}
}

func (m *manualTx) Commit(ctx context.Context) error {
	m.finish <- nil
	return <-m.result
}

func (m *manualTx) Rollback(ctx context.Context) error {
	m.finish <- errManualRollback
	if err := <-m.result; !errors.Is(err, errManualRollback) {
		return err
	}
	return nil
}

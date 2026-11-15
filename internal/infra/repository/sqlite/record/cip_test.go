package record

import (
	"testing"

	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	"github.com/concrnt/concrnt/internal/usecase/record/ciptest"
)

func TestSQLiteCIP(t *testing.T) {
	ciptest.Run(t, func(t *testing.T) ciptest.Backend {
		db := openDB(t).DB
		return ciptest.Backend{
			Record:     New(db),
			EntityMeta: sqlitedb.NewEntityMetaRepository(db),
		}
	})
}

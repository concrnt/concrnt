package record

import (
	"testing"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/record/ciptest"
)

func TestSpannerCIP(t *testing.T) {
	ciptest.Run(t, func(t *testing.T) ciptest.Backend {
		client, _ := testutil.CreateSpanner(t)
		return ciptest.Backend{
			Record:     New(client),
			EntityMeta: spannerdb.NewEntityMetaRepository(client),
		}
	})
}

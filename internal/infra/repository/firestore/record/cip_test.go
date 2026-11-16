package record

import (
	"testing"

	fsrepo "github.com/concrnt/concrnt/internal/infra/repository/firestore"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/record/ciptest"
)

func TestFirestoreCIP(t *testing.T) {
	ciptest.Run(t, func(t *testing.T) ciptest.Backend {
		client := testutil.CreateFirestore(t)
		return ciptest.Backend{
			Record:     New(client),
			EntityMeta: fsrepo.NewEntityMetaRepository(client),
		}
	})
}

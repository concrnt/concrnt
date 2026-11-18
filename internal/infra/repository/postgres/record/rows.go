package record

import (
	"encoding/json"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/concrnt/concrnt"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// recordKeysToQueryRows converts record keys joined with their commit log into query rows
// carrying the created_at sort key the query ordered by. A placeholder key
// (record_id NULL, reachable only through QueryByParentOrderByKey) becomes a key-only row holding just its cckv (CIP-5 §3.2.1).
func recordKeysToQueryRows(rks []RecordKey, span trace.Span) ([]ucrecord.QueryRow, error) {
	rows := make([]ucrecord.QueryRow, 0, len(rks))
	for _, rk := range rks {
		if rk.RecordID == nil {
			rows = append(rows, ucrecord.QueryRow{Row: concrnt.SignedDocument{CCKV: &rk.URI}})
			continue
		}

		var proof concrnt.Proof
		err := json.Unmarshal([]byte(rk.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(rk.Owner, concrnt.CCFSTypeConcrnt, *rk.RecordID)

		var createdAt time.Time
		if rk.RecordCreatedAt != nil {
			createdAt = *rk.RecordCreatedAt
		}

		rows = append(rows, ucrecord.QueryRow{
			Row: concrnt.SignedDocument{
				CCKV:     &rk.URI,
				CCFS:     &ccfs,
				Document: rk.Document.Document,
				Proof:    proof,
			},
			CreatedAt: createdAt,
		})
	}

	return rows, nil
}

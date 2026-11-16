package record

import (
	"encoding/json"

	"go.opentelemetry.io/otel/trace"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// Not Preload("Record.Document"): that is one more round trip per relation.
// The aliases are the ones gorm scans into RecordKey.Record and its Document.
const recordKeySelect = `record_keys.*, r.document_id AS "Record__document_id", r.owner AS "Record__owner", r.created_at AS "Record__created_at", c.document AS "Record__Document__document", c.proof AS "Record__Document__proof"`

// recordKeysToQueryRows converts joined record keys into query rows
// carrying the created_at sort key the query ordered by. A placeholder key
// (record_id NULL, reachable only through QueryByParentOrderByKey's LEFT
// JOIN) becomes a key-only row holding just its cckv (CIP-5 §3.2.1).
func recordKeysToQueryRows(rks []models.RecordKey, span trace.Span) ([]ucrecord.QueryRow, error) {
	rows := make([]ucrecord.QueryRow, 0, len(rks))
	for _, rk := range rks {
		if rk.RecordID == nil {
			rows = append(rows, ucrecord.QueryRow{Row: concrnt.SignedDocument{CCKV: &rk.URI}})
			continue
		}

		var proof concrnt.Proof
		err := json.Unmarshal([]byte(rk.Record.Document.Proof), &proof)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}

		ccfs := concrnt.ComposeCCFSURI(rk.Record.Owner, concrnt.CCFSTypeConcrnt, rk.Record.DocumentID)

		rows = append(rows, ucrecord.QueryRow{
			Row: concrnt.SignedDocument{
				CCKV:     &rk.URI,
				CCFS:     &ccfs,
				Document: rk.Record.Document.Document,
				Proof:    proof,
			},
			CreatedAt: rk.Record.CreatedAt,
		})
	}

	return rows, nil
}

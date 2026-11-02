package record

import (
	"encoding/json"
	"time"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// recordRow is one record key joined with its record and commit log in a
// single statement (no Preload round trips). The record and commit columns
// are NULL on a placeholder key, which only the LEFT JOIN listing returns.
type recordRow struct {
	URI       string
	RecordID  *string
	Owner     *string
	CreatedAt *time.Time
	Document  *string
	Proof     *string
}

const recordRowSelect = "record_keys.uri AS uri, record_keys.record_id AS record_id, r.owner AS owner, r.created_at AS created_at, c.document AS document, c.proof AS proof"

// joinRecords joins records (r) and commit_logs (c) onto a record_keys
// query; left keeps placeholder keys.
func joinRecords(query *gorm.DB, left bool) *gorm.DB {
	join := "JOIN"
	if left {
		join = "LEFT JOIN"
	}
	return query.
		Joins(join + " records r ON r.document_id = record_keys.record_id").
		Joins(join + " commit_logs c ON c.id = record_keys.record_id")
}

func scanRecordRows(query *gorm.DB) ([]recordRow, error) {
	var rows []recordRow
	if err := query.Select(recordRowSelect).Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// signedDocument builds the signed document of a row holding a record.
func (row recordRow) signedDocument() (concrnt.SignedDocument, error) {
	var proof concrnt.Proof
	if row.Proof != nil {
		if err := json.Unmarshal([]byte(*row.Proof), &proof); err != nil {
			return concrnt.SignedDocument{}, err
		}
	}
	uri := row.URI
	owner := ""
	if row.Owner != nil {
		owner = *row.Owner
	}
	document := ""
	if row.Document != nil {
		document = *row.Document
	}
	ccfs := concrnt.ComposeCCFSURI(owner, concrnt.CCFSTypeConcrnt, *row.RecordID)
	return concrnt.SignedDocument{CCKV: &uri, CCFS: &ccfs, Document: document, Proof: proof}, nil
}

// queryRows converts joined rows into query rows carrying the created_at
// sort key. A placeholder key (record_id NULL) becomes a key-only row
// holding just its cckv (CIP-5 §3.2.1).
func queryRows(rows []recordRow) ([]ucrecord.QueryRow, error) {
	out := make([]ucrecord.QueryRow, 0, len(rows))
	for _, row := range rows {
		if row.RecordID == nil {
			uri := row.URI
			out = append(out, ucrecord.QueryRow{Row: concrnt.SignedDocument{CCKV: &uri}})
			continue
		}
		sd, err := row.signedDocument()
		if err != nil {
			return nil, err
		}
		var createdAt time.Time
		if row.CreatedAt != nil {
			createdAt = *row.CreatedAt
		}
		out = append(out, ucrecord.QueryRow{Row: sd, CreatedAt: createdAt})
	}
	return out, nil
}

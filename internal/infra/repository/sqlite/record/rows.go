package record

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/concrnt/concrnt"
	sqlitedb "github.com/concrnt/concrnt/internal/infra/repository/sqlite"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// recordRow is one record key joined with its record and commit log. The
// record and commit columns are NULL on a placeholder key, which only the
// LEFT JOIN listing returns.
type recordRow struct {
	URI       string
	RecordID  sql.NullString
	Owner     sql.NullString
	CreatedAt sql.NullInt64
	Document  sql.NullString
	Proof     sql.NullString
}

const recordRowSelect = "SELECT record_keys.uri, record_keys.record_id, r.owner, r.created_at, c.document, c.proof FROM record_keys "

// joinRecords is the FROM clause joining records (r) and commit_logs (c);
// left keeps placeholder keys.
func joinRecords(left bool) string {
	join := "JOIN"
	if left {
		join = "LEFT JOIN"
	}
	return recordRowSelect + join + " records r ON r.document_id = record_keys.record_id " + join + " commit_logs c ON c.id = record_keys.record_id "
}

func scanRecordRows(ctx context.Context, q querier, query string, args ...any) ([]recordRow, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []recordRow{}
	for rows.Next() {
		var row recordRow
		if err := rows.Scan(&row.URI, &row.RecordID, &row.Owner, &row.CreatedAt, &row.Document, &row.Proof); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// signedDocument builds the signed document of a row holding a record.
func (row recordRow) signedDocument() (concrnt.SignedDocument, error) {
	var proof concrnt.Proof
	if row.Proof.Valid {
		if err := json.Unmarshal([]byte(row.Proof.String), &proof); err != nil {
			return concrnt.SignedDocument{}, err
		}
	}
	uri := row.URI
	ccfs := concrnt.ComposeCCFSURI(row.Owner.String, concrnt.CCFSTypeConcrnt, row.RecordID.String)
	return concrnt.SignedDocument{CCKV: &uri, CCFS: &ccfs, Document: row.Document.String, Proof: proof}, nil
}

// queryRows converts joined rows into query rows carrying the created_at
// sort key; a placeholder key becomes a key-only row (CIP-5 §3.2.1).
func queryRows(rows []recordRow) ([]ucrecord.QueryRow, error) {
	out := make([]ucrecord.QueryRow, 0, len(rows))
	for _, row := range rows {
		if !row.RecordID.Valid {
			uri := row.URI
			out = append(out, ucrecord.QueryRow{Row: concrnt.SignedDocument{CCKV: &uri}})
			continue
		}
		sd, err := row.signedDocument()
		if err != nil {
			return nil, err
		}
		var createdAt time.Time
		if row.CreatedAt.Valid {
			createdAt = sqlitedb.Time(row.CreatedAt.Int64)
		}
		out = append(out, ucrecord.QueryRow{Row: sd, CreatedAt: createdAt})
	}
	return out, nil
}

// commitRow is a state row (association, ack, acked) joined with its commit
// log; Owner is the namespace its ccfs URI is composed in.
type commitRow struct {
	DocumentID string
	Owner      string
	CreatedAt  int64
	Document   string
	Proof      string
}

func scanCommitRows(ctx context.Context, q querier, query string, args ...any) ([]ucrecord.QueryRow, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []ucrecord.QueryRow{}
	for rows.Next() {
		var row commitRow
		if err := rows.Scan(&row.DocumentID, &row.Owner, &row.CreatedAt, &row.Document, &row.Proof); err != nil {
			return nil, err
		}
		var proof concrnt.Proof
		if err := json.Unmarshal([]byte(row.Proof), &proof); err != nil {
			return nil, err
		}
		ccfs := concrnt.ComposeCCFSURI(row.Owner, concrnt.CCFSTypeConcrnt, row.DocumentID)
		out = append(out, ucrecord.QueryRow{
			Row:       concrnt.SignedDocument{CCFS: &ccfs, Document: row.Document, Proof: proof},
			CreatedAt: sqlitedb.Time(row.CreatedAt),
		})
	}
	return out, rows.Err()
}

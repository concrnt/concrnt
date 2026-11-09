package record

import (
	"context"
	"time"

	spannerdb "github.com/concrnt/concrnt/internal/infra/repository/spanner"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/repository/repoutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// recordRow is one record key joined with its record and commit log in a
// single statement. The record and commit columns are NULL on a placeholder
// key, which only the LEFT JOIN listing returns.
type recordRow struct {
	URI       string     `spanner:"uri"`
	RecordID  *string    `spanner:"record_id"`
	Owner     *string    `spanner:"owner"`
	CreatedAt *time.Time `spanner:"created_at"`
	Document  *string    `spanner:"document"`
	Proof     *string    `spanner:"proof"`
}

const recordRowSelect = "SELECT rk.uri AS uri, rk.record_id AS record_id, r.owner AS owner, r.created_at AS created_at, c.document AS document, c.proof AS proof FROM record_keys rk "

// joinRecords is the FROM clause joining records (r) and commit_logs (c)
// onto record_keys (rk); left keeps placeholder keys.
func joinRecords(left bool) string {
	join := "JOIN"
	if left {
		join = "LEFT JOIN"
	}
	return recordRowSelect + join + " records r ON r.id = rk.record_id " + join + " commit_logs c ON c.id = rk.record_id "
}

func scanRecordRows(ctx context.Context, q spannerdb.Reader, sql string, params map[string]any) ([]recordRow, error) {
	return spannerdb.QueryAll[recordRow](ctx, q, spanner.Statement{SQL: sql, Params: params})
}

// signedDocument builds the signed document of a row holding a record.
func (row recordRow) signedDocument() (concrnt.SignedDocument, error) {
	return repoutil.RecordDocument(row.URI, deref(row.Owner), *row.RecordID, deref(row.Document), deref(row.Proof))
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
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
			createdAt = row.CreatedAt.UTC()
		}
		out = append(out, ucrecord.QueryRow{Row: sd, CreatedAt: createdAt})
	}
	return out, nil
}

// commitRow is a state row (association, ack, acked) joined with its commit
// log; Owner is the namespace its ccfs URI is composed in.
type commitRow struct {
	DocumentID string    `spanner:"document_id"`
	Owner      string    `spanner:"owner"`
	CreatedAt  time.Time `spanner:"created_at"`
	Document   string    `spanner:"document"`
	Proof      string    `spanner:"proof"`
}

func commitRows(rows []commitRow) ([]ucrecord.QueryRow, error) {
	out := make([]ucrecord.QueryRow, len(rows))
	for i, row := range rows {
		sd, err := repoutil.CommitDocument(row.Owner, row.DocumentID, row.Document, row.Proof)
		if err != nil {
			return nil, err
		}
		out[i] = ucrecord.QueryRow{
			Row:       sd,
			CreatedAt: row.CreatedAt.UTC(),
		}
	}
	return out, nil
}

// timeRange appends the since / until predicates on column to the where
// clauses and params.
func timeRange(where []string, params map[string]any, column string, since, until *time.Time) []string {
	if since != nil {
		where = append(where, column+" >= @since")
		params["since"] = *since
	}
	if until != nil {
		where = append(where, column+" <= @until")
		params["until"] = *until
	}
	return where
}

// orderLimit renders the ORDER BY of the (created_at, id) pair and the LIMIT.
func orderLimit(createdAt, id string, order string, limit int, params map[string]any) string {
	dir := "ASC"
	if order == "desc" {
		dir = "DESC"
	}
	sql := " ORDER BY " + createdAt + " " + dir + ", " + id + " " + dir
	if limit > 0 {
		sql += " LIMIT @limit"
		params["limit"] = int64(limit)
	}
	return sql
}

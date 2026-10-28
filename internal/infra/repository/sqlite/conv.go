package sqlite

import (
	"database/sql"
	"encoding/json"
	"time"
)

// Timestamps are stored as INTEGER Unix microseconds (UTC): the precision
// of Postgres timestamptz, so cursors and dumps round-trip byte for byte,
// and an integer order that no collation can disturb.

func Micros(t time.Time) int64 {
	return t.UnixMicro()
}

func Time(us int64) time.Time {
	return time.UnixMicro(us).UTC()
}

func NullMicros(t *time.Time) sql.NullInt64 {
	if t == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: t.UnixMicro(), Valid: true}
}

func NullTime(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := Time(v.Int64)
	return &t
}

func NullString(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}

func StringPtr(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	s := v.String
	return &s
}

// Postgres text[] columns are JSON arrays here.

func JSONStrings(values []string) string {
	if values == nil {
		values = []string{}
	}
	b, _ := json.Marshal(values)
	return string(b)
}

func Strings(raw string) ([]string, error) {
	if raw == "" {
		return []string{}, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = []string{}
	}
	return out, nil
}

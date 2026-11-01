package sqlite

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// Timestamps are stored as INTEGER Unix microseconds (UTC): the precision
// of Postgres timestamptz, so cursors and dumps round-trip byte for byte,
// and an integer order that no collation can disturb. A time.Time must never
// reach the driver as a bind variable (it would be bound as RFC3339 text
// and compared against INTEGER), hence the conversions below.

func Micros(t time.Time) int64 {
	return t.UnixMicro()
}

func Time(us int64) time.Time {
	return time.UnixMicro(us).UTC()
}

func MicrosPtr(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	us := t.UnixMicro()
	return &us
}

func TimePtr(us *int64) *time.Time {
	if us == nil {
		return nil
	}
	t := Time(*us)
	return &t
}

func nowMicros() int64 {
	return Micros(time.Now())
}

// JSONStrings is a Postgres text[] column stored as a JSON array. It never
// reads or writes NULL/null: nil is "[]" on the way in and []string{} on
// the way out.
type JSONStrings []string

func (s JSONStrings) Value() (driver.Value, error) {
	if s == nil {
		s = JSONStrings{}
	}
	b, err := json.Marshal([]string(s))
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (s *JSONStrings) Scan(src any) error {
	var raw []byte
	switch v := src.(type) {
	case nil:
		*s = JSONStrings{}
		return nil
	case string:
		raw = []byte(v)
	case []byte:
		raw = v
	default:
		return fmt.Errorf("sqlite: cannot scan %T into JSONStrings", src)
	}
	if len(raw) == 0 {
		*s = JSONStrings{}
		return nil
	}
	var out []string
	if err := json.Unmarshal(raw, &out); err != nil {
		return err
	}
	if out == nil {
		out = []string{}
	}
	*s = out
	return nil
}

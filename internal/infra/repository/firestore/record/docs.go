//go:build contractv1

package record

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"cloud.google.com/go/firestore"

	"github.com/concrnt/concrnt"
	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
)

func keyID(uri string) string              { return fs.HashID(uri) }
func ackID(from, to, schema string) string { return fs.HashID(from, to, schema) }
func uniqueID(unique string) string        { return fs.HashID(unique) }

// Every document type decodes from a map, never from a snapshot: inside a
// transaction the map may be a pending write (tx.go), outside it is
// snap.Data(). The firestore.ServerTimestamp sentinel of a pending write
// reads as the zero time.

func str(m map[string]any, k string) string {
	v, _ := m[k].(string)
	return v
}

func boolean(m map[string]any, k string) bool {
	v, _ := m[k].(bool)
	return v
}

func timeAt(m map[string]any, k string) (time.Time, bool) {
	v, ok := m[k].(time.Time)
	return v, ok
}

func strs(m map[string]any, k string) []string {
	switch v := m[k].(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// ancestorsOf lists every strict path ancestor of a key URI from the owner
// root down to its parent, e.g. cckv://o/a/b -> [cckv://o, cckv://o/a]; it
// is empty for the owner root itself. It is the array QueryByPrefix matches
// on (SQL: uri LIKE 'cckv://o/a/%') and its last element is the parentURI.
func ancestorsOf(uri string) []string {
	idx := strings.Index(uri, "://")
	if idx < 0 {
		return nil
	}
	rest := uri[idx+3:]
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return nil
	}
	root := uri[:idx+3+slash]
	segments := strings.Split(strings.Trim(rest[slash+1:], "/"), "/")
	ancestors := make([]string, 0, len(segments))
	current := root
	ancestors = append(ancestors, current)
	for i := 0; i < len(segments)-1; i++ {
		current = current + "/" + segments[i]
		ancestors = append(ancestors, current)
	}
	return ancestors
}

// parentOf is the parent URI of a key: the URI one segment up, the owner
// root (cckv://<owner>) for a top-level key, "" for the owner root itself.
func parentOf(uri string) string {
	ancestors := ancestorsOf(uri)
	if len(ancestors) == 0 {
		return ""
	}
	return ancestors[len(ancestors)-1]
}

// commits/{cdid}
type commitDoc struct {
	IP          string
	Document    string
	Proof       string
	Owner       string
	GcCandidate bool
	CDate       time.Time
}

func (d commitDoc) toMap() map[string]any {
	return map[string]any{
		"ip":          d.IP,
		"document":    d.Document,
		"proof":       d.Proof,
		"owner":       d.Owner,
		"gcCandidate": d.GcCandidate,
		"cDate":       firestore.ServerTimestamp,
	}
}

func commitFromMap(m map[string]any) commitDoc {
	cDate, _ := timeAt(m, "cDate")
	return commitDoc{
		IP:          str(m, "ip"),
		Document:    str(m, "document"),
		Proof:       str(m, "proof"),
		Owner:       str(m, "owner"),
		GcCandidate: boolean(m, "gcCandidate"),
		CDate:       cDate.UTC(),
	}
}

func (d commitDoc) proof() (concrnt.Proof, error) {
	var proof concrnt.Proof
	if d.Proof == "" {
		return proof, nil
	}
	if err := json.Unmarshal([]byte(d.Proof), &proof); err != nil {
		return proof, errors.Join(errors.New("failed to unmarshal proof"), err)
	}
	return proof, nil
}

// records/{cdid}
type recordDoc struct {
	Owner         string
	Author        string
	Schema        string
	Redirect      string
	Policies      string
	Distributions []string
	CreatedAt     time.Time
}

func (d recordDoc) toMap() map[string]any {
	return map[string]any{
		"owner":         d.Owner,
		"author":        d.Author,
		"schema":        d.Schema,
		"redirect":      d.Redirect,
		"policies":      d.Policies,
		"distributions": d.Distributions,
		"createdAt":     d.CreatedAt,
		"cDate":         firestore.ServerTimestamp,
	}
}

// record_keys/{keyID(uri)}
type recordKeyDoc struct {
	URI       string
	ParentURI string
	Ancestors []string

	// absent on placeholder keys
	RecordID        string
	RecordCreatedAt *time.Time
	CleanOnUpdate   bool

	// denormalised from records/{RecordID}
	Owner         string
	Author        string
	Schema        string
	Redirect      string
	Policies      string
	Distributions []string
}

func (d recordKeyDoc) isPlaceholder() bool { return d.RecordID == "" || d.RecordCreatedAt == nil }

// toMap omits every record field on placeholders so orderBy(recordCreatedAt)
// (and an equality on schema or author) excludes them; a null value would
// still "exist".
func (d recordKeyDoc) toMap() map[string]any {
	ancestors := d.Ancestors
	if ancestors == nil {
		ancestors = []string{}
	}
	m := map[string]any{
		"uri":       d.URI,
		"parentURI": d.ParentURI,
		"ancestors": ancestors,
	}
	if d.isPlaceholder() {
		return m
	}
	distributions := d.Distributions
	if distributions == nil {
		distributions = []string{}
	}
	m["recordID"] = d.RecordID
	m["recordCreatedAt"] = *d.RecordCreatedAt
	m["cleanOnUpdate"] = d.CleanOnUpdate
	m["owner"] = d.Owner
	m["author"] = d.Author
	m["schema"] = d.Schema
	m["redirect"] = d.Redirect
	m["policies"] = d.Policies
	m["distributions"] = distributions
	return m
}

func recordKeyFromMap(m map[string]any) recordKeyDoc {
	d := recordKeyDoc{
		URI:           str(m, "uri"),
		ParentURI:     str(m, "parentURI"),
		Ancestors:     strs(m, "ancestors"),
		RecordID:      str(m, "recordID"),
		CleanOnUpdate: boolean(m, "cleanOnUpdate"),
		Owner:         str(m, "owner"),
		Author:        str(m, "author"),
		Schema:        str(m, "schema"),
		Redirect:      str(m, "redirect"),
		Policies:      str(m, "policies"),
		Distributions: strs(m, "distributions"),
	}
	if t, ok := timeAt(m, "recordCreatedAt"); ok {
		t = t.UTC()
		d.RecordCreatedAt = &t
	}
	return d
}

// acks/{ackID} and ackeds/{ackID}
type ackDoc struct {
	From       string
	To         string
	Schema     string
	DocumentID string
	Valid      bool
	CreatedAt  time.Time
}

func (d ackDoc) toMap() map[string]any {
	return map[string]any{
		"from":       d.From,
		"to":         d.To,
		"schema":     d.Schema,
		"documentID": d.DocumentID,
		"valid":      d.Valid,
		"createdAt":  d.CreatedAt,
		"cDate":      firestore.ServerTimestamp,
	}
}

func ackFromMap(m map[string]any) ackDoc {
	createdAt, _ := timeAt(m, "createdAt")
	return ackDoc{
		From:       str(m, "from"),
		To:         str(m, "to"),
		Schema:     str(m, "schema"),
		DocumentID: str(m, "documentID"),
		Valid:      boolean(m, "valid"),
		CreatedAt:  createdAt.UTC(),
	}
}

// associations/{cdid}
type associationDoc struct {
	TargetURI string
	Owner     string
	Author    string
	Schema    string
	Variant   string
	Unique    string
	CreatedAt time.Time
}

func (d associationDoc) toMap() map[string]any {
	return map[string]any{
		"targetURI": d.TargetURI,
		"owner":     d.Owner,
		"author":    d.Author,
		"schema":    d.Schema,
		"variant":   d.Variant,
		"unique":    d.Unique,
		"createdAt": d.CreatedAt,
		"cDate":     firestore.ServerTimestamp,
	}
}

func associationFromMap(m map[string]any) associationDoc {
	createdAt, _ := timeAt(m, "createdAt")
	return associationDoc{
		TargetURI: str(m, "targetURI"),
		Owner:     str(m, "owner"),
		Author:    str(m, "author"),
		Schema:    str(m, "schema"),
		Variant:   str(m, "variant"),
		Unique:    str(m, "unique"),
		CreatedAt: createdAt.UTC(),
	}
}

// entities/{ccid}
type entityDoc struct {
	Alias      string
	Domain     string
	Tag        string
	DocumentID string
	CreatedAt  time.Time
}

func entityFromMap(m map[string]any) entityDoc {
	createdAt, _ := timeAt(m, "createdAt")
	return entityDoc{
		Alias:      str(m, "alias"),
		Domain:     str(m, "domain"),
		Tag:        str(m, "tag"),
		DocumentID: str(m, "documentID"),
		CreatedAt:  createdAt.UTC(),
	}
}

// strPtr maps the empty string back to the nil the domain uses for "unset".

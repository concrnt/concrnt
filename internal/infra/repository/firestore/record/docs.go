package record

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
	"time"

	"cloud.google.com/go/firestore"

	fs "github.com/concrnt/concrnt/internal/infra/repository/firestore"
)

func keyID(uri string) string              { return fs.HashID(uri) }
func ackID(from, to, schema string) string { return fs.HashID(from, to, schema) }

// uniqueID is the id of the uniqueness marker of an association: the same
// logical association on the same target, whatever the incarnation. It is
// derived from the arguments of the write alone, so the marker is part of
// the one read every Apply* does; which incarnation it belongs to is a
// field (targetGen), compared against the target's gen read alongside.
func uniqueID(targetURI, unique string) string { return fs.HashID(targetURI, unique) }

// newGen names a key incarnation. It is random, never reused, and unique
// across keys, so it identifies the incarnation without the key's URI.
func newGen() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// Every document type is written from and decoded to a map: a field that
// is absent stays absent (placeholder keys, invalid ack states), which a
// struct with zero values could not express. The firestore.ServerTimestamp
// sentinel only ever goes out.

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

// keyChainOf lists the keys on the ancestor chain of a key URI that have a
// document of their own, top-down, the key itself last (the owner root has
// none).
func keyChainOf(uri string) []string {
	ancestors := ancestorsOf(uri)
	if len(ancestors) == 0 {
		return nil
	}
	return append(ancestors[1:], uri)
}

// commits/{cdid}
type commitDoc struct {
	IP          string
	Document    string
	Proof       string
	Owner       string
	GcCandidate bool
	CDate       time.Time
	// Anchor is the collection holding what the commit stored, "" for a
	// commit that stored nothing but itself (a delete); AnchorID identifies
	// the document in it: the cckv key of a record, the state id of an ack,
	// the ccid of an entity, nothing for an association (it has the
	// commit's id).
	Anchor   string
	AnchorID string
}

func (d commitDoc) toMap() map[string]any {
	return map[string]any{
		"ip":          d.IP,
		"document":    d.Document,
		"proof":       d.Proof,
		"owner":       d.Owner,
		"gcCandidate": d.GcCandidate,
		"anchor":      d.Anchor,
		"anchorID":    d.AnchorID,
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
		Anchor:      str(m, "anchor"),
		AnchorID:    str(m, "anchorID"),
	}
}

// record_keys/{keyID(uri)}
type recordKeyDoc struct {
	URI       string
	ParentURI string
	Ancestors []string
	// Gen is the key's incarnation (CONTRACT.md C11).
	Gen string

	// absent on placeholder keys
	RecordID        string
	RecordCreatedAt *time.Time
	CleanOnUpdate   bool
	Owner           string
	Author          string
	Schema          string
	Redirect        string
	Policies        string
	Distributions   []string
	Document        string
	Proof           string
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
		"gen":       d.Gen,
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
	m["document"] = d.Document
	m["proof"] = d.Proof
	return m
}

func recordKeyFromMap(m map[string]any) recordKeyDoc {
	d := recordKeyDoc{
		URI:           str(m, "uri"),
		ParentURI:     str(m, "parentURI"),
		Ancestors:     strs(m, "ancestors"),
		Gen:           str(m, "gen"),
		RecordID:      str(m, "recordID"),
		CleanOnUpdate: boolean(m, "cleanOnUpdate"),
		Owner:         str(m, "owner"),
		Author:        str(m, "author"),
		Schema:        str(m, "schema"),
		Redirect:      str(m, "redirect"),
		Policies:      str(m, "policies"),
		Distributions: strs(m, "distributions"),
		Document:      str(m, "document"),
		Proof:         str(m, "proof"),
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
	// only on a valid state: an invalid one is never returned by a read
	Document string
	Proof    string
}

func (d ackDoc) toMap() map[string]any {
	m := map[string]any{
		"from":       d.From,
		"to":         d.To,
		"schema":     d.Schema,
		"documentID": d.DocumentID,
		"valid":      d.Valid,
		"createdAt":  d.CreatedAt,
		"cDate":      firestore.ServerTimestamp,
	}
	if d.Valid {
		m["document"] = d.Document
		m["proof"] = d.Proof
	}
	return m
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
		Document:   str(m, "document"),
		Proof:      str(m, "proof"),
	}
}

// associations/{cdid}
type associationDoc struct {
	TargetURI string
	// TargetGen is the incarnation of the target the association was
	// created on; it is live only while that is the target's gen.
	TargetGen string
	Owner     string
	Author    string
	Schema    string
	Variant   string
	Unique    string
	CreatedAt time.Time
	Document  string
	Proof     string
}

func (d associationDoc) toMap() map[string]any {
	return map[string]any{
		"targetURI": d.TargetURI,
		"targetGen": d.TargetGen,
		"owner":     d.Owner,
		"author":    d.Author,
		"schema":    d.Schema,
		"variant":   d.Variant,
		"unique":    d.Unique,
		"createdAt": d.CreatedAt,
		"document":  d.Document,
		"proof":     d.Proof,
		"cDate":     firestore.ServerTimestamp,
	}
}

func associationFromMap(m map[string]any) associationDoc {
	createdAt, _ := timeAt(m, "createdAt")
	return associationDoc{
		TargetURI: str(m, "targetURI"),
		TargetGen: str(m, "targetGen"),
		Owner:     str(m, "owner"),
		Author:    str(m, "author"),
		Schema:    str(m, "schema"),
		Variant:   str(m, "variant"),
		Unique:    str(m, "unique"),
		CreatedAt: createdAt.UTC(),
		Document:  str(m, "document"),
		Proof:     str(m, "proof"),
	}
}

// association_uniques/{uniqueID(targetURI, unique)}
func uniqueMarker(targetURI, targetGen, associationID string) map[string]any {
	return map[string]any{
		"targetURI":     targetURI,
		"targetGen":     targetGen,
		"associationID": associationID,
	}
}

// entities/{ccid}
type entityDoc struct {
	Alias      string
	Domain     string
	Tag        string
	DocumentID string
	CreatedAt  time.Time
	Document   string
	Proof      string
}

func entityFromMap(m map[string]any) entityDoc {
	createdAt, _ := timeAt(m, "createdAt")
	return entityDoc{
		Alias:      str(m, "alias"),
		Domain:     str(m, "domain"),
		Tag:        str(m, "tag"),
		DocumentID: str(m, "documentID"),
		CreatedAt:  createdAt.UTC(),
		Document:   str(m, "document"),
		Proof:      str(m, "proof"),
	}
}

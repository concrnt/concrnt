// Package firestore implements the usecase repository interfaces on Cloud
// Firestore (Native mode).
//
// Data model (one collection per Postgres table, see
// internal/infra/database/models for the relational original):
//
//   - commits/{cdid}: the immutable commit log (document + proof).
//   - records/{cdid}: per-version record metadata (policies, distributions).
//   - record_keys/{hash(uri)}: the mutable key index. A key doc carries the
//     denormalized fields of the record it currently points at, so listing
//     and chunkline queries never join. Placeholder (directory) keys omit the
//     record fields entirely — orderBy(recordCreatedAt) then excludes them the
//     way "record_created_at IS NOT NULL" does in SQL. Keys reference their
//     parent by the parent's gen (a random per-instance identity standing in
//     for the bigserial id) so a deleted-and-recreated key detaches its old
//     children and associations exactly like Postgres' FK cascade did.
//   - acks/{hash(from,to,schema)}, ackeds/{...}: one state doc per triple.
//   - associations/{cdid} + association_uniques/{unique}: the second
//     collection stands in for the unique constraint.
//   - entities, entity_metas, servers, subscriptions, abuse_reports.
package firestore

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/zeebo/xxh3"
)

const (
	colCommits       = "commits"
	colRecords       = "records"
	colRecordKeys    = "record_keys"
	colAcks          = "acks"
	colAckeds        = "ackeds"
	colAssociations  = "associations"
	colAssocUniques  = "association_uniques"
	colEntities      = "entities"
	colEntityMetas   = "entity_metas"
	colServers       = "servers"
	colSubscriptions = "subscriptions"
	colAbuseReports  = "abuse_reports"
	colMeta          = "_meta"
)

// hashID derives a Firestore document id from an arbitrary string (document
// ids may not contain '/' and are capped at 1500 bytes).
func hashID(parts ...string) string {
	h := xxh3.Hash128([]byte(strings.Join(parts, "\x1f"))).Bytes()
	return hex.EncodeToString(h[:])
}

func keyID(uri string) string                      { return hashID(uri) }
func ackID(from, to, schema string) string         { return hashID(from, to, schema) }
func subscriptionID(vendorID, owner string) string { return hashID(vendorID, owner) }

// newGen mints a key-instance identity; the SDK generates it client-side.
func newGen(c *firestore.Client) string {
	return c.Collection(colRecordKeys).NewDoc().ID
}

// parentURIOf mirrors postgres.getOrCreateParentRecordKey: the parent is the
// URI one path segment up, and a key directly under the owner root has none.
func parentURIOf(uri string) (string, bool, error) {
	parentURI, err := url.JoinPath(uri, "..")
	if err != nil {
		return "", false, err
	}
	parsed, err := url.Parse(parentURI)
	if err != nil {
		return "", false, err
	}
	if parsed.Path == "" || parsed.Path == "/" {
		return "", false, nil
	}
	return parentURI, true, nil
}

// ancestorsOf lists every strict path ancestor of a key URI from the owner
// root down to its parent, e.g. cckv://o/a/b -> [cckv://o, cckv://o/a]. It is
// the array QueryByPrefix matches on (SQL: uri LIKE 'cckv://o/a/%').
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

// commits/{cdid}
type commitDoc struct {
	IP          string    `firestore:"ip"`
	Document    string    `firestore:"document"`
	Proof       string    `firestore:"proof"`
	Owner       string    `firestore:"owner"`
	GcCandidate bool      `firestore:"gcCandidate"`
	CDate       time.Time `firestore:"cDate"`
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

// records/{cdid}
type recordDoc struct {
	Owner         string    `firestore:"owner"`
	Author        string    `firestore:"author"`
	Schema        string    `firestore:"schema"`
	Redirect      string    `firestore:"redirect"`
	Policies      string    `firestore:"policies"`
	Distributions []string  `firestore:"distributions"`
	CreatedAt     time.Time `firestore:"createdAt"`
	CDate         time.Time `firestore:"cDate"`
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
	URI       string   `firestore:"uri"`
	Gen       string   `firestore:"gen"`
	ParentGen string   `firestore:"parentGen"`
	Ancestors []string `firestore:"ancestors"`

	// absent on placeholder keys
	RecordID        string     `firestore:"recordID"`
	RecordCreatedAt *time.Time `firestore:"recordCreatedAt"`
	CleanOnUpdate   bool       `firestore:"cleanOnUpdate"`

	// denormalized from records/{RecordID}
	Owner         string   `firestore:"owner"`
	Author        string   `firestore:"author"`
	Schema        string   `firestore:"schema"`
	Redirect      string   `firestore:"redirect"`
	Policies      string   `firestore:"policies"`
	Distributions []string `firestore:"distributions"`
}

func (d recordKeyDoc) isPlaceholder() bool { return d.RecordID == "" || d.RecordCreatedAt == nil }

// toMap omits every record field on placeholders so orderBy(recordCreatedAt)
// excludes them (a null value would still "exist").
func (d recordKeyDoc) toMap() map[string]any {
	m := map[string]any{
		"uri":       d.URI,
		"gen":       d.Gen,
		"parentGen": d.ParentGen,
		"ancestors": d.Ancestors,
	}
	if d.isPlaceholder() {
		return m
	}
	m["recordID"] = d.RecordID
	m["recordCreatedAt"] = *d.RecordCreatedAt
	m["cleanOnUpdate"] = d.CleanOnUpdate
	m["owner"] = d.Owner
	m["author"] = d.Author
	m["schema"] = d.Schema
	m["redirect"] = d.Redirect
	m["policies"] = d.Policies
	m["distributions"] = d.Distributions
	return m
}

// acks/{ackID} and ackeds/{ackID}
type ackDoc struct {
	From       string    `firestore:"from"`
	To         string    `firestore:"to"`
	Schema     string    `firestore:"schema"`
	DocumentID string    `firestore:"documentID"`
	Valid      bool      `firestore:"valid"`
	CreatedAt  time.Time `firestore:"createdAt"`
	CDate      time.Time `firestore:"cDate"`
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

// associations/{cdid}
type associationDoc struct {
	TargetKeyID string    `firestore:"targetKeyID"`
	TargetGen   string    `firestore:"targetGen"`
	Owner       string    `firestore:"owner"`
	Author      string    `firestore:"author"`
	Schema      string    `firestore:"schema"`
	Variant     string    `firestore:"variant"`
	Unique      string    `firestore:"unique"`
	CreatedAt   time.Time `firestore:"createdAt"`
	CDate       time.Time `firestore:"cDate"`
}

func (d associationDoc) toMap() map[string]any {
	return map[string]any{
		"targetKeyID": d.TargetKeyID,
		"targetGen":   d.TargetGen,
		"owner":       d.Owner,
		"author":      d.Author,
		"schema":      d.Schema,
		"variant":     d.Variant,
		"unique":      d.Unique,
		"createdAt":   d.CreatedAt,
		"cDate":       firestore.ServerTimestamp,
	}
}

// association_uniques/{unique}
type assocUniqueDoc struct {
	AssociationID string `firestore:"associationID"`
}

// entities/{ccid}
type entityDoc struct {
	Alias      string    `firestore:"alias"`
	Domain     string    `firestore:"domain"`
	Tag        string    `firestore:"tag"`
	DocumentID string    `firestore:"documentID"`
	CreatedAt  time.Time `firestore:"createdAt"`
	CDate      time.Time `firestore:"cDate"`
	MDate      time.Time `firestore:"mDate"`
}

// entity_metas/{ccid}
type entityMetaDoc struct {
	Inviter string    `firestore:"inviter"`
	Info    string    `firestore:"info"`
	CDate   time.Time `firestore:"cDate"`
	MDate   time.Time `firestore:"mDate"`
}

// servers/{fqdn}
type serverDoc struct {
	CSID        string    `firestore:"csID"`
	Tag         string    `firestore:"tag"`
	Layer       string    `firestore:"layer"`
	WellKnown   string    `firestore:"wellKnown"`
	CDate       time.Time `firestore:"cDate"`
	MDate       time.Time `firestore:"mDate"`
	LastScraped time.Time `firestore:"lastScraped"`
}

// subscriptions/{subscriptionID}
type subscriptionDoc struct {
	VendorID     string    `firestore:"vendorID"`
	Owner        string    `firestore:"owner"`
	Schemas      []string  `firestore:"schemas"`
	Prefixes     []string  `firestore:"prefixes"`
	Subscription string    `firestore:"subscription"`
	CDate        time.Time `firestore:"cDate"`
	MDate        time.Time `firestore:"mDate"`
}

// abuse_reports/{auto}
type abuseReportDoc struct {
	IP        string    `firestore:"ip"`
	Reporter  string    `firestore:"reporter"`
	TargetURI string    `firestore:"targetURI"`
	Body      string    `firestore:"body"`
	CDate     time.Time `firestore:"cDate"`
}

// strPtr maps the empty string back to the nil the domain uses for "unset".
func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// decode unmarshals a snapshot into v, wrapping the error with the doc path.
func decode(snap *firestore.DocumentSnapshot, v any) error {
	if err := snap.DataTo(v); err != nil {
		return fmt.Errorf("failed to decode %s: %w", snap.Ref.Path, err)
	}
	return nil
}

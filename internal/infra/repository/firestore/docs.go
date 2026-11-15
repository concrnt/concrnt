// Package firestore implements the repository interfaces on Cloud Firestore
// (Native mode): the stores beside the record store live here, the record
// store in the record subpackage, and the composite indexes every query
// needs are declared in indexes.go (the Firestore counterpart of a schema).
//
// Data model. What a read returns is stored on the
// document its access path lands on, so no read follows a pointer:
//
//   - commits/{cdid}: the commit log (document + proof), written once. A
//     commit names what it anchors (anchor = the collection, anchorID = the
//     cckv key / state id / ccid), so a ccfs lookup and the offline gc find
//     the dependent by id instead of by query.
//   - record_keys/{hash(uri)}: one document per key. A key holding a record
//     carries the record's signed document and proof next to the fields the
//     listings filter and order by; a placeholder (directory) key omits
//     every record field, which is how orderBy(recordCreatedAt) leaves it
//     out the way "record_created_at IS NOT NULL" does in SQL. Keys link to
//     their parent by URI (parentURI, the owner root for top-level keys)
//     and carry every strict ancestor URI for prefix queries. gen is the
//     random id of the key's incarnation: set when the key document is
//     created (placeholder included), kept by overwrites, gone with the
//     document.
//   - retained_versions/{cdid}: a marker for a record version that asked to
//     be retained and has been replaced since; the version itself is served
//     from commits by its ccfs URI.
//   - associations/{cdid}: the association with its signed document, bound
//     to its target's incarnation by targetGen. association_uniques/
//     {hash(targetURI, unique)} stands in for the unique constraint; a
//     marker counts only while its targetGen is the target's current one.
//   - acks/{hash(from,to,schema)}, ackeds/{...}: one state document per
//     triple, carrying the signed document it is anchored to while valid.
//   - entities/{ccid}: the entity with the signed document registering it.
//   - entity_metas, servers, subscriptions, abuse_reports,
//     aggregate_entries.
package firestore

import (
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/zeebo/xxh3"
)

const (
	ColCommits          = "commits"
	ColRecordKeys       = "record_keys"
	ColRetainedVersions = "retained_versions"
	ColAcks             = "acks"
	ColAckeds           = "ackeds"
	ColAssociations     = "associations"
	ColAssocUniques     = "association_uniques"
	ColEntities         = "entities"
	ColEntityMetas      = "entity_metas"
	ColServers          = "servers"
	ColSubscriptions    = "subscriptions"
	ColAbuseReports     = "abuse_reports"
	ColAggregateEntries = "aggregate_entries"
	ColMeta             = "_meta"
)

// HashID derives a Firestore document id from arbitrary strings (document
// ids may not contain '/' and are capped at 1500 bytes).
func HashID(parts ...string) string {
	h := xxh3.Hash128([]byte(strings.Join(parts, "\x1f"))).Bytes()
	return hex.EncodeToString(h[:])
}

func subscriptionID(vendorID, owner string) string { return HashID(vendorID, owner) }

// entity_metas/{ccid}
type entityMetaDoc struct {
	Inviter string    `firestore:"inviter"`
	Info    string    `firestore:"info"`
	CDate   time.Time `firestore:"cDate"`
	MDate   time.Time `firestore:"mDate"`
}

// servers/{fqdn}
type serverDoc struct {
	CSID      string    `firestore:"csID"`
	Tag       string    `firestore:"tag"`
	Layer     string    `firestore:"layer"`
	WellKnown string    `firestore:"wellKnown"`
	CDate     time.Time `firestore:"cDate"`
	MDate     time.Time `firestore:"mDate"`
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

// StrPtr maps the empty string back to the nil the domain uses for "unset".
func StrPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Deref is the inverse of StrPtr.
func Deref(s *string) string {
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

// upsert emulates INSERT ... ON CONFLICT DO UPDATE for a handful of fields:
// an existing document is patched (its cDate untouched), a missing one is
// created with cDate set unless the caller supplied one. Not transactional; used for rarely-written,
// last-writer-wins metadata only.
func upsert(ctx context.Context, ref *firestore.DocumentRef, fields map[string]any) error {
	updates := make([]firestore.Update, 0, len(fields))
	for k, v := range fields {
		updates = append(updates, firestore.Update{Path: k, Value: v})
	}
	_, err := ref.Update(ctx, updates)
	if err == nil {
		return nil
	}
	if !IsNotFound(err) {
		return err
	}
	created := maps.Clone(fields)
	if _, ok := created["cDate"]; !ok {
		created["cDate"] = firestore.ServerTimestamp
	}
	_, err = ref.Set(ctx, created)
	return err
}

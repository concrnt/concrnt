// Package firestore implements the repository interfaces on Cloud Firestore
// (Native mode): the stores beside the record store live here, the record
// store in the record subpackage, and the composite indexes every query
// needs are declared in indexes.go (the Firestore counterpart of a schema).
//
// Data model, one top-level collection per table of the SQL backends:
//
//   - commits/{cdid}: the immutable commit log (document + proof).
//   - records/{cdid}: per-version record metadata (policies, distributions).
//   - record_keys/{hash(uri)}: the mutable key index. A key doc carries the
//     denormalised fields of the record it points at, so listing and
//     chunkline queries never join; placeholder (directory) keys omit the
//     record fields entirely, which is how orderBy(recordCreatedAt) leaves
//     them out the way "record_created_at IS NOT NULL" does in SQL. Keys
//     link to their parent by URI (parentURI, the owner root for top-level
//     keys) and carry every strict ancestor URI for prefix queries.
//   - associations/{cdid} + association_uniques/{hash(unique)}: the second
//     collection stands in for the unique constraint.
//   - acks/{hash(from,to,schema)}, ackeds/{...}: one state doc per triple.
//   - entities, entity_metas, servers, subscriptions, abuse_reports,
//     aggregate_entries.
package firestore

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/zeebo/xxh3"
)

const (
	ColCommits          = "commits"
	ColRecords          = "records"
	ColRecordKeys       = "record_keys"
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
	created := make(map[string]any, len(fields)+1)
	for k, v := range fields {
		created[k] = v
	}
	if _, ok := created["cDate"]; !ok {
		created["cDate"] = firestore.ServerTimestamp
	}
	_, err = ref.Set(ctx, created)
	return err
}

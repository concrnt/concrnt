package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/database/models"
	"github.com/concrnt/concrnt/internal/testutil"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/schemas"
)

func TestRecordRepositoryWrites(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewRecordRepository(db)

	key := "cckv://con1owner/timeline/post-1"

	t.Run("create and update record", func(t *testing.T) {
		createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		distributions := []string{"cckv://con1channel/timeline"}
		oldSD := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:        "record",
			Key:         key,
			Value:       map[string]string{"body": "old"},
			Author:      "con1author",
			Schema:      "https://schema.example/post.json",
			CreatedAt:   createdAt,
			Distributes: &distributions,
		})
		onUpdate := "forget"
		withRepositoryTx(t, ctx, repo, "record-1-old", "127.0.0.1", oldSD, "con1owner", func(tx record.RepositoryTx) error {
			applied, err := repo.CreateRecord(ctx, tx, "record-1-old", key, "con1owner", "con1owner", "https://schema.example/post.json", &onUpdate, nil, distributions, nil, createdAt)
			require.True(t, applied)
			return err
		})

		var commit models.CommitLog
		require.NoError(t, db.Where("id = ?", "record-1-old").Take(&commit).Error)
		require.Equal(t, oldSD.Document, commit.Document)
		require.JSONEq(t, `{"type":"none"}`, commit.Proof)
		require.False(t, commit.GcCandidate)

		requireCommitOwner(t, db, "record-1-old", "con1owner")

		var stored models.Record
		require.NoError(t, db.Where("document_id = ?", "record-1-old").Take(&stored).Error)
		require.Equal(t, "con1owner", stored.Owner)
		require.Equal(t, "https://schema.example/post.json", stored.Schema)
		require.Equal(t, distributions, []string(stored.Distributions))
		require.True(t, stored.CreatedAt.Equal(createdAt))

		var recordKey models.RecordKey
		require.NoError(t, db.Where("uri = ?", key).Take(&recordKey).Error)
		require.NotNil(t, recordKey.RecordID)
		require.Equal(t, "record-1-old", *recordKey.RecordID)
		require.NotNil(t, recordKey.RecordCreatedAt)
		require.True(t, recordKey.RecordCreatedAt.Equal(createdAt))

		gotSD, err := repo.GetSignedDocument(ctx, key)
		require.NoError(t, err)
		require.Equal(t, oldSD.Document, gotSD.Document)
		require.Equal(t, oldSD.Proof, gotSD.Proof)

		newCreatedAt := time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC)
		newSD := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       key,
			Value:     map[string]string{"body": "new"},
			Author:    "con1author",
			Schema:    "https://schema.example/post.v2.json",
			CreatedAt: newCreatedAt,
		})
		withRepositoryTx(t, ctx, repo, "record-2-new", "127.0.0.1", newSD, "con1owner", func(tx record.RepositoryTx) error {
			applied, err := repo.CreateRecord(ctx, tx, "record-2-new", key, "con1owner", "con1owner", "https://schema.example/post.v2.json", &onUpdate, nil, []string{}, nil, newCreatedAt)
			require.True(t, applied)
			return err
		})

		require.NoError(t, db.Where("uri = ?", key).Take(&recordKey).Error)
		require.NotNil(t, recordKey.RecordID)
		require.Equal(t, "record-2-new", *recordKey.RecordID)
		require.NotNil(t, recordKey.RecordCreatedAt)
		require.True(t, recordKey.RecordCreatedAt.Equal(newCreatedAt))

		require.NoError(t, db.Where("id = ?", "record-1-old").Take(&commit).Error)
		require.True(t, commit.GcCandidate)

		var oldRecordCount int64
		require.NoError(t, db.Model(&models.Record{}).Where("document_id = ?", "record-1-old").Count(&oldRecordCount).Error)
		require.Zero(t, oldRecordCount)

		var newRecord models.Record
		require.NoError(t, db.Where("document_id = ?", "record-2-new").Take(&newRecord).Error)
		require.Equal(t, "https://schema.example/post.v2.json", newRecord.Schema)
		require.True(t, newRecord.CreatedAt.Equal(newCreatedAt))

		// accept-if-newer: a replayed older (or identical) document must not
		// roll the key back, and must not tombstone the stored newer version.
		tx, err := repo.BeginTx(ctx)
		require.NoError(t, err)
		applied, err := repo.CreateRecord(ctx, tx, "record-0-stale", key, "con1owner", "con1owner", "https://schema.example/post.json", &onUpdate, nil, distributions, nil, createdAt)
		require.NoError(t, err)
		require.False(t, applied)
		applied, err = repo.CreateRecord(ctx, tx, "record-2-new", key, "con1owner", "con1owner", "https://schema.example/post.v2.json", &onUpdate, nil, []string{}, nil, newCreatedAt)
		require.NoError(t, err)
		require.False(t, applied)
		require.NoError(t, tx.Rollback(ctx))

		var keptRecordKey models.RecordKey
		require.NoError(t, db.Where("uri = ?", key).Take(&keptRecordKey).Error)
		require.Equal(t, "record-2-new", *keptRecordKey.RecordID)
		var keptCommit models.CommitLog
		require.NoError(t, db.Where("id = ?", "record-2-new").Take(&keptCommit).Error)
		require.False(t, keptCommit.GcCandidate)
		var newRecordCount int64
		require.NoError(t, db.Model(&models.Record{}).Where("document_id = ?", "record-2-new").Count(&newRecordCount).Error)
		require.EqualValues(t, 1, newRecordCount)
	})

	// CIP-1 §5.7: omitting onUpdate means forget — replacing the key must drop
	// the old record and mark its commit for GC. Only an explicit "retain"
	// keeps the old generation.
	t.Run("onUpdate defaults to forget, explicit retain keeps history", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			onUpdate *string
			forgets  bool
		}{
			{"default", nil, true},
			{"retain", ptr("retain"), false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				key := "cckv://con1owner/timeline/onupdate-" + tc.name
				oldID := "onupdate-" + tc.name + "-1-old"
				newID := "onupdate-" + tc.name + "-2-new"
				for i, id := range []string{oldID, newID} {
					createdAt := time.Date(2026, 1, 2, 3, 4, 5+i, 0, time.UTC)
					sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
						Kind:      "record",
						Key:       key,
						Value:     map[string]string{"body": id},
						Author:    "con1author",
						Schema:    "https://schema.example/post.json",
						CreatedAt: createdAt,
						OnUpdate:  tc.onUpdate,
					})
					withRepositoryTx(t, ctx, repo, id, "127.0.0.1", sd, "con1owner", func(tx record.RepositoryTx) error {
						applied, err := repo.CreateRecord(ctx, tx, id, key, "con1owner", "con1owner", "https://schema.example/post.json", tc.onUpdate, nil, []string{}, nil, createdAt)
						require.True(t, applied)
						return err
					})
				}

				var recordKey models.RecordKey
				require.NoError(t, db.Where("uri = ?", key).Take(&recordKey).Error)
				require.NotNil(t, recordKey.RecordID)
				require.Equal(t, newID, *recordKey.RecordID)

				var commit models.CommitLog
				require.NoError(t, db.Where("id = ?", oldID).Take(&commit).Error)
				require.Equal(t, tc.forgets, commit.GcCandidate)

				var oldRecordCount int64
				require.NoError(t, db.Model(&models.Record{}).Where("document_id = ?", oldID).Count(&oldRecordCount).Error)
				if tc.forgets {
					require.Zero(t, oldRecordCount)
				} else {
					require.EqualValues(t, 1, oldRecordCount)
				}
			})
		}
	})

	// CIP-0 §8.4: entities are exempt from onUpdate — replacing an entity
	// document must keep the old commit intact (no GC flag), even when the
	// document itself declares onUpdate: forget.
	t.Run("entity replacement retains the old document", func(t *testing.T) {
		for i, id := range []string{"entity-1-old", "entity-2-new"} {
			createdAt := time.Date(2026, 1, 2, 3, 4, 5+i, 0, time.UTC)
			sd := repositorySignedDocument(t, concrnt.Document[schemas.Entity]{
				Kind:      "entity",
				Value:     schemas.Entity{Domain: "example.com"},
				Author:    "con1entity",
				Schema:    schemas.EntityURL,
				CreatedAt: createdAt,
				OnUpdate:  ptr("forget"),
			})
			withRepositoryTx(t, ctx, repo, id, "127.0.0.1", sd, "con1entity", func(tx record.RepositoryTx) error {
				applied, err := repo.CreateEntity(ctx, tx, "con1entity", nil, "example.com", id, createdAt)
				require.True(t, applied)
				return err
			})
		}

		var entity models.Entity
		require.NoError(t, db.Where("id = ?", "con1entity").Take(&entity).Error)
		require.Equal(t, "entity-2-new", entity.DocumentID)
		require.True(t, entity.CreatedAt.Equal(time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)))

		var oldCommit models.CommitLog
		require.NoError(t, db.Where("id = ?", "entity-1-old").Take(&oldCommit).Error)
		require.False(t, oldCommit.GcCandidate)

		// CIP-3 §3.4: the key is createdAt alone — an equal-createdAt
		// document is a no-op whatever its id, an older one too
		tx, err := repo.BeginTx(ctx)
		require.NoError(t, err)
		require.NoError(t, repo.CreateCommitLog(ctx, tx, "entity-9-same-time", "127.0.0.1", "{}", concrnt.Proof{Type: concrnt.ProofTypeNone}, "con1entity"))
		applied, err := repo.CreateEntity(ctx, tx, "con1entity", nil, "other.example.net", "entity-9-same-time", time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC))
		require.NoError(t, err)
		require.False(t, applied, "same createdAt must be a no-op")
		applied, err = repo.CreateEntity(ctx, tx, "con1entity", nil, "other.example.net", "entity-0-older", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
		require.NoError(t, err)
		require.False(t, applied, "older createdAt must be a no-op")
		require.NoError(t, tx.Rollback(ctx))
		require.NoError(t, db.Where("id = ?", "con1entity").Take(&entity).Error)
		require.Equal(t, "example.com", entity.Domain)
	})

	t.Run("create reference record", func(t *testing.T) {
		targetURI := "cckv://con1target/timeline/post-2"
		targetCreatedAt := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
		targetSD := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       targetURI,
			Value:     map[string]string{"body": "target"},
			Author:    "con1target",
			Schema:    "https://schema.example/target.json",
			CreatedAt: targetCreatedAt,
		})

		refSD := repositorySignedDocument(t, concrnt.Document[schemas.Reference]{
			Kind: "record",
			Key:  "cckv://con1owner/timeline/ref-1",
			Value: schemas.Reference{
				Href: targetURI,
			},
			Author:    "con1owner",
			Schema:    schemas.ReferenceURL,
			CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		})
		refSD.References = map[string]concrnt.SignedDocument{
			targetURI: targetSD,
		}

		withRepositoryTx(t, ctx, repo, "reference-record", "127.0.0.1", refSD, "con1owner", func(tx record.RepositoryTx) error {
			_, err := repo.CreateRecord(ctx, tx, "reference-record", "cckv://con1owner/timeline/ref-1", "con1owner", "con1owner", "https://schema.example/target.json", nil, nil, []string{}, &targetURI, targetCreatedAt)
			return err
		})

		var stored models.Record
		require.NoError(t, db.Where("document_id = ?", "reference-record").Take(&stored).Error)
		require.NotNil(t, stored.Redirect)
		require.Equal(t, targetURI, *stored.Redirect)
		require.Equal(t, "https://schema.example/target.json", stored.Schema)
		require.True(t, stored.CreatedAt.Equal(targetCreatedAt))
	})

	t.Run("timeline removal tuple matches chunkline body IDs", func(t *testing.T) {
		chunklineRepo := NewChunklineRepository(db)

		// plain record: item ID is the record key URI
		timeline, itemID, err := repo.GetTimelineRemoval(ctx, key)
		require.NoError(t, err)
		require.Equal(t, "cckv://con1owner/timeline", timeline)
		require.Equal(t, key, itemID)

		// reference record: item ID is the redirect target
		refKey := "cckv://con1owner/timeline/ref-1"
		refTarget := "cckv://con1target/timeline/post-2"
		timeline, itemID, err = repo.GetTimelineRemoval(ctx, refKey)
		require.NoError(t, err)
		require.Equal(t, "cckv://con1owner/timeline", timeline)
		require.Equal(t, refTarget, itemID)

		// the advertised ID must equal the BodyItem.ID() the body endpoint
		// serves for the same member, or readers can't match them up
		for keyURI, wantID := range map[string]string{key: key, refKey: refTarget} {
			var recordKey models.RecordKey
			require.NoError(t, db.Where("uri = ?", keyURI).Take(&recordKey).Error)
			require.NotNil(t, recordKey.RecordCreatedAt)
			chunkID := recordKey.RecordCreatedAt.Unix() / 600

			items, err := chunklineRepo.LoadLocalBody(ctx, "cckv://con1owner/timeline", chunkID)
			require.NoError(t, err)
			ids := make([]string, 0, len(items))
			for _, item := range items {
				ids = append(ids, item.ID())
			}
			require.Contains(t, ids, wantID)
		}

		// the timeline row itself is not a chunkline member
		timeline, itemID, err = repo.GetTimelineRemoval(ctx, "cckv://con1owner/timeline")
		require.NoError(t, err)
		require.Empty(t, timeline)
		require.Empty(t, itemID)

		// unknown keys report not-found
		_, _, err = repo.GetTimelineRemoval(ctx, "cckv://con1owner/timeline/no-such-key")
		require.Error(t, err)
	})

	t.Run("create association and toggle ack", func(t *testing.T) {
		variant := "reply"
		associationSD := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:               "association",
			Value:              map[string]string{"body": "comment"},
			Author:             "con1author",
			Schema:             "https://schema.example/comment.json",
			CreatedAt:          time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC),
			Associate:          &key,
			AssociationVariant: &variant,
		})
		associationUnique := fmt.Sprintf("%x", xxh3.HashString("con1owner"+"con1author"+key+variant))
		associationCreatedAt := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
		var inserted bool
		withRepositoryTx(t, ctx, repo, "association-record", "127.0.0.1", associationSD, "con1owner", func(tx record.RepositoryTx) error {
			var err error
			inserted, err = repo.CreateAssociation(ctx, tx, "association-record", key, "con1owner", "con1author", "https://schema.example/comment.json", &variant, associationUnique, associationCreatedAt)
			return err
		})
		require.True(t, inserted)

		var association models.Association
		require.NoError(t, db.Where("document_id = ?", "association-record").Take(&association).Error)
		require.Equal(t, "con1owner", association.Owner)
		require.Equal(t, "con1author", association.Author)
		require.Equal(t, variant, *association.Variant)
		require.Equal(t, associationUnique, association.Unique)
		requireCommitOwner(t, db, "association-record", "con1owner")

		// duplicate deliveries are silent no-ops: the same document re-sent
		// (primary-key conflict)...
		withRepositoryTx(t, ctx, repo, "association-record", "127.0.0.1", associationSD, "con1owner", func(tx record.RepositoryTx) error {
			var err error
			inserted, err = repo.CreateAssociation(ctx, tx, "association-record", key, "con1owner", "con1author", "https://schema.example/comment.json", &variant, associationUnique, associationCreatedAt)
			return err
		})
		require.False(t, inserted)

		// ...and the same logical association re-signed under a new document id
		// (unique-key conflict)
		withRepositoryTx(t, ctx, repo, "association-record-retry", "127.0.0.1", associationSD, "con1owner", func(tx record.RepositoryTx) error {
			var err error
			inserted, err = repo.CreateAssociation(ctx, tx, "association-record-retry", key, "con1owner", "con1author", "https://schema.example/comment.json", &variant, associationUnique, associationCreatedAt)
			return err
		})
		require.False(t, inserted)

		var associationCount int64
		require.NoError(t, db.Model(&models.Association{}).Where(`"unique" = ?`, associationUnique).Count(&associationCount).Error)
		require.EqualValues(t, 1, associationCount)

		ackSchema := "https://schema.example/like.json"
		ackCreatedAt := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
		ackDoc := func(kind string, createdAt time.Time) concrnt.SignedDocument {
			return repositorySignedDocument(t, concrnt.Document[map[string]string]{
				Kind:      kind,
				Value:     map[string]string{"context": "like"},
				Author:    "con1author",
				Schema:    ackSchema,
				CreatedAt: createdAt,
				Associate: &key,
			})
		}
		ackState := func() models.Ack {
			t.Helper()
			var ack models.Ack
			require.NoError(t, db.Where(`"from" = ? AND "to" = ? AND schema = ?`, "con1author", "con1owner", ackSchema).Take(&ack).Error)
			return ack
		}
		// attempt runs a transition in a transaction that is always rolled
		// back, so only the reported applied flag matters
		attempt := func(id string, unack bool, createdAt time.Time) bool {
			t.Helper()
			kind := "ack"
			if unack {
				kind = "unack"
			}
			sd := ackDoc(kind, createdAt)
			tx, err := repo.BeginTx(ctx)
			require.NoError(t, err)
			require.NoError(t, repo.CreateCommitLog(ctx, tx, id, "127.0.0.1", sd.Document, sd.Proof, "con1author"))
			transition := repo.Acknowledge
			if unack {
				transition = repo.UnAcknowledge
			}
			applied, err := transition(ctx, tx, id, "con1author", "con1owner", ackSchema, createdAt)
			require.NoError(t, err)
			require.NoError(t, tx.Rollback(ctx))
			return applied
		}

		withRepositoryTx(t, ctx, repo, "ack-1-on", "127.0.0.1", ackDoc("ack", ackCreatedAt), "con1author", func(tx record.RepositoryTx) error {
			applied, err := repo.Acknowledge(ctx, tx, "ack-1-on", "con1author", "con1owner", ackSchema, ackCreatedAt)
			require.True(t, applied)
			return err
		})
		ack := ackState()
		require.True(t, ack.Valid)
		require.Equal(t, "ack-1-on", ack.DocumentID)
		// the ack commit is the acker's alone (CIP-10 §5); the target side is
		// held as an acked commit, see TestAckedRepositoryAcceptIfNewer
		requireCommitOwner(t, db, "ack-1-on", "con1author")

		// CIP-10 §4: the transition key is createdAt alone. An unack with the
		// same createdAt is not strictly newer and must no-op even though its
		// document id sorts above the stored one.
		require.False(t, attempt("ack-9-same-time", true, ackCreatedAt), "same createdAt must be a no-op: the transition key is createdAt, not the document id")
		require.Equal(t, "ack-1-on", ackState().DocumentID)

		unackCreatedAt := ackCreatedAt.Add(time.Hour)
		withRepositoryTx(t, ctx, repo, "ack-2-off", "127.0.0.1", ackDoc("unack", unackCreatedAt), "con1author", func(tx record.RepositoryTx) error {
			applied, err := repo.UnAcknowledge(ctx, tx, "ack-2-off", "con1author", "con1owner", ackSchema, unackCreatedAt)
			require.True(t, applied)
			return err
		})
		unack := ackState()
		require.False(t, unack.Valid)
		require.Equal(t, "ack-2-off", unack.DocumentID)
		requireCommitOwner(t, db, "ack-2-off", "con1author")

		// a replayed ack older than the stored unack must not resurrect it...
		require.False(t, attempt("ack-0-stale", false, ackCreatedAt))
		afterStaleAck := ackState()
		require.False(t, afterStaleAck.Valid)
		require.Equal(t, "ack-2-off", afterStaleAck.DocumentID)

		// ...and after a newer ack (t2), an unack replayed from before it (t1)
		// must not roll the state back either.
		reackCreatedAt := ackCreatedAt.Add(24 * time.Hour)
		withRepositoryTx(t, ctx, repo, "ack-3-on", "127.0.0.1", ackDoc("ack", reackCreatedAt), "con1author", func(tx record.RepositoryTx) error {
			applied, err := repo.Acknowledge(ctx, tx, "ack-3-on", "con1author", "con1owner", ackSchema, reackCreatedAt)
			require.True(t, applied)
			return err
		})
		require.False(t, attempt("ack-2x-stale", true, unackCreatedAt))
		afterStaleUnack := ackState()
		require.True(t, afterStaleUnack.Valid)
		require.Equal(t, "ack-3-on", afterStaleUnack.DocumentID)
		require.True(t, afterStaleUnack.CreatedAt.Equal(reackCreatedAt))
	})

	t.Run("rollback removes commit and record", func(t *testing.T) {
		rollbackKey := "cckv://con1owner/timeline/rollback"
		rollbackSD := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       rollbackKey,
			Value:     map[string]string{"body": "rollback"},
			Author:    "con1author",
			Schema:    "https://schema.example/post.json",
			CreatedAt: time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC),
		})

		tx, err := repo.BeginTx(ctx)
		require.NoError(t, err)
		require.NoError(t, repo.CreateCommitLog(ctx, tx, "rollback-record", "127.0.0.1", rollbackSD.Document, rollbackSD.Proof, "con1owner"))
		_, err = repo.CreateRecord(ctx, tx, "rollback-record", rollbackKey, "con1owner", "con1owner", "https://schema.example/post.json", nil, nil, []string{}, nil, time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC))
		require.NoError(t, err)
		require.NoError(t, tx.Rollback(ctx))

		var count int64
		require.NoError(t, db.Model(&models.CommitLog{}).Where("id = ?", "rollback-record").Count(&count).Error)
		require.Zero(t, count)
		require.NoError(t, db.Model(&models.Record{}).Where("document_id = ?", "rollback-record").Count(&count).Error)
		require.Zero(t, count)
	})

	t.Run("delete removes record payload and keeps commits", func(t *testing.T) {
		deleteKey := "cckv://con1owner/timeline/delete-target"
		deleteTargetSD := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       deleteKey,
			Value:     map[string]string{"body": "delete target"},
			Author:    "con1author",
			Schema:    "https://schema.example/post.json",
			CreatedAt: time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC),
		})
		withRepositoryTx(t, ctx, repo, "delete-target", "127.0.0.1", deleteTargetSD, "con1owner", func(tx record.RepositoryTx) error {
			_, err := repo.CreateRecord(ctx, tx, "delete-target", deleteKey, "con1owner", "con1owner", "https://schema.example/post.json", nil, nil, []string{}, nil, time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC))
			return err
		})

		deleteSD := repositorySignedDocument(t, concrnt.Document[schemas.Delete]{
			Kind:      "delete",
			Value:     schemas.Delete(deleteKey),
			Author:    "con1author",
			CreatedAt: time.Date(2026, 6, 8, 8, 9, 10, 0, time.UTC),
		})
		withRepositoryTx(t, ctx, repo, "delete-commit", "127.0.0.1", deleteSD, "con1owner", func(tx record.RepositoryTx) error {
			return repo.DeleteRecordByKey(ctx, tx, deleteKey)
		})

		var count int64
		require.NoError(t, db.Model(&models.Record{}).Where("document_id = ?", "delete-target").Count(&count).Error)
		require.Zero(t, count)
		require.NoError(t, db.Model(&models.CommitLog{}).Where("id = ?", "delete-target").Count(&count).Error)
		require.EqualValues(t, 1, count)

		var commit models.CommitLog
		require.NoError(t, db.Where("id = ?", "delete-commit").Take(&commit).Error)
		require.Equal(t, deleteSD.Document, commit.Document)
		requireCommitOwner(t, db, "delete-commit", "con1owner")
	})

	t.Run("mark commit log gc candidate", func(t *testing.T) {
		sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       "cckv://con1owner/timeline/gc-flag",
			Value:     map[string]string{"body": "flag me"},
			Author:    "con1owner",
			Schema:    "https://schema.example/post.json",
			CreatedAt: time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC),
		})
		withRepositoryTx(t, ctx, repo, "gc-flag", "127.0.0.1", sd, "con1owner", func(tx record.RepositoryTx) error {
			return nil
		})

		var commit models.CommitLog
		require.NoError(t, db.Where("id = ?", "gc-flag").Take(&commit).Error)
		require.False(t, commit.GcCandidate)

		tx, err := repo.BeginTx(ctx)
		require.NoError(t, err)
		require.NoError(t, repo.MarkCommitLogGcCandidate(ctx, tx, "gc-flag"))
		require.NoError(t, repo.MarkCommitLogGcCandidate(ctx, tx, "no-such-commit"))
		require.NoError(t, tx.Commit(ctx))

		require.NoError(t, db.Where("id = ?", "gc-flag").Take(&commit).Error)
		require.True(t, commit.GcCandidate)

		err = repo.MarkCommitLogGcCandidate(ctx, nil, "gc-flag")
		require.Error(t, err)
	})

	t.Run("commit log methods marshal proof and ignore conflicts", func(t *testing.T) {
		href := "cckv://con1owner/timeline/conflict"
		proof := concrnt.Proof{
			Type: concrnt.ProofTypeNone,
			Href: &href,
		}

		tx, err := repo.BeginTx(ctx)
		require.NoError(t, err)
		require.NoError(t, repo.CreateCommitLog(ctx, tx, "commit-methods", "127.0.0.1", "first", proof, "con1owner"))
		// a re-delivery of the same id is ignored wholesale: document, proof
		// and owner all stay as first written
		require.NoError(t, repo.CreateCommitLog(ctx, tx, "commit-methods", "192.0.2.1", "second", concrnt.Proof{Type: "ignored"}, "con1other"))
		require.NoError(t, tx.Commit(ctx))

		var commit models.CommitLog
		require.NoError(t, db.Where("id = ?", "commit-methods").Take(&commit).Error)
		require.Equal(t, "127.0.0.1", commit.IP)
		require.Equal(t, "first", commit.Document)
		require.JSONEq(t, `{"type":"none","href":"cckv://con1owner/timeline/conflict"}`, commit.Proof)
		require.Equal(t, "con1owner", commit.Owner)
	})

	t.Run("write methods reject invalid tx", func(t *testing.T) {
		err := repo.CreateCommitLog(ctx, nil, "invalid", "127.0.0.1", "{}", concrnt.Proof{Type: concrnt.ProofTypeNone}, "con1owner")
		require.Error(t, err)

		err = repo.CreateCommitLog(ctx, fakeRecordTx{}, "invalid", "127.0.0.1", "{}", concrnt.Proof{Type: concrnt.ProofTypeNone}, "con1owner")
		require.Error(t, err)

		_, err = repo.Acknowledge(ctx, nil, "invalid", "con1author", "con1owner", "https://schema.example/like.json", time.Now())
		require.Error(t, err)

		_, err = repo.Acknowledge(ctx, fakeRecordTx{}, "invalid", "con1author", "con1owner", "https://schema.example/like.json", time.Now())
		require.Error(t, err)

		_, err = repo.Acknowledged(ctx, nil, "invalid", "con1author", "con1owner", "https://schema.example/like.json", time.Now())
		require.Error(t, err)

		_, err = repo.Acknowledged(ctx, fakeRecordTx{}, "invalid", "con1author", "con1owner", "https://schema.example/like.json", time.Now())
		require.Error(t, err)

		_, err = repo.CreateRecord(ctx, nil, "invalid", key, "con1owner", "con1owner", "https://schema.example/post.json", nil, nil, []string{}, nil, time.Now())
		require.Error(t, err)

		_, err = repo.CreateRecord(ctx, fakeRecordTx{}, "invalid", key, "con1owner", "con1owner", "https://schema.example/post.json", nil, nil, []string{}, nil, time.Now())
		require.Error(t, err)

		err = repo.DeleteRecordByKey(ctx, nil, key)
		require.Error(t, err)

		err = repo.DeleteRecordByKey(ctx, fakeRecordTx{}, key)
		require.Error(t, err)

		err = repo.DeleteRecordByDocumentID(ctx, nil, "invalid")
		require.Error(t, err)

		err = repo.DeleteRecordByDocumentID(ctx, fakeRecordTx{}, "invalid")
		require.Error(t, err)

		err = repo.DeleteAssociation(ctx, nil, "invalid")
		require.Error(t, err)

		err = repo.DeleteAssociation(ctx, fakeRecordTx{}, "invalid")
		require.Error(t, err)
	})
}

// QueryRecordSubtree must match by path hierarchy — never siblings sharing a
// string prefix, never keys that only match via unescaped LIKE metacharacters —
// and deleting the enumerated keys must remove exactly those records (with
// their associations) while leaving siblings and commitlogs in place.
func TestRecordSubtreeQueryAndDelete(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewRecordRepository(db)

	base := "cckv://con1owner/lists/item"
	keys := []string{
		base,                             // the base record itself
		base + "/a",                      // child
		base + "/a/b",                    // grandchild
		base + "2",                       // sibling sharing the string prefix — never in the subtree
		"cckv://con1owner/lists/it_em/x", // LIKE-metacharacter decoy: '_' matches any char unescaped
	}
	createdAt := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)
	for i, key := range keys {
		id := fmt.Sprintf("subtree-%d", i)
		sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       key,
			Value:     map[string]string{"body": key},
			Author:    "con1owner",
			Schema:    "https://schema.example/item.json",
			CreatedAt: createdAt,
		})
		withRepositoryTx(t, ctx, repo, id, "127.0.0.1", sd, "con1owner", func(tx record.RepositoryTx) error {
			_, err := repo.CreateRecord(ctx, tx, id, key, "con1owner", "con1owner", "https://schema.example/item.json", nil, nil, []string{}, nil, createdAt)
			return err
		})
	}

	// an association targeting a subtree member must cascade with the delete
	variant := "reply"
	associationSD := repositorySignedDocument(t, concrnt.Document[map[string]string]{
		Kind:      "association",
		Value:     map[string]string{"body": "on child"},
		Author:    "con1author",
		Schema:    "https://schema.example/comment.json",
		CreatedAt: createdAt,
		Associate: &keys[1],
	})
	withRepositoryTx(t, ctx, repo, "subtree-association", "127.0.0.1", associationSD, "con1owner", func(tx record.RepositoryTx) error {
		_, err := repo.CreateAssociation(ctx, tx, "subtree-association", keys[1], "con1owner", "con1author", "https://schema.example/comment.json", &variant, "subtree-unique", createdAt)
		return err
	})

	urisOf := func(sds []concrnt.SignedDocument) []string {
		uris := make([]string, 0, len(sds))
		for _, sd := range sds {
			require.NotNil(t, sd.CCKV)
			require.NotNil(t, sd.CCFS)
			uris = append(uris, *sd.CCKV)
		}
		return uris
	}

	t.Run("subtree including self", func(t *testing.T) {
		sds, err := repo.QueryRecordSubtree(ctx, base, true)
		require.NoError(t, err)
		require.Equal(t, []string{base, base + "/a", base + "/a/b"}, urisOf(sds))
	})

	t.Run("children only", func(t *testing.T) {
		sds, err := repo.QueryRecordSubtree(ctx, base, false)
		require.NoError(t, err)
		require.Equal(t, []string{base + "/a", base + "/a/b"}, urisOf(sds))
	})

	t.Run("metacharacters in base are escaped", func(t *testing.T) {
		// unescaped, 'it_em' would also match 'it/em', 'item', ... — it must
		// only match its own literal subtree
		sds, err := repo.QueryRecordSubtree(ctx, "cckv://con1owner/lists/it_em", true)
		require.NoError(t, err)
		require.Equal(t, []string{"cckv://con1owner/lists/it_em/x"}, urisOf(sds))
	})

	t.Run("subtree delete removes exactly the enumerated keys", func(t *testing.T) {
		sds, err := repo.QueryRecordSubtree(ctx, base, true)
		require.NoError(t, err)

		deleteSD := repositorySignedDocument(t, concrnt.Document[schemas.Delete]{
			Kind:      "delete",
			Value:     schemas.Delete(base + "*"),
			Author:    "con1owner",
			CreatedAt: createdAt,
		})
		withRepositoryTx(t, ctx, repo, "subtree-delete", "127.0.0.1", deleteSD, "con1owner", func(tx record.RepositoryTx) error {
			for _, uri := range urisOf(sds) {
				if err := repo.DeleteRecordByKey(ctx, tx, uri); err != nil {
					return err
				}
			}
			return nil
		})

		// the subtree is gone: records and their key rows
		var count int64
		for i := range 3 {
			require.NoError(t, db.Model(&models.Record{}).Where("document_id = ?", fmt.Sprintf("subtree-%d", i)).Count(&count).Error)
			require.Zero(t, count, "record %d must be deleted", i)
		}
		require.NoError(t, db.Model(&models.RecordKey{}).Where("uri IN ?", []string{base, base + "/a", base + "/a/b"}).Where("record_id IS NOT NULL").Count(&count).Error)
		require.Zero(t, count)

		// the association on the deleted child cascaded away
		require.NoError(t, db.Model(&models.Association{}).Where("document_id = ?", "subtree-association").Count(&count).Error)
		require.Zero(t, count)

		// siblings and decoys survive
		for i := 3; i < 5; i++ {
			require.NoError(t, db.Model(&models.Record{}).Where("document_id = ?", fmt.Sprintf("subtree-%d", i)).Count(&count).Error)
			require.EqualValues(t, 1, count, "record %d must survive", i)
		}

		// commitlogs of the deleted records stay (delete never erases history)
		for i := range 3 {
			require.NoError(t, db.Model(&models.CommitLog{}).Where("id = ?", fmt.Sprintf("subtree-%d", i)).Count(&count).Error)
			require.EqualValues(t, 1, count, "commitlog %d must survive", i)
		}
	})

}

// The query API's author filter narrows list queries to documents written by
// one author — e.g. an AP outbox enumerating only the user's own posts out of
// their timeline. All keys in a timeline live in the timeline owner's space,
// so the discriminating column is the document author (which distribute
// references preserve: the document-reference proof forces the reference
// document's author to match the target's).
func TestRecordQueryAuthorFilter(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewRecordRepository(db)

	parent := "cckv://con1owner/tl"
	entries := []struct {
		key    string
		author string
	}{
		{parent + "/e1", "con1alice"},
		{parent + "/e2", "con1bob"},
		{parent + "/e3", "con1alice"},
	}
	createdAt := time.Date(2026, 8, 14, 9, 10, 11, 0, time.UTC)
	for i, e := range entries {
		id := fmt.Sprintf("authorfilter-%d", i)
		sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       e.key,
			Value:     map[string]string{"body": e.key},
			Author:    e.author,
			Schema:    "https://schema.example/item.json",
			CreatedAt: createdAt.Add(time.Duration(i) * time.Second),
		})
		withRepositoryTx(t, ctx, repo, id, "127.0.0.1", sd, "con1owner", func(tx record.RepositoryTx) error {
			_, err := repo.CreateRecord(ctx, tx, id, e.key, "con1owner", e.author, "https://schema.example/item.json", nil, nil, []string{}, nil, createdAt.Add(time.Duration(i)*time.Second))
			return err
		})
	}

	urisOfRows := func(rows []record.QueryRow) []string {
		uris := make([]string, 0, len(rows))
		for _, row := range rows {
			require.NotNil(t, row.Row.CCKV)
			uris = append(uris, *row.Row.CCKV)
		}
		return uris
	}

	t.Run("by parent with author", func(t *testing.T) {
		rows, err := repo.QueryByParent(ctx, parent, "", "con1alice", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/e1", parent + "/e3"}, urisOfRows(rows))
	})

	t.Run("by parent without author returns all", func(t *testing.T) {
		rows, err := repo.QueryByParent(ctx, parent, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/e1", parent + "/e2", parent + "/e3"}, urisOfRows(rows))
	})

	t.Run("by prefix with author", func(t *testing.T) {
		rows, err := repo.QueryByPrefix(ctx, parent+"/", "", "con1bob", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/e2"}, urisOfRows(rows))
	})
}

// CIP-12 §5.3: the hierarchical policy stack is emitted root-first with the
// target resource itself last.
func TestHierarchicalRecordPoliciesRootFirst(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewRecordRepository(db)

	policyJSON := `{"entries":[{"url":"https://example.com/p.json"}]}`
	keys := []string{
		"cckv://con1powner/l1",
		"cckv://con1powner/l1/l2",
		"cckv://con1powner/l1/l2/l3",
	}
	for i, key := range keys {
		id := fmt.Sprintf("policy-record-%d", i)
		createdAt := time.Date(2026, 6, 1, 0, 0, i, 0, time.UTC)
		sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       key,
			Value:     map[string]string{"body": "x"},
			Author:    "con1powner",
			Schema:    "https://schema.example/post.json",
			CreatedAt: createdAt,
		})
		p := policyJSON
		withRepositoryTx(t, ctx, repo, id, "127.0.0.1", sd, "con1powner", func(tx record.RepositoryTx) error {
			applied, err := repo.CreateRecord(ctx, tx, id, key, "con1powner", "con1powner", "https://schema.example/post.json", nil, &p, []string{}, nil, createdAt)
			require.True(t, applied)
			return err
		})
	}

	stack, err := repo.GetHierarchicalRecordPolicies(ctx, keys[2])
	require.NoError(t, err)

	sources := make([]string, len(stack))
	for i, layer := range stack {
		sources[i] = layer.Source
	}
	require.Equal(t, keys, sources, "layers must be emitted root-first, self last")
}

// A level with no policy of its own but with distributions must still be
// emitted — as an empty layer carrying its virtual parents — otherwise the
// destination timelines' policies never reach the stack (CIP-12 §5.3).
func TestHierarchicalRecordPoliciesEmitsPolicylessDistributingLevel(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewRecordRepository(db)

	policyJSON := `{"entries":[{"url":"https://example.com/p.json"}]}`
	parentKey := "cckv://con1downer/d1"
	leafKey := "cckv://con1downer/d1/post"
	timeline := "cckv://con1tl/timelines/home"

	for i, tc := range []struct {
		key           string
		policies      *string
		distributions []string
	}{
		{parentKey, &policyJSON, []string{}},
		{leafKey, nil, []string{timeline}},
	} {
		id := fmt.Sprintf("distributing-record-%d", i)
		createdAt := time.Date(2026, 6, 2, 0, 0, i, 0, time.UTC)
		sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       tc.key,
			Value:     map[string]string{"body": "x"},
			Author:    "con1downer",
			Schema:    "https://schema.example/post.json",
			CreatedAt: createdAt,
		})
		withRepositoryTx(t, ctx, repo, id, "127.0.0.1", sd, "con1downer", func(tx record.RepositoryTx) error {
			applied, err := repo.CreateRecord(ctx, tx, id, tc.key, "con1downer", "con1downer", "https://schema.example/post.json", nil, tc.policies, tc.distributions, nil, createdAt)
			require.True(t, applied)
			return err
		})
	}

	stack, err := repo.GetHierarchicalRecordPolicies(ctx, leafKey)
	require.NoError(t, err)

	require.Len(t, stack, 2)
	require.Equal(t, parentKey, stack[0].Source)
	require.Equal(t, leafKey, stack[1].Source)
	require.Empty(t, stack[1].Entries, "a policyless level is an empty layer")
	require.NotNil(t, stack[1].VirtualParents)
	require.Equal(t, []string{timeline}, *stack[1].VirtualParents)
}

// A parent placeholder row (record_id IS NULL) must not trip the conditional
// upsert: writing a record to a key that so far only exists as a placeholder
// parent must apply.
func TestCreateRecordAppliesOverParentPlaceholder(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewRecordRepository(db)

	parentKey := "cckv://con1ph/parent"
	childKey := "cckv://con1ph/parent/child"

	commit := func(id, key string, createdAt time.Time) {
		sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       key,
			Value:     map[string]string{"body": "x"},
			Author:    "con1ph",
			Schema:    "https://schema.example/post.json",
			CreatedAt: createdAt,
		})
		withRepositoryTx(t, ctx, repo, id, "127.0.0.1", sd, "con1ph", func(tx record.RepositoryTx) error {
			applied, err := repo.CreateRecord(ctx, tx, id, key, "con1ph", "con1ph", "https://schema.example/post.json", nil, nil, []string{}, nil, createdAt)
			require.True(t, applied)
			return err
		})
	}

	// committing the child materializes the parent as a placeholder
	commit("placeholder-child", childKey, time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC))

	var rk models.RecordKey
	require.NoError(t, db.Where("uri = ?", parentKey).Take(&rk).Error)
	require.Nil(t, rk.RecordID, "the parent must exist as a placeholder")

	commit("placeholder-parent", parentKey, time.Date(2026, 6, 3, 0, 0, 1, 0, time.UTC))

	rk = models.RecordKey{}
	require.NoError(t, db.Where("uri = ?", parentKey).Take(&rk).Error)
	require.NotNil(t, rk.RecordID)
	require.Equal(t, "placeholder-parent", *rk.RecordID)
}

// SELECT ... FOR UPDATE cannot lock a RecordKey row that does not exist yet,
// so two commits racing on a fresh key both pass the fast-path — the
// conditional ON CONFLICT clause is what keeps the newer document. The older
// commit must come back unapplied whether it lands while the newer one is
// still in flight (blocked on the unique index) or after it committed.
func TestCreateRecordFreshKeyConditionalUpsert(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewRecordRepository(db)

	key := "cckv://con1race/fresh"
	newID, newAt := "race-2-new", time.Date(2026, 6, 4, 0, 0, 1, 0, time.UTC)
	oldID, oldAt := "race-1-old", time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	signedFor := func(createdAt time.Time) concrnt.SignedDocument {
		return repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       key,
			Value:     map[string]string{"body": "x"},
			Author:    "con1race",
			Schema:    "https://schema.example/post.json",
			CreatedAt: createdAt,
		})
	}

	// the newer commit inserts the fresh RecordKey but holds its tx open, so
	// the older commit can neither see nor lock the row
	tx1, err := repo.BeginTx(ctx)
	require.NoError(t, err)
	sdNew := signedFor(newAt)
	require.NoError(t, repo.CreateCommitLog(ctx, tx1, newID, "127.0.0.1", sdNew.Document, sdNew.Proof, "con1owner"))
	applied, err := repo.CreateRecord(ctx, tx1, newID, key, "con1race", "con1race", "https://schema.example/post.json", nil, nil, []string{}, nil, newAt)
	require.NoError(t, err)
	require.True(t, applied)

	type outcome struct {
		applied bool
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		tx2, err := repo.BeginTx(ctx)
		if err != nil {
			done <- outcome{err: err}
			return
		}
		sdOld := signedFor(oldAt)
		if err := repo.CreateCommitLog(ctx, tx2, oldID, "127.0.0.1", sdOld.Document, sdOld.Proof, "con1owner"); err != nil {
			_ = tx2.Rollback(ctx)
			done <- outcome{err: err}
			return
		}
		applied, err := repo.CreateRecord(ctx, tx2, oldID, key, "con1race", "con1race", "https://schema.example/post.json", nil, nil, []string{}, nil, oldAt)
		if err != nil {
			_ = tx2.Rollback(ctx)
			done <- outcome{err: err}
			return
		}
		if applied {
			done <- outcome{applied: applied, err: tx2.Commit(ctx)}
			return
		}
		_ = tx2.Rollback(ctx)
		done <- outcome{applied: applied}
	}()

	// let the older commit reach the unique-index wait, then land the newer one
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, tx1.Commit(ctx))

	res := <-done
	require.NoError(t, res.err)
	require.False(t, res.applied, "the older document must lose the fresh-key race")

	var rk models.RecordKey
	require.NoError(t, db.Where("uri = ?", key).Take(&rk).Error)
	require.NotNil(t, rk.RecordID)
	require.Equal(t, newID, *rk.RecordID)
}

// withRepositoryTx runs fn inside a commit transaction that already holds the
// commit log for sd, recorded under owner (the single entity every commit is
// attributed to).
func withRepositoryTx(t *testing.T, ctx context.Context, repo record.Repository, id string, ip string, sd concrnt.SignedDocument, owner string, fn func(tx record.RepositoryTx) error) {
	t.Helper()

	tx, err := repo.BeginTx(ctx)
	require.NoError(t, err)
	require.NoError(t, repo.CreateCommitLog(ctx, tx, id, ip, sd.Document, sd.Proof, owner))
	if err := fn(tx); err != nil {
		require.NoError(t, tx.Rollback(ctx))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit(ctx))
}

func ptr[T any](v T) *T {
	return &v
}

type fakeRecordTx struct{}

func (fakeRecordTx) Commit(ctx context.Context) error {
	return nil
}

func (fakeRecordTx) Rollback(ctx context.Context) error {
	return nil
}

// requireCommitOwner asserts the single owner a commit log is recorded under.
func requireCommitOwner(t *testing.T, db *gorm.DB, commitID string, owner string) {
	t.Helper()

	var commit models.CommitLog
	require.NoError(t, db.Where("id = ?", commitID).Take(&commit).Error)
	require.Equal(t, owner, commit.Owner, "owner of commit %s", commitID)
}

func repositorySignedDocument(t *testing.T, doc any) concrnt.SignedDocument {
	t.Helper()

	docBytes, err := json.Marshal(doc)
	require.NoError(t, err)

	return concrnt.SignedDocument{
		Document: string(docBytes),
		Proof: concrnt.Proof{
			Type: concrnt.ProofTypeNone,
		},
	}
}

// The repository dump (/repository, conctl dump-commitlog) is scoped by the
// single owner recorded on each commit log, in commit order; other owners'
// and ownerless commits are excluded.
func TestGetAllCommitLogsByOwner(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewRecordRepository(db)

	userA, userB := "con1aaaa", "con1bbbb"
	seed := func(id, owner string) {
		t.Helper()
		sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       "cckv://" + owner + "/" + id,
			Value:     map[string]string{"id": id},
			Author:    owner,
			Schema:    "https://schema.example/post.json",
			CreatedAt: time.Now(),
		})
		tx, err := repo.BeginTx(ctx)
		require.NoError(t, err)
		require.NoError(t, repo.CreateCommitLog(ctx, tx, id, "127.0.0.1", sd.Document, sd.Proof, owner))
		require.NoError(t, tx.Commit(ctx))
	}
	seed("a-1", userA)
	seed("b-1", userB)
	seed("a-2", userA)
	seed("unowned", "")

	logs, err := repo.GetAllCommitLogs(ctx, userA)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.Contains(t, logs[0].Document, `"id":"a-1"`)
	require.Contains(t, logs[1].Document, `"id":"a-2"`)

	logs, err = repo.GetAllCommitLogs(ctx, userB)
	require.NoError(t, err)
	require.Len(t, logs, 1)

	logs, err = repo.GetAllCommitLogs(ctx, "con1nobody")
	require.NoError(t, err)
	require.Empty(t, logs)
}

// CIP-5 orderby=key: a parent listing in cckv order that keeps placeholder
// (intermediate) keys as key-only rows, pages by inclusive key bounds, drops
// placeholders under a record-column filter, and accepts the owner root.
func TestRecordQueryByParentOrderByKey(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)

	ctx := context.Background()
	repo := NewRecordRepository(db)

	owner := "con1keyorder"
	root := "cckv://" + owner
	parent := root + "/app"
	keys := []string{parent + "/b", parent + "/a", parent + "/posts/1"}
	createdAt := time.Date(2026, 9, 21, 9, 0, 0, 0, time.UTC)
	for i, key := range keys {
		id := fmt.Sprintf("keyorder-%d", i)
		sd := repositorySignedDocument(t, concrnt.Document[map[string]string]{
			Kind:      "record",
			Key:       key,
			Value:     map[string]string{"body": key},
			Author:    owner,
			Schema:    "https://schema.example/item.json",
			CreatedAt: createdAt.Add(time.Duration(i) * time.Second),
		})
		withRepositoryTx(t, ctx, repo, id, "127.0.0.1", sd, owner, func(tx record.RepositoryTx) error {
			_, err := repo.CreateRecord(ctx, tx, id, key, owner, owner, "https://schema.example/item.json", nil, nil, []string{}, nil, createdAt.Add(time.Duration(i)*time.Second))
			return err
		})
	}

	urisOfRows := func(rows []record.QueryRow) []string {
		uris := make([]string, 0, len(rows))
		for _, row := range rows {
			require.NotNil(t, row.Row.CCKV)
			uris = append(uris, *row.Row.CCKV)
		}
		return uris
	}

	t.Run("ascending with the placeholder as a key-only row", func(t *testing.T) {
		rows, err := repo.QueryByParentOrderByKey(ctx, parent, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/a", parent + "/b", parent + "/posts"}, urisOfRows(rows))
		require.NotEmpty(t, rows[0].Row.Document)
		require.NotNil(t, rows[0].Row.CCFS)
		require.Empty(t, rows[2].Row.Document, "a placeholder key carries no document")
		require.Nil(t, rows[2].Row.CCFS)
		require.Equal(t, concrnt.Proof{}, rows[2].Row.Proof)
	})

	t.Run("descending", func(t *testing.T) {
		rows, err := repo.QueryByParentOrderByKey(ctx, parent, "", "", nil, nil, 0, "desc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/posts", parent + "/b", parent + "/a"}, urisOfRows(rows))
	})

	t.Run("since and until are inclusive key bounds", func(t *testing.T) {
		since := parent + "/b"
		rows, err := repo.QueryByParentOrderByKey(ctx, parent, "", "", &since, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/b", parent + "/posts"}, urisOfRows(rows))

		until := parent + "/b"
		rows, err = repo.QueryByParentOrderByKey(ctx, parent, "", "", nil, &until, 0, "desc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/b", parent + "/a"}, urisOfRows(rows))

		rows, err = repo.QueryByParentOrderByKey(ctx, parent, "", "", nil, nil, 2, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/a", parent + "/b"}, urisOfRows(rows))
	})

	t.Run("schema filter drops placeholders", func(t *testing.T) {
		rows, err := repo.QueryByParentOrderByKey(ctx, parent, "https://schema.example/item.json", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/a", parent + "/b"}, urisOfRows(rows))

		rows, err = repo.QueryByParentOrderByKey(ctx, parent, "", "con1someoneelse", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows)
	})

	t.Run("owner root lists the top-level keys", func(t *testing.T) {
		rows, err := repo.QueryByParentOrderByKey(ctx, root, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent}, urisOfRows(rows))
		require.Empty(t, rows[0].Row.Document)

		rows, err = repo.QueryByParentOrderByKey(ctx, "cckv://con1keyorderx", "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows, "a sibling owner sharing the prefix must not match")
	})

	t.Run("createdAt ordering also accepts the owner root", func(t *testing.T) {
		rows, err := repo.QueryByParent(ctx, root, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Empty(t, rows, "top-level placeholder keys have no document to list")

		rows, err = repo.QueryByParent(ctx, parent, "", "", nil, nil, 0, "asc")
		require.NoError(t, err)
		require.Equal(t, []string{parent + "/b", parent + "/a"}, urisOfRows(rows), "placeholders stay out of the createdAt listing")
	})
}

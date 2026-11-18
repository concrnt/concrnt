package migrations

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	pgrecord "github.com/concrnt/concrnt/internal/infra/repository/postgres/record"
	"github.com/concrnt/concrnt/internal/testutil"
)

func ptr[T any](v T) *T { return &v }

// The layout before the record columns moved onto record_keys: a records
// table keyed by document id, pointed at by record_keys.record_id.
type legacyRecord struct {
	DocumentID    string             `gorm:"primaryKey;type:text"`
	Document      pgrecord.CommitLog `gorm:"foreignKey:DocumentID;references:ID;constraint:OnDelete:CASCADE;"`
	Owner         string             `gorm:"type:text"`
	Author        string             `gorm:"type:text"`
	Redirect      *string            `gorm:"type:text"`
	Schema        string             `gorm:"type:text"`
	Policies      *string            `gorm:"type:text"`
	Distributions pq.StringArray     `gorm:"type:text[]"`
	CreatedAt     time.Time          `gorm:"type:timestamp with time zone;not null"`
}

func (legacyRecord) TableName() string { return "records" }

type legacyRecordKey struct {
	ID              int64        `gorm:"primaryKey;autoIncrement"`
	ParentID        *int64       `gorm:"index:idx_record_keys_parent_id_record_id,priority:1"`
	URI             string       `gorm:"type:text;unique"`
	RecordID        *string      `gorm:"type:text;uniqueIndex;index:idx_record_keys_parent_id_record_id,priority:2"`
	Record          legacyRecord `gorm:"foreignKey:RecordID;references:DocumentID;constraint:OnDelete:CASCADE;"`
	RecordCreatedAt *time.Time   `gorm:"type:timestamp with time zone"`
	CleanOnUpdate   bool         `gorm:"type:boolean;not null;default:false"`
}

func (legacyRecordKey) TableName() string { return "record_keys" }

// foldRecordsIntoRecordKeys folds a database in the legacy layout: the record columns are
// copied onto the key rows, a retained version the key moved on from stays
// served by ccfs, a retained version that was deleted (no records row left)
// and the delete commits that kept it are flagged for gc, and record_keys
// stops referencing records (which itself is left for the operator).
func TestMigrateFoldsRecordsIntoRecordKeys(t *testing.T) {
	db, cleanup := testutil.CreateDB()
	t.Cleanup(cleanup)
	ctx := context.Background()

	require.NoError(t, db.AutoMigrate(&pgrecord.CommitLog{}, &legacyRecord{}, &legacyRecordKey{}))

	owner := "con1legacy"
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	commit := func(id string, doc concrnt.Document[map[string]string], gc bool) {
		t.Helper()
		docBytes, err := json.Marshal(doc)
		require.NoError(t, err)
		require.NoError(t, db.Create(&pgrecord.CommitLog{ID: id, Document: string(docBytes), Proof: `{"type":"none"}`, Owner: owner, GcCandidate: gc}).Error)
	}
	record := func(id, key string, createdAt time.Time) legacyRecord {
		commit(id, concrnt.Document[map[string]string]{Kind: "record", Key: key, Author: owner, Schema: "https://schema.example/post.json", CreatedAt: createdAt}, false)
		r := legacyRecord{DocumentID: id, Owner: owner, Author: owner, Schema: "https://schema.example/post.json", Distributions: pq.StringArray{"cckv://con1dest/tl"}, Policies: ptr(`{"entries":[]}`), CreatedAt: createdAt}
		require.NoError(t, db.Create(&r).Error)
		return r
	}

	timeline := "cckv://" + owner + "/tl"
	post := timeline + "/1"
	profile := "cckv://" + owner + "/profile"

	placeholder := legacyRecordKey{URI: timeline}
	require.NoError(t, db.Create(&placeholder).Error)
	record("post-1", post, at)
	require.NoError(t, db.Create(&legacyRecordKey{URI: post, ParentID: &placeholder.ID, RecordID: ptr("post-1"), RecordCreatedAt: &at, CleanOnUpdate: true}).Error)

	record("profile-retained", profile, at)                   // retained, the key moved on
	record("profile-current", profile, at.Add(2*time.Second)) // current
	require.NoError(t, db.Create(&legacyRecordKey{URI: profile, RecordID: ptr("profile-current"), RecordCreatedAt: ptr(at.Add(2 * time.Second))}).Error)
	commit("profile-deleted", concrnt.Document[map[string]string]{Kind: "record", Key: profile, Author: owner, CreatedAt: at.Add(time.Second)}, false) // retained, then deleted
	commit("delete-1", concrnt.Document[map[string]string]{Kind: "delete", Author: owner, CreatedAt: at.Add(3 * time.Second)}, false)
	commit("forgotten", concrnt.Document[map[string]string]{Kind: "record", Key: profile, Author: owner, CreatedAt: at}, true)
	commit("association-1", concrnt.Document[map[string]string]{Kind: "association", Author: owner, CreatedAt: at}, false)

	// the startup order: the repositories' AutoMigrate, then the versioned
	// migration
	repo := pgrecord.New(db)
	require.NoError(t, repo.Migrate())
	require.NoError(t, foldRecordsIntoRecordKeys(WithDB(ctx, db)))
	require.NoError(t, repo.Migrate())
	require.NoError(t, foldRecordsIntoRecordKeys(WithDB(ctx, db)), "re-running the migration is a no-op")

	require.True(t, db.Migrator().HasTable("records"), "records is left for the operator to drop")
	require.False(t, db.Migrator().HasIndex(&pgrecord.RecordKey{}, "idx_record_keys_parent_id_record_id"))
	var legacyForeignKeys int64
	require.NoError(t, db.Raw(`SELECT count(*) FROM pg_constraint WHERE conrelid = 'record_keys'::regclass AND confrelid = 'records'::regclass`).Scan(&legacyForeignKeys).Error)
	require.Zero(t, legacyForeignKeys)

	var rk pgrecord.RecordKey
	require.NoError(t, db.Where("uri = ?", post).Take(&rk).Error)
	require.Equal(t, owner, rk.Owner)
	require.Equal(t, owner, rk.Author)
	require.Equal(t, "https://schema.example/post.json", rk.Schema)
	require.Equal(t, []string{"cckv://con1dest/tl"}, []string(rk.Distributions))
	require.Equal(t, `{"entries":[]}`, *rk.Policies)
	require.True(t, rk.RecordCreatedAt.Equal(at))

	var ph pgrecord.RecordKey
	require.NoError(t, db.Where("uri = ?", timeline).Take(&ph).Error)
	require.Nil(t, ph.RecordID)
	require.Empty(t, ph.Schema)

	rows, err := repo.QueryByParent(ctx, timeline, "", "", nil, nil, 0, "asc")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, post, *rows[0].Row.CCKV)

	ccfs := func(id string) string { return concrnt.ComposeCCFSURI(owner, concrnt.CCFSTypeConcrnt, id) }
	_, err = repo.GetSignedDocument(ctx, ccfs("profile-retained"))
	require.NoError(t, err, "a retained version the key moved on from stays served")
	_, err = repo.GetSignedDocument(ctx, ccfs("profile-deleted"))
	require.ErrorIs(t, err, domain.ErrNotFound, "a deleted retained version is no longer served")

	gc := func(id string) bool {
		var cl pgrecord.CommitLog
		require.NoError(t, db.Where("id = ?", id).Take(&cl).Error)
		return cl.GcCandidate
	}
	require.False(t, gc("post-1"))
	require.False(t, gc("profile-current"))
	require.False(t, gc("profile-retained"))
	require.True(t, gc("profile-deleted"))
	require.True(t, gc("delete-1"))
	require.True(t, gc("forgotten"))
	require.False(t, gc("association-1"))

	// a key row written now points at a commit without a records row; with
	// the foreign key to records gone that is accepted, and a rerun of the
	// migration does not take the new retained version for a deleted one
	newAt := at.Add(time.Hour)
	commit("profile-new", concrnt.Document[map[string]string]{Kind: "record", Key: profile, Author: owner, CreatedAt: newAt}, false)
	commit("profile-newer", concrnt.Document[map[string]string]{Kind: "record", Key: profile, Author: owner, CreatedAt: newAt.Add(time.Second)}, false)
	for i, id := range []string{"profile-new", "profile-newer"} {
		createdAt := newAt.Add(time.Duration(i) * time.Second)
		tx, err := repo.BeginTx(ctx)
		require.NoError(t, err)
		applied, err := repo.CreateRecord(ctx, tx, id, profile, owner, owner, "https://schema.example/post.json", ptr("retain"), nil, nil, nil, createdAt)
		require.NoError(t, err)
		require.True(t, applied)
		require.NoError(t, tx.Commit(ctx))
	}
	require.NoError(t, foldRecordsIntoRecordKeys(WithDB(ctx, db)))
	require.False(t, gc("profile-new"), "the retained version written after the migration stays served")
}

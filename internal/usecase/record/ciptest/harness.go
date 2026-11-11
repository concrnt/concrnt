package ciptest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/cdid"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/aggregate/storetest"
	"github.com/concrnt/concrnt/internal/service"
	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/internal/usecase/aggregate"
	"github.com/concrnt/concrnt/internal/usecase/chunkline"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/usecase/server"
	"github.com/concrnt/concrnt/schemas"
)

const (
	// localDomain is the server under test; every user the suite registers
	// lives here, so no commit ever leaves the process.
	localDomain = "ciptest.example"
	layer       = "ciptest"
	clientIP    = "192.0.2.1"

	postSchema   = "https://ciptest.example/post.json"
	noteSchema   = "https://ciptest.example/note.json"
	likeSchema   = "https://ciptest.example/like.json"
	replySchema  = "https://ciptest.example/reply.json"
	followSchema = "https://ciptest.example/follow.json"
	deleteSchema = "https://schema.concrnt.net/delete.json"
)

// world is one server: the real record and chunkline usecases over the
// backend under test, with everything around them (queue, pubsub, kvs,
// policy, peer servers, count cache) held in memory.
type world struct {
	t   *testing.T
	ctx context.Context

	backend   Backend
	records   *record.Usecase
	timelines *chunkline.Usecase

	// jobs is nil in an offline world (the way conctl runs the usecase).
	jobs   *jobQueue
	signal *signalRecorder
	kvs    *memoryKVS
	policy *policySwitch

	// origin is where the test's document timestamps start: recent enough
	// for the backdate window, far enough back that everything a test
	// stamps lies in the past.
	origin time.Time
}

func newWorld(t *testing.T, backend Backend, offline bool) *world {
	t.Helper()
	require.NotNil(t, backend.Record, "Backend.Record is required")
	require.NotNil(t, backend.EntityMeta, "Backend.EntityMeta is required")

	w := &world{
		t:       t,
		ctx:     context.Background(),
		backend: backend,
		signal:  &signalRecorder{},
		kvs:     newMemoryKVS(),
		policy:  newPolicySwitch(),
		origin:  time.Now().Add(-48 * time.Hour).Truncate(time.Hour),
	}

	cfg := &domain.Config{FQDN: localDomain, Layer: layer}
	cl := client.New(localDomain)
	servers := server.New(peerServers{layer: layer}, cfg, concrnt.SoftwareInfo{}, service.NewModuleManager(map[string]string{}, nil), cl)

	// a nil *jobQueue in the interface would not be the nil the usecase
	// checks for
	var jobs usecase.JobQueue
	if !offline {
		w.jobs = newJobQueue()
		jobs = w.jobs
	}

	w.records = record.New(backend.Record, backend.EntityMeta, servers, cfg, cl, w.signal, w.policy, jobs, w.kvs)
	// the count cache sits in front of every count read, so the suite runs
	// the usecase with a real one: a backend whose Apply* outcomes are wrong
	// shows up as a cached count that disagrees with the primary data
	w.records.SetAggregates(aggregate.New(storetest.NewInMemory(), 0, 0))
	w.timelines = chunkline.New(backend.Record, nil, w.kvs, w.records)

	// as in the server's own wiring: the client answers documents of local
	// owners from the usecase, which is how a ccid is routed to this server
	cl.SetLocalResolver(func(ctx context.Context, uri string) (concrnt.SignedDocument, bool, error) {
		parsed, err := concrnt.ParseCCURI(uri)
		if err != nil {
			return concrnt.SignedDocument{}, false, nil
		}
		if local, err := w.records.IsLocalEntityByCCID(ctx, parsed.Owner); err != nil || !local {
			return concrnt.SignedDocument{}, false, nil
		}
		sd, err := w.records.GetSigned(ctx, uri)
		if errors.Is(err, domain.ErrNotFound) {
			return concrnt.SignedDocument{}, false, client.ErrNotFound
		}
		if err != nil {
			return concrnt.SignedDocument{}, false, nil
		}
		return *sd, true, nil
	})

	return w
}

// at is the test's n-th second: documents are ordered by these, never by
// the wall clock.
func (w *world) at(n int) time.Time {
	return w.origin.Add(time.Duration(n) * time.Second)
}

// user is a keypair registered on this server.
type user struct {
	ccid string
	priv string
}

// root is the user's entity URI and the root of its keys.
func (u user) root() string {
	return "cckv://" + u.ccid
}

func (u user) key(path string) string {
	return u.root() + "/" + path
}

// ccfs is the content address of the document id in u's namespace.
func (u user) ccfs(id string) string {
	return concrnt.ComposeCCFSURI(u.ccid, concrnt.CCFSTypeConcrnt, id)
}

// newUser registers a fresh identity the way the residence usecase does:
// the registration state first, then the entity document through Commit.
func (w *world) newUser() user {
	w.t.Helper()
	priv := make([]byte, 32)
	_, err := rand.Read(priv)
	require.NoError(w.t, err)
	u := user{priv: hex.EncodeToString(priv)}
	u.ccid, err = concrnt.PrivKeyToAddr(u.priv, "con")
	require.NoError(w.t, err)

	require.NoError(w.t, w.backend.EntityMeta.SaveMeta(w.ctx, domain.EntityMeta{ID: u.ccid, Info: "{}"}))
	w.commit(w.sign(u, entityDoc(u, localDomain, w.at(0))), domain.CommitModeLocalOnlyExecute)
	return u
}

type document = concrnt.Document[any]

func entityDoc(u user, home string, createdAt time.Time) document {
	return document{
		Kind:      "entity",
		Author:    u.ccid,
		Schema:    schemas.EntityURL,
		Value:     schemas.Entity{Domain: home},
		CreatedAt: createdAt,
	}
}

// recordDoc is a record at key authored by author; mods adjust the rest.
func recordDoc(author user, key string, createdAt time.Time, mods ...func(*document)) document {
	doc := document{
		Kind:      "record",
		Key:       key,
		Author:    author.ccid,
		Schema:    postSchema,
		Value:     map[string]string{"body": key},
		CreatedAt: createdAt,
	}
	for _, mod := range mods {
		mod(&doc)
	}
	return doc
}

func withSchema(schema string) func(*document) {
	return func(doc *document) { doc.Schema = schema }
}

func withValue(value any) func(*document) {
	return func(doc *document) { doc.Value = value }
}

func retained() func(*document) {
	return func(doc *document) {
		retain := "retain"
		doc.OnUpdate = &retain
	}
}

func distributedTo(destinations ...string) func(*document) {
	return func(doc *document) { doc.Distributes = &destinations }
}

// associationDoc is an association on target; variant "" means none.
func associationDoc(author user, target, schema, variant string, value any, createdAt time.Time) document {
	doc := document{
		Kind:      "association",
		Author:    author.ccid,
		Schema:    schema,
		Value:     value,
		Associate: &target,
		CreatedAt: createdAt,
	}
	if variant != "" {
		doc.AssociationVariant = &variant
	}
	return doc
}

// ackDoc is an ack or unack (kind) of to by from.
func ackDoc(kind string, from, to user, createdAt time.Time) document {
	associate := to.root()
	return document{
		Kind:      kind,
		Author:    from.ccid,
		Schema:    followSchema,
		Associate: &associate,
		CreatedAt: createdAt,
	}
}

func deleteDoc(author user, target string, createdAt time.Time) document {
	return document{
		Kind:      "delete",
		Author:    author.ccid,
		Schema:    deleteSchema,
		Value:     target,
		CreatedAt: createdAt,
	}
}

// sign signs doc with u's master key.
func (w *world) sign(u user, doc document) concrnt.SignedDocument {
	w.t.Helper()
	raw, err := json.Marshal(doc)
	require.NoError(w.t, err)
	sig, err := concrnt.SignBytes(raw, u.priv)
	require.NoError(w.t, err)
	signature := hex.EncodeToString(sig)
	return concrnt.SignedDocument{
		Document: string(raw),
		Proof:    concrnt.Proof{Type: concrnt.ProofTypeEcrecover, Signature: &signature},
	}
}

// commit sends sd through the usecase and requires it to be accepted.
func (w *world) commit(sd concrnt.SignedDocument, mode domain.CommitMode) {
	w.t.Helper()
	_, err := w.records.Commit(w.ctx, clientIP, sd, mode)
	require.NoError(w.t, err)
}

// execute commits doc signed by its author the way a client request does.
func (w *world) execute(author user, doc document) concrnt.SignedDocument {
	w.t.Helper()
	sd := w.sign(author, doc)
	w.commit(sd, domain.CommitModeExecute)
	return sd
}

// drain runs the background jobs to the end and requires all of them to
// succeed.
func (w *world) drain() {
	w.t.Helper()
	require.NoError(w.t, w.jobs.drain(w.ctx))
	require.Empty(w.t, w.jobs.pendingTypes())
}

func idOf(t *testing.T, sd concrnt.SignedDocument) string {
	t.Helper()
	id, err := sd.CDID()
	require.NoError(t, err)
	return id
}

func idsOf(t *testing.T, sds ...concrnt.SignedDocument) []string {
	t.Helper()
	ids := make([]string, 0, len(sds))
	for _, sd := range sds {
		ids = append(ids, idOf(t, sd))
	}
	return ids
}

// stored requires uri to resolve and returns the document it holds.
func (w *world) stored(uri string) string {
	w.t.Helper()
	sd, err := w.records.GetSigned(w.ctx, uri)
	require.NoError(w.t, err, "GetSigned(%s)", uri)
	return sd.Document
}

func (w *world) requireMissing(uri string) {
	w.t.Helper()
	_, err := w.records.GetSigned(w.ctx, uri)
	require.ErrorIs(w.t, err, domain.ErrNotFound, "GetSigned(%s)", uri)
}

// systemContext is the system service account, which Replicate serves
// without read-access filtering.
func (w *world) systemContext() context.Context {
	return context.WithValue(w.ctx, interop.ServiceAccountTypeCtxKey, "system")
}

// feed is the replication feed of owner ("" = every owner) in receipt
// order, as document ids.
func (w *world) feed(owner string) []string {
	w.t.Helper()
	result, err := w.records.Replicate(w.systemContext(), owner, nil, nil, 10000, "asc")
	require.NoError(w.t, err)
	require.Nil(w.t, result.Next, "the feed does not fit the suite's page")
	return idsOf(w.t, result.Items...)
}

// dump is the commit log export of owner, as document ids.
func (w *world) dump(owner user) []string {
	w.t.Helper()
	ctx := context.WithValue(w.ctx, interop.RequesterCtxKey, domain.Entity{ID: owner.ccid, Domain: localDomain})
	lines, err := w.records.DumpCommitLogs(ctx)
	require.NoError(w.t, err)
	ids := []string{}
	for _, line := range strings.Split(strings.TrimSpace(lines), "\n") {
		if line == "" {
			continue
		}
		var sd concrnt.SignedDocument
		require.NoError(w.t, json.Unmarshal([]byte(line), &sd))
		ids = append(ids, idOf(w.t, sd))
	}
	return ids
}

// gcCandidates are the ids of the commits flagged for garbage collection.
// No usecase reads the flag, so this goes to the repository's offline
// listing, with a cutoff past everything the suite writes.
func (w *world) gcCandidates() []string {
	w.t.Helper()
	cutoff := cdid.New([10]byte{}, time.Now().Add(24*time.Hour)).String()
	entries, err := w.backend.Record.ListGcCandidates(w.ctx, cutoff, "", 10000)
	require.NoError(w.t, err)
	count, err := w.backend.Record.CountGcCandidates(w.ctx, cutoff)
	require.NoError(w.t, err)
	require.EqualValues(w.t, len(entries), count, "CountGcCandidates disagrees with ListGcCandidates")
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.ID)
	}
	return ids
}

// documentsOf are the raw documents of a listing, in order.
func documentsOf(items []concrnt.SignedDocument) []string {
	documents := make([]string, 0, len(items))
	for _, item := range items {
		documents = append(documents, item.Document)
	}
	return documents
}

func documents(sds ...concrnt.SignedDocument) []string {
	return documentsOf(sds)
}

// keysOf are the cckv keys of a listing, in order.
func keysOf(t *testing.T, items []concrnt.SignedDocument) []string {
	t.Helper()
	keys := make([]string, 0, len(items))
	for _, item := range items {
		require.NotNil(t, item.CCKV, "listed row without a cckv")
		keys = append(keys, *item.CCKV)
	}
	return keys
}

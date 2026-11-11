package record

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/testutil"
	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// rpcCounter counts the Firestore RPCs a client issues, by method name.
type rpcCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *rpcCounter) record(fullMethod string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts != nil {
		c.counts[fullMethod[strings.LastIndex(fullMethod, "/")+1:]]++
	}
}

func (c *rpcCounter) dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			c.record(method)
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			c.record(method)
			return streamer(ctx, desc, cc, method, opts...)
		}),
	}
}

// during returns the RPCs fn issued.
func (c *rpcCounter) during(fn func()) map[string]int {
	c.mu.Lock()
	c.counts = map[string]int{}
	c.mu.Unlock()
	fn()
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := c.counts
	c.counts = nil
	return counts
}

// The RPC names: a get (single or batch) is BatchGetDocuments, a query
// RunQuery.
const (
	rpcGet   = "BatchGetDocuments"
	rpcQuery = "RunQuery"
)

func budgetCommit(id, owner, document string) ucrecord.Commit {
	return ucrecord.Commit{ID: id, IP: "127.0.0.1", Document: document, Proof: concrnt.Proof{Type: concrnt.ProofTypeNone}, Owner: owner}
}

func budgetRecord(id, key string, createdAt time.Time) ucrecord.RecordCommit {
	document, _ := json.Marshal(map[string]string{"id": id, "key": key})
	return ucrecord.RecordCommit{
		Commit: budgetCommit(id, "con1owner", string(document)),
		Key:    key, Author: "con1owner", Schema: "https://schema.example/post.json", CreatedAt: createdAt,
	}
}

func budgetAssociation(id, target, author string, createdAt time.Time) ucrecord.AssociationCommit {
	return ucrecord.AssociationCommit{
		Commit: budgetCommit(id, "con1owner", `{"id":"`+id+`"}`),
		Target: target, Author: author, Schema: "https://schema.example/like.json", Unique: target + "#" + author, CreatedAt: createdAt,
	}
}

// TestRoundTripBudgets pins what every contract operation costs in RPCs
// (the "Round trips" of record.Repository). Each Apply* is one transaction
// with exactly one read, a batch get, and no query. A read is one get or
// one query; where it is one of each, they run in parallel (the key that
// tells the live incarnation, or that a timeline exists).
func TestRoundTripBudgets(t *testing.T) {
	ctx := context.Background()
	counter := &rpcCounter{}
	repo := New(testutil.CreateFirestore(t, counter.dialOptions()...))

	at := time.Unix(1000*600+10, 0).UTC()
	timeline := "cckv://con1owner/apps/timeline"
	post := timeline + "/post-1"
	ccfs := concrnt.ComposeCCFSURI("con1owner", concrnt.CCFSTypeConcrnt, "post-1")

	applied := func(t *testing.T, name string, apply func() (ucrecord.ApplyStatus, error)) {
		t.Helper()
		var status ucrecord.ApplyStatus
		var err error
		counts := counter.during(func() { status, err = apply() })
		require.NoError(t, err, name)
		require.Equal(t, ucrecord.Applied, status, name)
		require.Equal(t, 1, counts[rpcGet], "%s reads once: %v", name, counts)
		require.Equal(t, 1, counts["Commit"], "%s is one transaction: %v", name, counts)
		delete(counts, rpcGet)
		delete(counts, "Commit")
		// the SDK may begin the transaction with a call of its own
		delete(counts, "BeginTransaction")
		require.Empty(t, counts, "%s issues nothing else, no query in particular", name)
	}

	t.Run("writes", func(t *testing.T) {
		applied(t, "ApplyRecord", func() (ucrecord.ApplyStatus, error) {
			outcome, err := repo.ApplyRecord(ctx, budgetRecord("post-1", post, at))
			return outcome.Status, err
		})
		retain := "retain"
		kept := budgetRecord("kept-1", timeline+"/kept", at)
		kept.OnUpdate = &retain
		applied(t, "ApplyRecord", func() (ucrecord.ApplyStatus, error) {
			outcome, err := repo.ApplyRecord(ctx, kept)
			return outcome.Status, err
		})
		applied(t, "ApplyRecord over a retained version", func() (ucrecord.ApplyStatus, error) {
			outcome, err := repo.ApplyRecord(ctx, budgetRecord("kept-2", timeline+"/kept", at.Add(time.Second)))
			return outcome.Status, err
		})
		for i, author := range []string{"con1alice", "con1bob", "con1carol"} {
			applied(t, "ApplyAssociation", func() (ucrecord.ApplyStatus, error) {
				return repo.ApplyAssociation(ctx, budgetAssociation(fmt.Sprintf("like-%d", i), post, author, at.Add(time.Duration(i)*time.Second)))
			})
		}
		applied(t, "ApplyAck", func() (ucrecord.ApplyStatus, error) {
			outcome, err := repo.ApplyAck(ctx, ucrecord.AckCommit{
				Commit: budgetCommit("ack-1", "con1alice", `{"id":"ack-1"}`),
				Side:   ucrecord.AckerSide, From: "con1alice", To: "con1owner", Schema: "https://schema.example/follow.json", Valid: true, CreatedAt: at,
			})
			return outcome.Status, err
		})
		applied(t, "ApplyEntity", func() (ucrecord.ApplyStatus, error) {
			alias := "owner.example.com"
			return repo.ApplyEntity(ctx, ucrecord.EntityCommit{
				Commit: budgetCommit("entity-1", "con1owner", `{"id":"entity-1"}`),
				CCID:   "con1owner", Alias: &alias, Domain: "example.com", CreatedAt: at,
			})
		})
		for i := range 3 {
			_, err := repo.ApplyRecord(ctx, budgetRecord(fmt.Sprintf("doomed-%d", i), fmt.Sprintf("cckv://con1owner/doomed/%d", i), at))
			require.NoError(t, err)
		}
		_, err := repo.ApplyAssociation(ctx, budgetAssociation("doomed-like", "cckv://con1owner/doomed/2", "con1alice", at))
		require.NoError(t, err)
		commit := budgetCommit("delete-1", "con1owner", `{"id":"delete-1"}`)
		var outcome ucrecord.DeleteOutcome
		applied(t, "ApplyDelete", func() (ucrecord.ApplyStatus, error) {
			var err error
			outcome, err = repo.ApplyDelete(ctx, ucrecord.DeleteCommit{
				Commit: &commit,
				Removals: []ucrecord.Removal{
					{Kind: ucrecord.RemoveKey, URI: "cckv://con1owner/doomed/0"},
					{Kind: ucrecord.RemoveVersion, URI: "cckv://con1owner/doomed/1", DocumentID: "doomed-1"},
					{Kind: ucrecord.RemoveVersion, URI: timeline + "/kept", DocumentID: "kept-1"},
					{Kind: ucrecord.RemoveAssociation, URI: "cckv://con1owner/doomed/2", DocumentID: "doomed-like"},
				},
				GcCommitIDs: []string{"doomed-0", "doomed-1", "delete-1", "unknown"},
			})
			return outcome.Status, err
		})
		require.Equal(t, []bool{true, true, true, true}, outcome.Removed)
		require.Len(t, outcome.Cleanups, 1, "the two ended incarnations share a token")
	})

	reads := []struct {
		name string
		want map[string]int
		read func() error
	}{
		{"GetSignedDocument by cckv", map[string]int{rpcGet: 1}, func() error {
			_, err := repo.GetSignedDocument(ctx, post)
			return err
		}},
		{"GetSignedDocument by ccfs", map[string]int{rpcGet: 2}, func() error {
			sd, err := repo.GetSignedDocument(ctx, ccfs)
			if err == nil {
				require.NotNil(t, sd.CCKV)
			}
			return err
		}},
		{"GetSignedDocument by ccfs of a commit without a key", map[string]int{rpcGet: 1}, func() error {
			_, err := repo.GetSignedDocument(ctx, concrnt.ComposeCCFSURI("con1owner", concrnt.CCFSTypeConcrnt, "like-0"))
			return err
		}},
		{"GetSignedDocuments, cckv and ccfs mixed", map[string]int{rpcGet: 2}, func() error {
			sds, err := repo.GetSignedDocuments(ctx, []string{post, timeline + "/kept", ccfs, concrnt.ComposeCCFSURI("con1owner", concrnt.CCFSTypeConcrnt, "kept-2")})
			require.Len(t, sds, 4)
			return err
		}},
		{"QueryByParent", map[string]int{rpcQuery: 1}, func() error {
			rows, err := repo.QueryByParent(ctx, timeline, "", "", nil, nil, 10, "desc")
			require.Len(t, rows, 2)
			require.NotEmpty(t, rows[0].Row.Document)
			return err
		}},
		{"QueryByPrefix", map[string]int{rpcQuery: 1}, func() error {
			rows, err := repo.QueryByPrefix(ctx, "cckv://con1owner/apps/", "", "", nil, nil, 10, "desc")
			require.Len(t, rows, 2)
			return err
		}},
		{"QueryByParentOrderByKey", map[string]int{rpcQuery: 1}, func() error {
			rows, err := repo.QueryByParentOrderByKey(ctx, timeline, "", "", nil, nil, 10, "asc")
			require.Len(t, rows, 2)
			return err
		}},
		{"QueryRecordSubtree", map[string]int{rpcQuery: 1}, func() error {
			sds, err := repo.QueryRecordSubtree(ctx, timeline, false, "", 10)
			require.Len(t, sds, 2)
			return err
		}},
		{"QueryRecordSubtree including the base", map[string]int{rpcQuery: 1, rpcGet: 1}, func() error {
			_, err := repo.QueryRecordSubtree(ctx, timeline, true, "", 10)
			return err
		}},
		{"GetAssociatedRecords", map[string]int{rpcQuery: 1, rpcGet: 1}, func() error {
			rows, err := repo.GetAssociatedRecords(ctx, post, "", "", "", nil, nil, 2, "desc")
			require.Len(t, rows, 2)
			require.NotEmpty(t, rows[0].Row.Document)
			return err
		}},
		{"ComputeAssociationSummary", map[string]int{rpcQuery: 1, rpcGet: 1}, func() error {
			summary, err := repo.ComputeAssociationSummary(ctx, post, 10)
			require.Len(t, summary.Authors, 3)
			return err
		}},
		{"GetAcknowledgeRecords", map[string]int{rpcQuery: 1}, func() error {
			rows, err := repo.GetAcknowledgeRecords(ctx, "con1alice", "", "", nil, nil, 10, "desc")
			require.Len(t, rows, 1)
			require.NotEmpty(t, rows[0].Row.Document)
			return err
		}},
		{"ComputeAcknowledgeCounts", map[string]int{rpcQuery: 1}, func() error {
			_, err := repo.ComputeAcknowledgeCounts(ctx, "con1alice", "", "")
			return err
		}},
		{"GetHierarchicalRecordPolicies by cckv", map[string]int{rpcGet: 1}, func() error {
			_, err := repo.GetHierarchicalRecordPolicies(ctx, post)
			return err
		}},
		{"GetHierarchicalRecordPolicies by ccfs", map[string]int{rpcGet: 2}, func() error {
			_, err := repo.GetHierarchicalRecordPolicies(ctx, ccfs)
			return err
		}},
		{"GetHierarchicalRecordPoliciesMulti", map[string]int{rpcGet: 1}, func() error {
			stacks, err := repo.GetHierarchicalRecordPoliciesMulti(ctx, []string{post, timeline + "/kept", "cckv://con1owner/elsewhere/x"})
			require.Len(t, stacks, 3)
			return err
		}},
		{"GetTimelineRemoval", map[string]int{rpcGet: 1}, func() error {
			_, _, err := repo.GetTimelineRemoval(ctx, post)
			return err
		}},
		{"GetChunklineManifest", map[string]int{rpcQuery: 1, rpcGet: 1}, func() error {
			manifest, err := repo.GetChunklineManifest(ctx, timeline)
			if err == nil {
				require.NotNil(t, manifest.FirstChunk)
			}
			return err
		}},
		{"LookupLocalItrs, two timelines", map[string]int{rpcQuery: 2, rpcGet: 1}, func() error {
			itrs, err := repo.LookupLocalItrs(ctx, []string{timeline, "cckv://con1owner/apps"}, 1000)
			require.Len(t, itrs, 1)
			return err
		}},
		{"LoadLocalBody", map[string]int{rpcQuery: 1, rpcGet: 1}, func() error {
			items, err := repo.LoadLocalBody(ctx, timeline, 1000)
			require.Len(t, items, 2)
			return err
		}},
		{"GetEntityByCCID", map[string]int{rpcGet: 1}, func() error {
			entity, err := repo.GetEntityByCCID(ctx, "con1owner")
			if err == nil {
				require.NotEmpty(t, entity.SignedDocument.Document)
			}
			return err
		}},
		{"GetEntityByAlias", map[string]int{rpcQuery: 1}, func() error {
			_, err := repo.GetEntityByAlias(ctx, "owner.example.com")
			return err
		}},
	}
	for _, tc := range reads {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			counts := counter.during(func() { err = tc.read() })
			require.NoError(t, err)
			require.Equal(t, tc.want, counts)
		})
	}
}

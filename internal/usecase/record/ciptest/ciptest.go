// Package ciptest is the protocol-level conformance suite of a record
// store (CONTRACT.md P5): it drives the real record and chunkline usecases
// over the backend under test and asserts what a client or a peer server
// can observe (commits, reads, listings, counts, events, the replication
// feed). Passing it is what "implements concrnt" means for a backend; how
// the backend stores anything is never asserted. The per-method contract
// of record.Repository is repotest's business.
//
// The name ends in "test" (C10): only test code may import this package.
package ciptest

import (
	"testing"

	"github.com/concrnt/concrnt/internal/usecase"
	"github.com/concrnt/concrnt/internal/usecase/record"
)

// Backend is one backend under test.
type Backend struct {
	Record     record.Repository
	EntityMeta usecase.EntityMetaRepository
}

type conformanceTest struct {
	name string
	// offline runs the usecase without a job queue, the way conctl does.
	offline bool
	run     func(w *world)
}

var conformanceTests = []conformanceTest{
	{name: "record/accept-if-newer", run: testRecordAcceptIfNewer},
	{name: "record/redelivery", run: testRecordRedelivery},
	{name: "record/onUpdate-forget", run: testOnUpdateForget},
	{name: "record/onUpdate-retain", run: testOnUpdateRetain},
	{name: "record/import-does-not-deliver", run: testImportDoesNotDeliver},
	{name: "record/policy-denial-stores-nothing", run: testPolicyDenialStoresNothing},
	{name: "entity/accept-if-newer", run: testEntityAcceptIfNewer},
	{name: "association/listing", run: testAssociationListing},
	{name: "association/uniqueness", run: testAssociationUniqueness},
	{name: "association/missing-target", run: testAssociationMissingTarget},
	{name: "association/counts", run: testAssociationCounts},
	{name: "association/listing-survives-sibling-delete", run: testAssociationListingSurvivesSiblingDelete},
	{name: "association/delete-then-recreate", run: testDeleteThenRecreate},
	{name: "ack/state-machine", run: testAckStateMachine},
	{name: "ack/import", run: testAckImport},
	{name: "delete/single", run: testDeleteSingle},
	{name: "delete/single-retained", run: testDeleteSingleRetained},
	{name: "delete/by-ccfs", run: testDeleteByCCFS},
	{name: "delete/retained-version-by-ccfs", run: testDeleteRetainedVersion},
	{name: "delete/missing-target", run: testDeleteMissingTarget},
	{name: "delete/association", run: testDeleteAssociation},
	{name: "delete/range-with-self", run: testRangeDeleteWithSelf},
	{name: "delete/range-children-only", run: testRangeDeleteChildrenOnly},
	{name: "delete/range-paged", run: testRangeDeletePaged},
	{name: "delete/range-offline", offline: true, run: testRangeDeleteOffline},
	{name: "delete/cleanup-changes-nothing", run: testCleanupChangesNothing},
	{name: "delete/reference-sweep", run: testReferenceSweep},
	{name: "delete/reference-sweep-association", run: testReferenceSweepAssociation},
	{name: "query/by-parent", run: testQueryByParent},
	{name: "query/by-prefix", run: testQueryByPrefix},
	{name: "query/by-key", run: testQueryByKey},
	{name: "query/owner-root", run: testQueryOwnerRoot},
	{name: "chunkline/manifest", run: testChunklineManifest},
	{name: "chunkline/itr", run: testChunklineItr},
	{name: "chunkline/body", run: testChunklineBody},
	{name: "replication/feed", run: testReplicationFeed},
}

// Run runs every CIP conformance test against the backend open returns; open
// must return a fresh, isolated backend per call and is called once per test.
func Run(t *testing.T, open func(t *testing.T) Backend) {
	for _, test := range conformanceTests {
		t.Run(test.name, func(t *testing.T) {
			test.run(newWorld(t, open(t), test.offline))
		})
	}
}

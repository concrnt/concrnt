//go:build !contractv1

// Package record will be the Firestore implementation of record.Repository.
// The implementation in this directory still speaks the v1 contract and is
// kept out of the build by the contractv1 tag until it is ported; porting it
// means removing that tag from every file here and deleting this one.
package record

import (
	"cloud.google.com/go/firestore"

	ucrecord "github.com/concrnt/concrnt/internal/usecase/record"
)

// New keeps the server and conctl building; selecting the backend fails at
// start instead of at the first request.
func New(client *firestore.Client) ucrecord.Repository {
	panic("backends.database firestore: the record repository is not ported to repository contract v2 yet")
}

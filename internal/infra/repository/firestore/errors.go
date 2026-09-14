package firestore

import (
	"errors"
	"log/slog"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// isNotFound reports whether err is Firestore's "document does not exist".
func isNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

// isAlreadyExists reports whether err is a Create on an existing document.
func isAlreadyExists(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}

// queryErr logs the composite-index hint Firestore embeds in a
// FailedPrecondition error (it names the index and carries a console URL to
// create it) and returns err unchanged, gRPC status intact so a transaction
// can still see Aborted.
func queryErr(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.FailedPrecondition && strings.Contains(err.Error(), "index") {
		slog.Error("firestore query needs a composite index; deploy deploy/firestore/firestore.indexes.json",
			slog.String("detail", err.Error()))
	}
	return err
}

// errInvalidTx is returned when a transactional method receives a handle that
// this backend did not create.
var errInvalidTx = errors.New("invalid record repository transaction")

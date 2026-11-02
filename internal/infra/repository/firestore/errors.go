package firestore

import (
	"log/slog"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsNotFound reports whether err is Firestore's "document does not exist".
func IsNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

// QueryErr logs the composite-index hint Firestore embeds in a
// FailedPrecondition error (it names the index and carries a console URL to
// create it) and returns err unchanged, gRPC status intact so a transaction
// can still see Aborted.
func QueryErr(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.FailedPrecondition && strings.Contains(err.Error(), "index") {
		slog.Error("firestore query needs a composite index; deploy deploy/firestore/firestore.indexes.json",
			slog.String("detail", err.Error()))
	}
	return err
}

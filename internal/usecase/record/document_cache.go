package record

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/concrnt/concrnt"
)

// The resolved document cache. A record's row changes only by a commit to
// its key or a delete, both of which drop the entry after the commit; the
// TTL bounds a write that bypassed them (an offline tool) or an entry
// re-stored by a read racing the invalidation.
const documentCacheTTL = 3600

var documentCacheRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "concrnt",
	Name:      "document_cache_requests_total",
	Help:      "Signed document lookups by URI against the document cache, by result (hit, miss, error: the cache was unreachable).",
}, []string{"result"})

func documentCacheKey(uri string) string {
	sum := sha256.Sum256([]byte(uri))
	return "doc:" + hex.EncodeToString(sum[:])
}

// signedDocumentCached is repo.GetSignedDocument behind the shared cache; a
// missing document is not cached (it may be committed right after).
func (uc *Usecase) signedDocumentCached(ctx context.Context, uri string) (*concrnt.SignedDocument, error) {
	if uc.sharedCache == nil {
		return uc.repo.GetSignedDocument(ctx, uri)
	}

	key := documentCacheKey(uri)
	var cached concrnt.SignedDocument
	result := uc.sharedCacheGet(key, &cached)
	documentCacheRequests.WithLabelValues(result).Inc()
	if result == "hit" {
		return &cached, nil
	}

	sd, err := uc.repo.GetSignedDocument(ctx, uri)
	if err != nil {
		return nil, err
	}
	if result == "miss" {
		uc.sharedCacheSet(key, sd, documentCacheTTL)
	}
	return sd, nil
}

// documentCacheInvalidated drops the cached documents at uris (nil entries
// are skipped) once the write that changed them has committed.
func (uc *Usecase) documentCacheInvalidated(uris ...*string) PostProcessAction {
	return func(ctx context.Context) error {
		if uc.sharedCache == nil {
			return nil
		}
		var errs error
		for _, uri := range uris {
			if uri == nil || *uri == "" {
				continue
			}
			if err := uc.sharedCacheDelete(documentCacheKey(*uri)); err != nil {
				errs = errors.Join(errs, fmt.Errorf("invalidate cached document %s: %w", *uri, err))
			}
		}
		return errs
	}
}

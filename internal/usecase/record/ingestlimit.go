package record

import (
	"context"
	"errors"
	"log/slog"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/ratelimit"
)

// limitUnknownAuthor charges the shared unknown bucket when this server holds
// no entity for author yet.
func (uc *Usecase) limitUnknownAuthor(ctx context.Context, author string) error {
	if uc.config.IngestLimit.Unknown <= 0 {
		return nil
	}
	_, err := uc.repo.GetEntityByCCID(ctx, author)
	if err == nil {
		return nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	return uc.limitIngest(ctx, "ingest:unknown", uc.config.IngestLimit.Unknown, "too many commits from unknown users")
}

// limitDomain charges the bucket of a remote domain, sized by how much this
// server trusts it.
func (uc *Usecase) limitDomain(ctx context.Context, domainName string) error {
	trust := 0.0
	if uc.trust != nil && uc.config.IngestLimit.ScalesWithTrust() {
		trust = uc.trust.DomainTrust(ctx, domainName)
	}
	return uc.limitIngest(ctx, "ingest:domain:"+domainName, uc.config.IngestLimit.DomainLimit(trust), "too many commits from "+domainName)
}

// limitIngest counts one commit against the bucket and returns a
// domain.RateLimitError when it is over perMinute. A limiter failure lets the
// commit through: losing the limit for a while is better than refusing all
// federation because the counter store is down.
func (uc *Usecase) limitIngest(ctx context.Context, bucket string, perMinute int, reason string) error {
	result, err := uc.ingest.Allow(ctx, bucket, ratelimit.PerMinute(perMinute))
	if err != nil {
		slog.Error("ingest limiter failed, letting the commit through",
			slog.String("bucket", bucket),
			slog.String("error", err.Error()),
		)
		return nil
	}
	if !result.Allowed {
		return domain.RateLimitError{Reason: reason, RetryAfter: result.RetryAfter}
	}
	return nil
}

// Package trust scores how much this server trusts other servers, from whom
// its own users acknowledge.
//
// The server's admin names seed users, who are fully trusted. Trust flows
// from them along acknowledgements (see scores), first to other local users
// and from those on to the remote users they acknowledge. A remote server is
// trusted as much as its users are, added up.
package trust

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/concrnt/concrnt/internal/domain"
)

// refreshInterval is how stale the scores may get before they are recomputed.
const refreshInterval = 5 * time.Minute

// AckEdge is a local user's acknowledgement, with the domain its target
// lives on.
type AckEdge struct {
	From     string
	To       string
	ToDomain string
}

type Repository interface {
	// GetLocalAckEdges lists the valid acknowledgements made by users of
	// localDomain. schemas, when not empty, restricts them to those schemas.
	GetLocalAckEdges(ctx context.Context, localDomain string, schemas []string) ([]AckEdge, error)
}

type Usecase struct {
	repo   Repository
	config *domain.Config
	now    func() time.Time

	mu          sync.Mutex
	domains     map[string]float64
	refreshedAt time.Time
	refreshing  bool
}

func New(repo Repository, config *domain.Config) *Usecase {
	return &Usecase{repo: repo, config: config, now: time.Now}
}

// DomainTrust is how much this server trusts domain, from 0 (nobody trusted
// here acknowledges anyone there) to 1 (its users hold at least one seed's
// worth of trust between them).
//
// It never waits on the database: it answers from the last computed scores
// and, when those are stale, starts recomputing them in the background.
// Until the first computation finishes every domain scores 0.
func (uc *Usecase) DomainTrust(ctx context.Context, domainName string) float64 {
	if len(uc.config.IngestLimit.Trust.Seeds) == 0 {
		return 0
	}

	uc.mu.Lock()
	defer uc.mu.Unlock()

	if !uc.refreshing && uc.now().Sub(uc.refreshedAt) >= refreshInterval {
		uc.refreshing = true
		go func() {
			ctx := context.WithoutCancel(ctx)
			if err := uc.Refresh(ctx); err != nil {
				slog.Error("failed to refresh domain trust", slog.String("error", err.Error()))
			}
		}()
	}

	return uc.domains[domainName]
}

// Refresh recomputes the scores now. On failure the previous scores stay in
// use and the next DomainTrust call retries after refreshInterval.
func (uc *Usecase) Refresh(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Usecase.Trust.Refresh")
	defer span.End()

	domains, err := uc.compute(ctx)

	uc.mu.Lock()
	defer uc.mu.Unlock()
	uc.refreshing = false
	uc.refreshedAt = uc.now()
	if err != nil {
		span.RecordError(err)
		return err
	}
	uc.domains = domains
	slog.Info("domain trust refreshed", slog.Int("trustedDomains", len(domains)))
	return nil
}

func (uc *Usecase) compute(ctx context.Context) (map[string]float64, error) {
	conf := uc.config.IngestLimit.Trust

	ackEdges, err := uc.repo.GetLocalAckEdges(ctx, uc.config.FQDN, conf.AckSchemas)
	if err != nil {
		return nil, err
	}

	edges := make([]Edge, len(ackEdges))
	domainOf := make(map[string]string, len(ackEdges))
	for i, edge := range ackEdges {
		edges[i] = Edge{From: edge.From, To: edge.To}
		domainOf[edge.To] = edge.ToDomain
	}

	domains := map[string]float64{}
	for entity, score := range scores(conf.Seeds, edges) {
		if name := domainOf[entity]; name != "" && name != uc.config.FQDN {
			domains[name] += score
		}
	}
	for name, total := range domains {
		domains[name] = math.Min(1, total)
	}
	return domains, nil
}

// Package trust scores how much this server trusts other servers, from whom
// its own users acknowledge.
//
// The server's operator marks seed entities, which are fully trusted. Trust
// flows from them along acknowledgements (see scores), first to other local
// users and from those on to the remote users they acknowledge. Every
// entity's score is stored on the entity, and a remote server is trusted as
// much as its entities are, added up.
package trust

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/concrnt/concrnt/internal/domain"
)

// reloadInterval is how stale a replica's copy of the domain scores may get
// before it reads them again.
const reloadInterval = time.Minute

// AckEdge is a local user's acknowledgement, with the domain its target
// lives on.
type AckEdge struct {
	From     string
	To       string
	ToDomain string
}

type Repository interface {
	// GetTrustSeeds lists the entities marked as trust seeds.
	GetTrustSeeds(ctx context.Context) ([]string, error)
	// GetLocalAckEdges lists the valid acknowledgements made by users of
	// localDomain. schemas, when not empty, restricts them to those schemas.
	GetLocalAckEdges(ctx context.Context, localDomain string, schemas []string) ([]AckEdge, error)
	// SaveEntityTrust stores a computation's result: the entities in scores
	// get their score, every other entity 0.
	SaveEntityTrust(ctx context.Context, scores map[string]float64) error
	// GetDomainTrust adds up the stored trust of each domain's entities.
	GetDomainTrust(ctx context.Context) (map[string]float64, error)
}

type Usecase struct {
	repo   Repository
	config *domain.Config
	now    func() time.Time

	mu         sync.Mutex
	domains    map[string]float64
	loadedAt   time.Time
	refreshing bool
}

func New(repo Repository, config *domain.Config) *Usecase {
	return &Usecase{repo: repo, config: config, now: time.Now}
}

// DomainTrust is how much this server trusts domain, from 0 (nobody trusted
// here acknowledges anyone there) to 1 (its users hold at least one seed's
// worth of trust between them).
//
// It never waits on the database: it answers from the scores last read and,
// when those are stale, starts reading them again in the background. Until
// the first read finishes every domain scores 0.
func (uc *Usecase) DomainTrust(ctx context.Context, domainName string) float64 {
	uc.mu.Lock()
	defer uc.mu.Unlock()

	if !uc.refreshing && uc.now().Sub(uc.loadedAt) >= reloadInterval {
		uc.refreshing = true
		go func() {
			if err := uc.Refresh(context.WithoutCancel(ctx)); err != nil {
				slog.Error("failed to refresh domain trust", slog.String("error", err.Error()))
			}
		}()
	}

	return uc.domains[domainName]
}

// Refresh reads the domain scores, as stored by the latest recomputation. On
// failure the scores read before stay in use.
func (uc *Usecase) Refresh(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Usecase.Trust.Refresh")
	defer span.End()

	domains, err := uc.load(ctx)

	uc.mu.Lock()
	defer uc.mu.Unlock()
	uc.refreshing = false
	uc.loadedAt = uc.now()
	if err != nil {
		span.RecordError(err)
		return err
	}
	uc.domains = domains
	return nil
}

func (uc *Usecase) load(ctx context.Context) (map[string]float64, error) {
	totals, err := uc.repo.GetDomainTrust(ctx)
	if err != nil {
		return nil, err
	}
	domains := make(map[string]float64, len(totals))
	for name, total := range totals {
		if name != uc.config.FQDN {
			domains[name] = math.Min(1, total)
		}
	}
	return domains, nil
}

// Recompute scores every entity from the current seeds and acknowledgements
// and stores the result. The cluster's leader runs it on a schedule (see
// worker.TrustRecomputer).
func (uc *Usecase) Recompute(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Usecase.Trust.Recompute")
	defer span.End()

	seeds, err := uc.repo.GetTrustSeeds(ctx)
	if err != nil {
		span.RecordError(err)
		return err
	}

	// without seeds nobody holds trust: the stored scores are still cleared
	var edges []Edge
	if len(seeds) > 0 {
		ackEdges, err := uc.repo.GetLocalAckEdges(ctx, uc.config.FQDN, uc.config.IngestLimit.TrustAckSchemas)
		if err != nil {
			span.RecordError(err)
			return err
		}
		edges = make([]Edge, len(ackEdges))
		for i, edge := range ackEdges {
			edges[i] = Edge{From: edge.From, To: edge.To}
		}
	}

	result := scores(seeds, edges)
	if err := uc.repo.SaveEntityTrust(ctx, result); err != nil {
		span.RecordError(err)
		return err
	}
	slog.Info("entity trust recomputed", slog.Int("seeds", len(seeds)), slog.Int("trustedEntities", len(result)))
	return nil
}

package record

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/cdid"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/schemas"
)

// JobTypeRecordDelivery is the JobQueue job type for federation delivery;
// its payload is a DeliveryJob and its handler is Deliver.
const JobTypeRecordDelivery = "RecordDelivery"

// DeliveryLocalKind describes what to do when a delivery job's destination
// resolves to this server itself.
type DeliveryLocalKind string

// DeliveryRemoteKind describes what to do when a delivery job's destination
// resolves to another domain.
type DeliveryRemoteKind string

const (
	DeliveryLocalNone    DeliveryLocalKind = "none"    // nothing to do locally
	DeliveryLocalPublish DeliveryLocalKind = "publish" // publish Event on Dest/ResolveURI
	DeliveryLocalCommit  DeliveryLocalKind = "commit"  // re-enter Commit(ip, Payload, CommitModeExecute)

	DeliveryRemoteNone   DeliveryRemoteKind = "none"   // nothing to do remotely
	DeliveryRemoteCommit DeliveryRemoteKind = "commit" // POST Payload to the resolved host
)

// DeliveryJob describes a single unit of federation delivery work: resolve a
// destination (unless already known), then act locally and/or remotely. It
// is the payload of a JobTypeRecordDelivery job.
//
// Exactly one of ResolveURI / Host should be set: ResolveURI is a CCURI that
// Deliver must resolve to a host via Client.ResolveResourceHost; Host is an
// already-known destination domain that requires no resolution (e.g. an
// entity's registered Domain).
type DeliveryJob struct {
	ResolveURI string `json:"resolveUri,omitempty"`
	Host       string `json:"host,omitempty"`

	Payload concrnt.SignedDocument `json:"payload,omitempty"`

	Local  DeliveryLocalKind  `json:"local"`
	Remote DeliveryRemoteKind `json:"remote"`

	// Event is used when Local == DeliveryLocalPublish; the publish channel
	// is ResolveURI.
	Event *concrnt.Event `json:"event,omitempty"`

	// IP is used when Local == DeliveryLocalCommit, to re-enter Commit.
	IP string `json:"ip,omitempty"`
}

func (uc *Usecase) createReferenceDistributionActions(ctx context.Context, ip string, author string, href string, requester domain.Entity, sd concrnt.SignedDocument, destinations []string, mode domain.CommitMode) ([]PostProcessAction, error) {
	if mode != domain.CommitModeExecute || len(destinations) == 0 {
		return nil, nil
	}

	requesterSD, err := uc.GetSigned(ctx, requester.CCKVWithHint())
	if err != nil {
		return nil, err
	}

	// the key segment is the hash-based CDID of the href, so a record keeps
	// the same reference key across accept-if-newer overwrites and the new
	// reference replaces the old row instead of piling up next to it
	refSegment := cdid.MakeHash([]byte(href)).String()

	postProcesses := make([]PostProcessAction, 0, len(destinations))
	for _, destURI := range destinations {
		key, err := url.JoinPath(destURI, refSegment)
		if err != nil {
			slog.Error("failed to join path for distribution", slog.String("destination", destURI), slog.String("href", href), slog.String("error", err.Error()))
			continue
		}

		distDoc := concrnt.Document[schemas.Reference]{
			Kind: "record",
			Key:  key,
			Value: schemas.Reference{
				Href: href,
			},
			Author:    author,
			Schema:    schemas.ReferenceURL,
			CreatedAt: time.Now(),
		}
		docBytes, err := json.Marshal(distDoc)
		if err != nil {
			return nil, err
		}
		distSD := concrnt.SignedDocument{
			Document: string(docBytes),
			Proof: concrnt.Proof{
				Type: concrnt.ProofTypeDocumentReference,
				Href: &href,
			},
			References: map[string]concrnt.SignedDocument{
				requester.CCKV(): *requesterSD,
				href:             sd,
			},
		}

		destURI := destURI
		postProcesses = append(postProcesses,
			func(ctx context.Context) error {
				return uc.jobs.Enqueue(ctx, JobTypeRecordDelivery, DeliveryJob{
					ResolveURI: destURI,
					Payload:    distSD,
					Local:      DeliveryLocalCommit,
					Remote:     DeliveryRemoteCommit,
					IP:         ip,
				})
			},
		)
	}

	return postProcesses, nil
}

// enqueueEventDelivery delivers a realtime event job. When the channel's
// owner lives on this server the event is published right away instead of
// waiting behind federation jobs in the queue (Deliver would do nothing
// remote for a local host anyway).
func (uc *Usecase) enqueueEventDelivery(ctx context.Context, job DeliveryJob) error {
	// Deliver's host resolution, with the owner from the entity cache instead
	// of a resolve request; an unknown owner goes through the queue
	local := false
	if parsed, err := concrnt.ParseCCURI(job.ResolveURI); err == nil && job.Event != nil {
		if concrnt.IsCCID(parsed.Owner) {
			isLocal, err := uc.IsLocalEntityByCCID(ctx, parsed.Owner)
			local = err == nil && isLocal
		} else {
			local = parsed.Owner == uc.config.FQDN
		}
	}
	if local {
		err := uc.signal.Publish(ctx, job.ResolveURI, *job.Event)
		if err == nil {
			return nil
		}
		slog.Warn("local publish failed, queueing it for retry", slog.String("channel", job.ResolveURI), slog.String("error", err.Error()))
	}
	return uc.jobs.Enqueue(ctx, JobTypeRecordDelivery, job)
}

// Deliver executes one delivery job produced by this usecase: resolve the
// destination (unless it's already a known host), then act locally (publish
// a realtime event, or re-enter Commit) or remotely (POST to the resolved
// host). It is the handler registered for JobTypeRecordDelivery.
func (uc *Usecase) Deliver(ctx context.Context, job DeliveryJob) error {
	ctx, span := tracer.Start(ctx, "Usecase.Record.Deliver")
	defer span.End()

	host := job.Host
	if job.ResolveURI != "" {
		resolved, err := uc.client.ResolveResourceHost(ctx, job.ResolveURI)
		if err != nil {
			span.RecordError(err)
			return err
		}
		host = resolved
	}

	if host == uc.config.FQDN {
		switch job.Local {
		case DeliveryLocalPublish:
			if job.Event == nil {
				err := fmt.Errorf("delivery: local publish job for %s missing event", job.ResolveURI)
				span.RecordError(err)
				return err
			}
			return uc.signal.Publish(ctx, job.ResolveURI, *job.Event)
		case DeliveryLocalCommit:
			_, err := uc.Commit(ctx, job.IP, job.Payload, domain.CommitModeExecute)
			return err
		default:
			return nil
		}
	}

	if job.Remote == DeliveryRemoteCommit {
		return uc.client.Commit(ctx, host, job.Payload)
	}
	return nil
}

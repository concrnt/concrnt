package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/SherClockHolmes/webpush-go"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/infra/health"
	"github.com/concrnt/concrnt/internal/infra/push"
	"github.com/concrnt/concrnt/schemas"
)

type NotificationUsecase interface {
	List(ctx context.Context) ([]domain.NotificationSubscription, error)
	IncrementCounter(ctx context.Context, vendorID, owner string) (int64, error)
}

type RealtimeUsecase interface {
	Realtime(ctx context.Context, request <-chan []string, response chan<- concrnt.Event)
}

// NotificationDeduper claims a notification key so that the same push is sent
// at most once across replicas (e.g. during a leadership handover). A claim
// whose send failed is released so an overlapping replica may still deliver
// it; a crash between claim and send remains unrecoverable, which is
// acceptable for best-effort push notifications.
type NotificationDeduper interface {
	Claim(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Release(ctx context.Context, key string) error
}

type NotificationReactor struct {
	notification NotificationUsecase
	realtime     RealtimeUsecase
	dedup        NotificationDeduper
	opts         webpush.Options
	heartbeat    *health.Heartbeat
}

// reactorDeadline is the liveness deadline of the sync loop: its 10s tick
// plus one subscription listing, with margin.
const reactorDeadline = 2 * time.Minute

// NewNotificationReactor wires the push reactor; watchdog (nil to skip)
// gets the sync loop's liveness heartbeat.
func NewNotificationReactor(
	notification NotificationUsecase,
	realtime RealtimeUsecase,
	dedup NotificationDeduper,
	opts webpush.Options,
	watchdog *health.Watchdog,
) *NotificationReactor {
	return &NotificationReactor{
		notification: notification,
		realtime:     realtime,
		dedup:        dedup,
		opts:         opts,
		heartbeat:    watchdog.Register("worker/notification-reactor", reactorDeadline),
	}
}

type notificationWorker struct {
	mdate  time.Time
	cancel context.CancelFunc
}

func (r *NotificationReactor) Start(ctx context.Context) {
	go r.run(ctx)
}

func (r *NotificationReactor) run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	workers := make(map[string]notificationWorker)

	defer r.heartbeat.Stop()
	r.heartbeat.Beat()
	for {
		select {
		case <-ctx.Done():
			for _, worker := range workers {
				worker.cancel()
			}
			return
		case <-ticker.C:
			r.heartbeat.Beat()
			r.syncWorkers(ctx, workers)
		}
	}
}

func (r *NotificationReactor) syncWorkers(ctx context.Context, workers map[string]notificationWorker) {
	subscriptions, err := r.notification.List(ctx)
	if err != nil {
		slog.Error("failed to list notification subscriptions", slog.String("error", err.Error()))
		return
	}

	valid := make([]string, 0, len(subscriptions))
	for _, sub := range subscriptions {
		id := sub.VendorID + sub.Owner
		valid = append(valid, id)

		if worker, ok := workers[id]; ok {
			if worker.mdate.Equal(sub.MDate) {
				continue
			}
			worker.cancel()
			delete(workers, id)
		}

		workerCtx, cancel := context.WithCancel(ctx)
		workers[id] = notificationWorker{mdate: sub.MDate, cancel: cancel}
		go r.runWorker(workerCtx, sub)
	}

	for id, worker := range workers {
		if slices.Contains(valid, id) {
			continue
		}
		worker.cancel()
		delete(workers, id)
	}
}

func (r *NotificationReactor) runWorker(ctx context.Context, sub domain.NotificationSubscription) {
	slog.Info("notification worker started", slog.String("vendorID", sub.VendorID), slog.String("owner", sub.Owner))

	var subscription webpush.Subscription
	if err := json.Unmarshal([]byte(sub.Subscription), &subscription); err != nil {
		slog.Error("failed to decode webpush subscription", slog.String("error", err.Error()))
		return
	}

	request := make(chan []string)
	realtime := make(chan concrnt.Event)

	go r.realtime.Realtime(ctx, request, realtime)
	select {
	case request <- sub.Prefixes:
	case <-ctx.Done():
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-realtime:
			if !eventMatchesSchemas(event, sub.Schemas) {
				continue
			}

			claimedKey := ""
			if r.dedup != nil {
				key := notificationDedupKey(event, sub)
				claimed, err := r.dedup.Claim(ctx, key, 5*time.Minute)
				if err != nil {
					// prefer a duplicate push over a lost one
					slog.Warn("failed to claim notification, sending anyway", slog.String("error", err.Error()))
				} else if !claimed {
					continue
				} else {
					claimedKey = key
				}
			}

			notification := buildNotificationPayload(event)
			// the counter means "notifications the user has not looked at yet",
			// so it is bumped regardless of whether the push itself lands
			if count, err := r.notification.IncrementCounter(ctx, sub.VendorID, sub.Owner); err != nil {
				slog.Warn("failed to increment notification counter", slog.String("error", err.Error()))
			} else {
				notification.Badge = count
			}

			payload, err := json.Marshal(notification)
			if err != nil {
				slog.Error("failed to encode notification payload", slog.String("error", err.Error()))
				r.releaseClaim(ctx, claimedKey)
				continue
			}

			resp, err := webpush.SendNotification(payload, &subscription, &r.opts)
			if err != nil {
				push.RecordSend(push.KindNotification, 0, err)
				slog.Error("failed to send notification", slog.String("error", err.Error()))
				r.releaseClaim(ctx, claimedKey)
				continue
			}
			push.RecordSend(push.KindNotification, resp.StatusCode, nil)

			if resp.StatusCode != httpStatusCreated {
				body, readErr := io.ReadAll(resp.Body)
				if readErr != nil {
					slog.Error("failed to read notification response body", slog.String("error", readErr.Error()))
				}
				slog.Error(
					"notification failed",
					slog.String("vendorID", sub.VendorID),
					slog.String("owner", sub.Owner),
					slog.String("status", resp.Status),
					slog.String("body", string(body)),
				)
				r.releaseClaim(ctx, claimedKey)
			}
			resp.Body.Close()
		}
	}
}

// releaseClaim gives the dedup claim back after a failed send so an
// overlapping replica can still deliver the push.
func (r *NotificationReactor) releaseClaim(ctx context.Context, key string) {
	if r.dedup == nil || key == "" {
		return
	}
	if err := r.dedup.Release(ctx, key); err != nil {
		slog.Warn("failed to release notification claim", slog.String("error", err.Error()))
	}
}

const httpStatusCreated = 201

func notificationDedupKey(event concrnt.Event, sub domain.NotificationSubscription) string {
	// the association URI is part of the event identity: "associated" events
	// carry the shared target as URI, so without it two different users
	// reacting to the same post within the TTL would collide and the second
	// push would be silently dropped
	association := ""
	if event.Association != nil {
		association = *event.Association
	}
	sum := sha256.Sum256([]byte(
		event.Source + "|" + event.URI + "|" + event.Type + "|" + association + "|" +
			strconv.FormatInt(event.Timestamp.UnixNano(), 10) + "|" +
			sub.VendorID + "|" + sub.Owner,
	))
	return "notification_dedup:" + hex.EncodeToString(sum[:])
}

func eventMatchesSchemas(event concrnt.Event, filter []string) bool {
	if len(filter) == 0 {
		return true
	}

	for _, sd := range event.References {
		var doc concrnt.Document[any]
		if err := json.Unmarshal([]byte(sd.Document), &doc); err != nil {
			continue
		}
		// Match the document's own schema (e.g. a client subscribing to
		// reference.json directly) as well as the resolved content schema.
		if slices.Contains(filter, doc.Schema) {
			return true
		}
		if eff := effectiveSchema(sd); eff != doc.Schema && slices.Contains(filter, eff) {
			return true
		}
	}

	return false
}

// effectiveSchema returns the content schema a subscription filters on for a
// signed document. A reference record's own schema is always reference.json, so
// a subscription filtering on the underlying content schema (reply, mention,
// ...) would never match; resolve the effective schema the same way createRecord
// does. See internal/usecase/record.go createRecord. Returns "" if the document
// cannot be parsed.
func effectiveSchema(sd concrnt.SignedDocument) string {
	var doc concrnt.Document[any]
	if err := json.Unmarshal([]byte(sd.Document), &doc); err != nil {
		return ""
	}
	if doc.Schema != schemas.ReferenceURL {
		return doc.Schema
	}

	var refDoc concrnt.Document[schemas.Reference]
	if err := json.Unmarshal([]byte(sd.Document), &refDoc); err != nil {
		return doc.Schema
	}
	if refSD, ok := sd.References[refDoc.Value.Href]; ok {
		var targetDoc concrnt.Document[any]
		if err := json.Unmarshal([]byte(refSD.Document), &targetDoc); err == nil && targetDoc.Schema != "" {
			return targetDoc.Schema
		}
	}
	if refDoc.Value.Schema != nil {
		return *refDoc.Value.Schema
	}
	return doc.Schema
}

// buildNotificationPayload reduces a realtime Event to the minimal structure a
// device needs to render a push notification: the association document's URI
// (which the client resolves via /api/v2/resolve for schema-specific fields,
// the actor's profile and the target message body), plus its effective schema,
// author and creation time as conveniences. The full Event embeds signed
// documents and far exceeds the 4096-byte WebPush/FCM limit. On a malformed or
// non-association event it degrades to just the URI/timestamp so the device
// still shows a generic notification rather than dropping it.
func buildNotificationPayload(event concrnt.Event) concrnt.NotificationPayload {
	payload := concrnt.NotificationPayload{
		Type:      concrnt.NotificationTypeNotification,
		URI:       event.URI,
		CreatedAt: event.Timestamp,
	}
	if event.Association == nil {
		return payload
	}
	payload.URI = *event.Association

	sd, ok := event.References[*event.Association]
	if !ok {
		return payload
	}
	payload.Schema = effectiveSchema(sd)

	var doc concrnt.Document[json.RawMessage]
	if err := json.Unmarshal([]byte(sd.Document), &doc); err == nil {
		payload.Author = doc.Author
		if !doc.CreatedAt.IsZero() {
			payload.CreatedAt = doc.CreatedAt
		}
	}
	return payload
}

package firestore

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/notification"
)

type NotificationRepository struct {
	client *firestore.Client
}

func NewNotificationRepository(client *firestore.Client) notification.Repository {
	return &NotificationRepository{client: client}
}

func (r *NotificationRepository) col() *firestore.CollectionRef {
	return r.client.Collection(colSubscriptions)
}

func subscriptionToDomain(m subscriptionDoc) domain.NotificationSubscription {
	return domain.NotificationSubscription{
		VendorID:     m.VendorID,
		Owner:        m.Owner,
		Schemas:      append([]string{}, m.Schemas...),
		Prefixes:     append([]string{}, m.Prefixes...),
		Subscription: m.Subscription,
		CDate:        m.CDate,
		MDate:        m.MDate,
	}
}

// Subscribe upserts the subscription keyed by (vendorID, owner).
func (r *NotificationRepository) Subscribe(ctx context.Context, subscription domain.NotificationSubscription) (domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Subscribe")
	defer span.End()

	ref := r.col().Doc(subscriptionID(subscription.VendorID, subscription.Owner))
	schemas := subscription.Schemas
	if schemas == nil {
		schemas = []string{}
	}
	prefixes := subscription.Prefixes
	if prefixes == nil {
		prefixes = []string{}
	}
	err := upsert(ctx, ref, map[string]any{
		"vendorID":     subscription.VendorID,
		"owner":        subscription.Owner,
		"schemas":      schemas,
		"prefixes":     prefixes,
		"subscription": subscription.Subscription,
		"mDate":        firestore.ServerTimestamp,
	})
	if err != nil {
		span.RecordError(err)
		return domain.NotificationSubscription{}, err
	}
	return r.Get(ctx, subscription.VendorID, subscription.Owner)
}

func (r *NotificationRepository) List(ctx context.Context) ([]domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.List")
	defer span.End()

	it := r.col().Documents(ctx)
	defer it.Stop()

	result := []domain.NotificationSubscription{}
	for {
		snap, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		var m subscriptionDoc
		if err := decode(snap, &m); err != nil {
			span.RecordError(err)
			return nil, err
		}
		result = append(result, subscriptionToDomain(m))
	}
	return result, nil
}

func (r *NotificationRepository) Get(ctx context.Context, vendorID, owner string) (domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Get")
	defer span.End()

	snap, err := r.col().Doc(subscriptionID(vendorID, owner)).Get(ctx)
	if err != nil {
		if isNotFound(err) {
			return domain.NotificationSubscription{}, domain.NotFoundError{Resource: "notification subscription"}
		}
		span.RecordError(err)
		return domain.NotificationSubscription{}, err
	}
	var m subscriptionDoc
	if err := decode(snap, &m); err != nil {
		span.RecordError(err)
		return domain.NotificationSubscription{}, err
	}
	return subscriptionToDomain(m), nil
}

func (r *NotificationRepository) Delete(ctx context.Context, vendorID, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Delete")
	defer span.End()

	if _, err := r.col().Doc(subscriptionID(vendorID, owner)).Delete(ctx); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

package spanner

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/notification"
)

type NotificationRepository struct {
	client *spanner.Client
}

func NewNotificationRepository(client *spanner.Client) notification.Repository {
	return &NotificationRepository{client: client}
}

type subscriptionRow struct {
	VendorID     string    `spanner:"vendor_id"`
	Owner        string    `spanner:"owner"`
	Schemas      []string  `spanner:"schemas"`
	Prefixes     []string  `spanner:"prefixes"`
	Subscription string    `spanner:"subscription"`
	CDate        time.Time `spanner:"c_date"`
	MDate        time.Time `spanner:"m_date"`
}

const subscriptionSelect = "SELECT vendor_id, owner, schemas, prefixes, subscription, c_date, m_date FROM subscriptions"

func (m subscriptionRow) domain() domain.NotificationSubscription {
	schemas, prefixes := m.Schemas, m.Prefixes
	if schemas == nil {
		schemas = []string{}
	}
	if prefixes == nil {
		prefixes = []string{}
	}
	return domain.NotificationSubscription{
		VendorID:     m.VendorID,
		Owner:        m.Owner,
		Schemas:      schemas,
		Prefixes:     prefixes,
		Subscription: m.Subscription,
		CDate:        m.CDate.UTC(),
		MDate:        m.MDate.UTC(),
	}
}

// Subscribe upserts by (vendor_id, owner); a zero CDate / MDate is stamped
// now.
func (r *NotificationRepository) Subscribe(ctx context.Context, subscription domain.NotificationSubscription) (domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Subscribe")
	defer span.End()

	model := subscriptionRow{
		VendorID:     subscription.VendorID,
		Owner:        subscription.Owner,
		Schemas:      subscription.Schemas,
		Prefixes:     subscription.Prefixes,
		Subscription: subscription.Subscription,
		CDate:        subscription.CDate,
		MDate:        subscription.MDate,
	}
	if model.Schemas == nil {
		model.Schemas = []string{}
	}
	if model.Prefixes == nil {
		model.Prefixes = []string{}
	}
	stamp := now()
	if model.CDate.IsZero() {
		model.CDate = stamp
	}
	if model.MDate.IsZero() {
		model.MDate = stamp
	}

	_, err := r.client.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("subscriptions",
		[]string{"vendor_id", "owner", "schemas", "prefixes", "subscription", "c_date", "m_date"},
		[]any{model.VendorID, model.Owner, model.Schemas, model.Prefixes, model.Subscription, model.CDate, model.MDate})})
	if err != nil {
		span.RecordError(err)
		return domain.NotificationSubscription{}, err
	}
	return model.domain(), nil
}

func (r *NotificationRepository) List(ctx context.Context) ([]domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.List")
	defer span.End()

	models, err := QueryAll[subscriptionRow](ctx, r.client.Single(), spanner.Statement{SQL: subscriptionSelect})
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := make([]domain.NotificationSubscription, 0, len(models))
	for _, model := range models {
		result = append(result, model.domain())
	}
	return result, nil
}

func (r *NotificationRepository) Get(ctx context.Context, vendorID, owner string) (domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Get")
	defer span.End()

	model, found, err := QueryOne[subscriptionRow](ctx, r.client.Single(), spanner.Statement{
		SQL:    subscriptionSelect + " WHERE vendor_id = @vendor_id AND owner = @owner",
		Params: map[string]any{"vendor_id": vendorID, "owner": owner},
	})
	if err != nil {
		span.RecordError(err)
		return domain.NotificationSubscription{}, err
	}
	if !found {
		return domain.NotificationSubscription{}, domain.NotFoundError{Resource: "notification subscription"}
	}
	return model.domain(), nil
}

func (r *NotificationRepository) Delete(ctx context.Context, vendorID, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Delete")
	defer span.End()

	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{
			SQL:    "DELETE FROM subscriptions WHERE vendor_id = @vendor_id AND owner = @owner",
			Params: map[string]any{"vendor_id": vendorID, "owner": owner},
		})
		return err
	})
	if err != nil {
		span.RecordError(err)
	}
	return err
}

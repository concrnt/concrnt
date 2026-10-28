package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/notification"
)

type NotificationRepository struct {
	db *DB
}

func NewNotificationRepository(db *DB) notification.Repository {
	return &NotificationRepository{db: db}
}

const subscriptionColumns = "vendor_id, owner, schemas, prefixes, subscription, c_date, m_date"

func scanSubscription(row interface{ Scan(dest ...any) error }) (domain.NotificationSubscription, error) {
	var s domain.NotificationSubscription
	var schemas, prefixes string
	var cDate, mDate int64
	if err := row.Scan(&s.VendorID, &s.Owner, &schemas, &prefixes, &s.Subscription, &cDate, &mDate); err != nil {
		return s, err
	}
	var err error
	if s.Schemas, err = Strings(schemas); err != nil {
		return s, err
	}
	if s.Prefixes, err = Strings(prefixes); err != nil {
		return s, err
	}
	s.CDate, s.MDate = Time(cDate), Time(mDate)
	return s, nil
}

func (r *NotificationRepository) Subscribe(ctx context.Context, subscription domain.NotificationSubscription) (domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Subscribe")
	defer span.End()

	now := time.Now()
	if subscription.CDate.IsZero() {
		subscription.CDate = now
	}
	subscription.MDate = now
	_, err := r.db.ExecContext(ctx, `INSERT INTO subscriptions (`+subscriptionColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (vendor_id, owner) DO UPDATE SET schemas = excluded.schemas, prefixes = excluded.prefixes, subscription = excluded.subscription, m_date = excluded.m_date`,
		subscription.VendorID, subscription.Owner, JSONStrings(subscription.Schemas), JSONStrings(subscription.Prefixes), subscription.Subscription, Micros(subscription.CDate), Micros(subscription.MDate))
	if err != nil {
		span.RecordError(err)
		return domain.NotificationSubscription{}, err
	}
	return r.Get(ctx, subscription.VendorID, subscription.Owner)
}

func (r *NotificationRepository) List(ctx context.Context) ([]domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.List")
	defer span.End()

	rows, err := r.db.QueryContext(ctx, "SELECT "+subscriptionColumns+" FROM subscriptions")
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer rows.Close()

	result := []domain.NotificationSubscription{}
	for rows.Next() {
		s, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

func (r *NotificationRepository) Get(ctx context.Context, vendorID, owner string) (domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Get")
	defer span.End()

	s, err := scanSubscription(r.db.QueryRowContext(ctx, "SELECT "+subscriptionColumns+" FROM subscriptions WHERE vendor_id = ? AND owner = ?", vendorID, owner))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.NotificationSubscription{}, domain.NotFoundError{Resource: "notification subscription"}
	}
	if err != nil {
		span.RecordError(err)
		return domain.NotificationSubscription{}, err
	}
	return s, nil
}

func (r *NotificationRepository) Delete(ctx context.Context, vendorID, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Delete")
	defer span.End()

	_, err := r.db.ExecContext(ctx, "DELETE FROM subscriptions WHERE vendor_id = ? AND owner = ?", vendorID, owner)
	return err
}

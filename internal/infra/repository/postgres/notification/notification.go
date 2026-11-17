package notification

import (
	"context"
	"errors"

	"github.com/lib/pq"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/notification"
)

var tracer = otel.Tracer("repository")

type Repository struct {
	db *gorm.DB
}

var _ notification.Repository = (*Repository)(nil)

func New(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Migrate() error {
	return r.db.AutoMigrate(&Subscription{})
}

func modelToDomainNotification(m Subscription) domain.NotificationSubscription {
	return domain.NotificationSubscription{
		VendorID:     m.VendorID,
		Owner:        m.Owner,
		Schemas:      []string(m.Schemas),
		Prefixes:     []string(m.Prefixes),
		Subscription: m.Subscription,
		CDate:        m.CDate,
		MDate:        m.MDate,
	}
}

func domainToModelNotification(d domain.NotificationSubscription) Subscription {
	return Subscription{
		VendorID:     d.VendorID,
		Owner:        d.Owner,
		Schemas:      pq.StringArray(d.Schemas),
		Prefixes:     pq.StringArray(d.Prefixes),
		Subscription: d.Subscription,
		CDate:        d.CDate,
		MDate:        d.MDate,
	}
}

func (r *Repository) Subscribe(ctx context.Context, subscription domain.NotificationSubscription) (domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Subscribe")
	defer span.End()

	model := domainToModelNotification(subscription)
	if err := r.db.WithContext(ctx).Save(&model).Error; err != nil {
		span.RecordError(err)
		return domain.NotificationSubscription{}, err
	}

	return modelToDomainNotification(model), nil
}

func (r *Repository) List(ctx context.Context) ([]domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.List")
	defer span.End()

	var models []Subscription
	if err := r.db.WithContext(ctx).Find(&models).Error; err != nil {
		span.RecordError(err)
		return nil, err
	}

	result := make([]domain.NotificationSubscription, 0, len(models))
	for _, model := range models {
		result = append(result, modelToDomainNotification(model))
	}
	return result, nil
}

func (r *Repository) Get(ctx context.Context, vendorID, owner string) (domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Get")
	defer span.End()

	var model Subscription
	err := r.db.WithContext(ctx).
		Where("vendor_id = ? AND owner = ?", vendorID, owner).
		Take(&model).Error
	if err != nil {
		span.RecordError(err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return domain.NotificationSubscription{}, domain.NotFoundError{Resource: "notification subscription"}
		}
		return domain.NotificationSubscription{}, err
	}

	return modelToDomainNotification(model), nil
}

func (r *Repository) Delete(ctx context.Context, vendorID, owner string) error {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Delete")
	defer span.End()

	if err := r.db.WithContext(ctx).
		Where("vendor_id = ? AND owner = ?", vendorID, owner).
		Delete(&Subscription{}).Error; err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

package sqlite

import (
	"context"
	"errors"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/notification"
)

type NotificationRepository struct {
	db *gorm.DB
}

func NewNotificationRepository(db *gorm.DB) notification.Repository {
	return &NotificationRepository{db: db}
}

func modelToDomainNotification(m Subscription) domain.NotificationSubscription {
	return domain.NotificationSubscription{
		VendorID:     m.VendorID,
		Owner:        m.Owner,
		Schemas:      []string(m.Schemas),
		Prefixes:     []string(m.Prefixes),
		Subscription: m.Subscription,
		CDate:        Time(m.CDate),
		MDate:        Time(m.MDate),
	}
}

func domainToModelNotification(d domain.NotificationSubscription) Subscription {
	m := Subscription{
		VendorID:     d.VendorID,
		Owner:        d.Owner,
		Schemas:      JSONStrings(d.Schemas),
		Prefixes:     JSONStrings(d.Prefixes),
		Subscription: d.Subscription,
	}
	// a zero date is left for the hook to stamp
	if !d.CDate.IsZero() {
		m.CDate = Micros(d.CDate)
	}
	if !d.MDate.IsZero() {
		m.MDate = Micros(d.MDate)
	}
	return m
}

func (r *NotificationRepository) Subscribe(ctx context.Context, subscription domain.NotificationSubscription) (domain.NotificationSubscription, error) {
	ctx, span := tracer.Start(ctx, "Repository.Notification.Subscribe")
	defer span.End()

	model := domainToModelNotification(subscription)
	if err := r.db.WithContext(ctx).Save(&model).Error; err != nil {
		span.RecordError(err)
		return domain.NotificationSubscription{}, err
	}

	return modelToDomainNotification(model), nil
}

func (r *NotificationRepository) List(ctx context.Context) ([]domain.NotificationSubscription, error) {
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

func (r *NotificationRepository) Get(ctx context.Context, vendorID, owner string) (domain.NotificationSubscription, error) {
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

func (r *NotificationRepository) Delete(ctx context.Context, vendorID, owner string) error {
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

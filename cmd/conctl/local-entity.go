package main

import (
	"context"
	"errors"

	"gorm.io/gorm"

	"github.com/concrnt/concrnt/internal/infra/repository/postgres"
)

// localEntityChecker returns a memoizing predicate for "is this CCID an entity
// registered on this server" (entities.domain == fqdn), the locality test the
// repair operations share when deciding which side of a relationship this
// server holds.
func localEntityChecker(ctx context.Context, db *gorm.DB, fqdn string) func(ccid string) (bool, error) {
	cache := map[string]bool{}
	return func(ccid string) (bool, error) {
		if v, ok := cache[ccid]; ok {
			return v, nil
		}
		var entity postgres.Entity
		err := db.WithContext(ctx).Select("id", "domain").Where("id = ?", ccid).Take(&entity).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return false, err
		}
		local := err == nil && entity.Domain == fqdn
		cache[ccid] = local
		return local, nil
	}
}

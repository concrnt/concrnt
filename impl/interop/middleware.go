package interop

import (
	"context"
	"encoding/json"

	"github.com/labstack/echo/v5"

	"github.com/concrnt/concrnt/impl/tags"
)

func ReceiveGatewayAuthPropagation(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx, span := tracer.Start(c.Request().Context(), "Auth.Service.ReceiveGatewayAuthPropagation")
		defer span.End()

		header := c.Request().Header

		requesterHeader := header.Get(RequesterHeader)
		if requesterHeader != "" {
			var requester Entity
			err := json.Unmarshal([]byte(requesterHeader), &requester)
			if err == nil {
				ctx = context.WithValue(ctx, RequesterCtxKey, requester)
			}
		}

		tagHeader := header.Get(RequesterTagHeader)
		if tagHeader != "" {
			tag := tags.Parse(tagHeader)
			ctx = context.WithValue(ctx, RequesterTagCtxKey, tag)
		}

		c.SetRequest(c.Request().WithContext(ctx))
		return next(c)
	}
}

package middleware

import (
	"context"
	"fmt"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/xinguang/go-recaptcha"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/impl/interop"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/usecase/server"
	"github.com/concrnt/concrnt/jwt"
	"github.com/concrnt/concrnt/schemas"
)

var tracer = otel.Tracer("auth")

type AuthMiddleware struct {
	config domain.Config
	client *client.Client
	server *server.Usecase
	record *record.Usecase
}

func NewAuthMiddleware(
	config domain.Config,
	client *client.Client,
	server *server.Usecase,
	record *record.Usecase,
) *AuthMiddleware {
	return &AuthMiddleware{
		config: config,
		client: client,
		server: server,
		record: record,
	}
}

func (s *AuthMiddleware) IdentifyIdentity(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, span := tracer.Start(c.Request().Context(), "Auth.Service.IdentifyIdentity")
		defer span.End()

		// # authtoken
		// 実体はjwtトークン
		// requesterが本人であることを証明するのに使う。
		authHeader := c.Request().Header.Get("authorization")

		if authHeader != "" {
			split := strings.Split(authHeader, " ")
			if len(split) != 2 {
				span.RecordError(fmt.Errorf("invalid authentication header"))
				goto skipCheckAuthorization
			}

			authType, token := split[0], split[1]
			if authType != "Bearer" {
				span.RecordError(fmt.Errorf("only Bearer is acceptable"))
				goto skipCheckAuthorization
			}

			header, claims, err := jwt.Parse(token)
			if err != nil {
				span.RecordError(errors.Wrap(err, "jwt validation failed"))
				goto skipCheckAuthorization
			}

			if claims.Issuer == s.config.CSID { // login as service account

				err = jwt.Validate(token, s.config.CSID)
				if err != nil {
					span.RecordError(errors.Wrap(err, "jwt signature validation failed"))
					goto skipCheckAuthorization
				}

				ctx = context.WithValue(ctx, interop.RequesterCtxKey, s.config.CSID)
				span.SetAttributes(attribute.String("RequesterId", s.config.CSID))
				ctx = context.WithValue(ctx, interop.ServiceAccountTypeCtxKey, claims.Subject)
				span.SetAttributes(attribute.String("ServiceAccountType", claims.Subject))

				c.SetRequest(c.Request().WithContext(ctx))
				return next(c)
			}

			if claims.Audience != s.config.FQDN && claims.Audience != s.config.CSID {
				err := fmt.Errorf("jwt audience mismatch: expected %s, got %s", s.config.FQDN, claims.Audience)
				span.RecordError(err)
				goto skipCheckAuthorization
			}

			if claims.Subject != "concrnt" {
				err := fmt.Errorf("invalid subject")
				span.RecordError(err)
				goto skipCheckAuthorization
			}

			requester, err := s.record.GetEntity(ctx, claims.Issuer)
			if err != nil {
				span.RecordError(errors.Wrap(err, "AuthMiddleware.IdentifyIdentity: s.entity.Get failed"))
				goto skipCheckAuthorization
			}
			entityTag := requester.Tag()

			if entityTag.Has("_blocked") {
				err := fmt.Errorf("entity is blocked")
				return echo.NewHTTPError(403, err.Error())
			}

			srv, err := s.server.Resolve(ctx, requester.Domain, nil)
			if err != nil {
				span.RecordError(errors.Wrap(err, "AuthMiddleware.IdentifyIdentity: s.server.Resolve failed"))
				goto skipCheckAuthorization
			}
			srvTag := srv.Tag()

			if srvTag.Has("_blocked") {
				err := fmt.Errorf("server is blocked")
				return echo.NewHTTPError(403, err.Error())
			}

			keyID := header.KeyID
			if keyID == "" {
				keyID = claims.Issuer
			}

			parsed, err := concrnt.ParseCCURI(keyID)
			if err != nil {
				span.RecordError(errors.Wrap(err, "failed to parse issuer as CCURI"))
				goto skipCheckAuthorization
			}
			ccid := parsed.Owner

			parsedIssuer, err := concrnt.ParseCCURI(claims.Issuer)
			if err != nil {
				span.RecordError(errors.Wrap(err, "failed to parse issuer as CCURI"))
				goto skipCheckAuthorization
			}
			issuerCCID := parsedIssuer.Owner

			// The KeyID header is caller-supplied: the key it names must
			// belong to the JWT's issuer, whose entity becomes the requester.
			// Without this binding, a valid signature by one identity's key
			// could authenticate as any other issuer.
			if ccid != issuerCCID {
				span.RecordError(fmt.Errorf("key owner does not match jwt issuer: expected %s, got %s", issuerCCID, ccid))
				goto skipCheckAuthorization
			}

			if parsed.Key == "" { // login as raw key

				err = jwt.Validate(token, ccid)
				if err != nil {
					span.RecordError(errors.Wrap(err, "jwt signature validation failed"))
					goto skipCheckAuthorization
				}

			} else { // login as subkey

				// GetRecord verifies the subkey document's signature by
				// default and caches both the document and the verification
				// result. CIP-13 §6: the enact document itself must be
				// master-key (ecrecover-direct) signed — an enact signed by
				// another subkey must not authenticate.
				var subKeyDoc concrnt.Document[schemas.Subkey]
				err := s.client.GetRecord(ctx, keyID, &client.Options{
					AllowedProofTypes: []string{concrnt.ProofTypeEcrecover},
				}, &subKeyDoc)
				if err != nil {
					span.RecordError(err)
					goto skipCheckAuthorization
				}

				// CIP-13: only an enact document (schema subkey.json)
				// authorizes a subkey. A revoked-subkey document resolving at
				// the key means the subkey is no longer valid for
				// authentication (§7), so it is rejected here too.
				if subKeyDoc.Schema != schemas.SubkeyURL {
					span.RecordError(fmt.Errorf("subkey document is not a subkey enact document: %s", subKeyDoc.Schema))
					goto skipCheckAuthorization
				}

				if subKeyDoc.Author != ccid {
					span.RecordError(fmt.Errorf("subkey document author does not match issuer"))
					goto skipCheckAuthorization
				}

				err = jwt.Validate(token, subKeyDoc.Value.CKID)
				if err != nil {
					span.RecordError(errors.Wrap(err, "jwt signature validation failed"))
					goto skipCheckAuthorization
				}
			}

			ctx = context.WithValue(ctx, interop.RequesterCtxKey, *requester)
			span.SetAttributes(attribute.String("RequesterId", ccid))

			ctx = context.WithValue(ctx, interop.RequesterTagCtxKey, entityTag)
			span.SetAttributes(attribute.String("RequesterTag", entityTag.ToString()))
		}

	skipCheckAuthorization:
		c.SetRequest(c.Request().WithContext(ctx))
		return next(c)
	}
}

// Recaptcha is a middleware factory that returns a middleware to verify reCAPTCHA challenges.
// It expects the challenge response in the "captcha" header and uses the provided validator.
// If verification is successful, it sets a flag in the context.
func Recaptcha(validator *recaptcha.ReCAPTCHA) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			ctx, span := tracer.Start(c.Request().Context(), "Auth.Service.Recaptcha")
			defer span.End()

			challenge := c.Request().Header.Get("captcha")
			if challenge != "" {
				err := validator.Verify(challenge)
				if err == nil {
					ctx = context.WithValue(ctx, interop.CaptchaVerifiedCtxKey, true)
				} else {
					ctx = context.WithValue(ctx, interop.CaptchaVerifiedCtxKey, false)
				}
			} else {
				ctx = context.WithValue(ctx, interop.CaptchaVerifiedCtxKey, false)
			}

			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
}

package rest

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/concrnt/concrnt/internal/worker"
)

// LocalDemand reports the realtime subscription demand of this replica's own
// registered clients only — no cluster awareness. This is what a leader polls
// on every replica to build the cluster-wide demand set, so it must never
// itself forward to another replica (that would loop leader->peer->leader).
type LocalDemand interface {
	CurrentSubscriptions() []string
}

// AggregateDemand reports the realtime subscription demand across the whole
// cluster, as tracked by whichever replica currently runs the singleton
// subscriber (the leader).
type AggregateDemand interface {
	CurrentSubscriptions() []string
}

// Ensurer requests that peer subscriptions cover the given prefixes; only
// meaningful on the leader, which is the only replica that dials peers.
type Ensurer interface {
	EnsureSubscriptions(ctx context.Context, prefixes []string)
}

// LeaderState tells whether this replica currently runs the singleton workers.
type LeaderState interface {
	IsLeader() bool
}

// SubscriberCoordinationHandler serves the replica-to-replica API that keeps
// the cluster's single federation Subscriber fed with every replica's
// realtime demand: workers forward and query subscription state here, and the
// leader polls each replica's local demand. Mount it only on the internal
// listener — these endpoints must not be reachable from outside the cluster.
type SubscriberCoordinationHandler struct {
	local     LocalDemand
	aggregate AggregateDemand
	ensurer   Ensurer
	leader    LeaderState
}

func NewSubscriberCoordinationHandler(local LocalDemand, aggregate AggregateDemand, ensurer Ensurer, leader LeaderState) *SubscriberCoordinationHandler {
	return &SubscriberCoordinationHandler{
		local:     local,
		aggregate: aggregate,
		ensurer:   ensurer,
		leader:    leader,
	}
}

func (h *SubscriberCoordinationHandler) RegisterRoutes(e *echo.Echo) {
	e.GET("/internal/demand", h.handleDemand)
	e.GET("/internal/subscriptions", h.handleSubscriptions)
	e.POST("/internal/subscriptions/ensure", h.handleEnsure)
}

// handleDemand returns the prefixes wanted by this replica's own realtime
// sessions. The leader polls this on every replica (including itself) to
// build the cluster-wide demand set.
func (h *SubscriberCoordinationHandler) handleDemand(c *echo.Context) error {
	prefixes := h.local.CurrentSubscriptions()
	if prefixes == nil {
		prefixes = []string{}
	}
	return c.JSON(http.StatusOK, prefixes)
}

// handleSubscriptions returns the cluster-wide aggregate subscription state.
// Only the leader can answer this — it is the only replica that actually
// knows it, and forwarding would let two GET /internal/subscriptions handlers
// loop off each other during a leadership handover.
func (h *SubscriberCoordinationHandler) handleSubscriptions(c *echo.Context) error {
	if !h.leader.IsLeader() {
		return c.String(http.StatusConflict, "not the leader")
	}

	prefixes := h.aggregate.CurrentSubscriptions()
	if prefixes == nil {
		prefixes = []string{}
	}
	return c.JSON(http.StatusOK, prefixes)
}

// handleEnsure applies subscription demand immediately. Only the leader runs
// the Subscriber, so non-leaders reject the request; the caller falls back to
// the periodic peer poll.
func (h *SubscriberCoordinationHandler) handleEnsure(c *echo.Context) error {
	if !h.leader.IsLeader() {
		return c.String(http.StatusConflict, "not the leader")
	}

	var req worker.EnsureRequest
	if err := c.Bind(&req); err != nil {
		return c.String(http.StatusBadRequest, "invalid request")
	}

	h.ensurer.EnsureSubscriptions(c.Request().Context(), req.Prefixes)
	return c.String(http.StatusOK, "ok")
}

package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/impl/interop"
)

const (
	// receiveTimeout bounds a silent read; a ping then tells a quiet channel
	// from a dead connection
	receiveTimeout = 30 * time.Second
	// receiveBackoff spaces the retries while Redis is unreachable
	receiveBackoff = 500 * time.Millisecond
)

// RedisPubsub is the worker.PubSub on Redis Pub/Sub. Redis matches patterns
// per connection and in time linear in their number, so instead of one
// PSUBSCRIBE per subscriber the replica holds a single subscription to every
// event channel and matches prefixes in-process (fanout).
type RedisPubsub struct {
	rdb  *redis.Client
	fan  *fanout
	once sync.Once
}

func NewRedisPubsub(redisClient *redis.Client) *RedisPubsub {
	return &RedisPubsub{
		rdb: redisClient,
		fan: newFanout(),
	}
}

func (s *RedisPubsub) Publish(ctx context.Context, channel string, event concrnt.Event) error {

	event.Source = channel

	jsonstr, err := json.Marshal(event)
	if err != nil {
		return err
	}

	err = s.rdb.Publish(ctx, interop.EventChannelPrefix+channel, jsonstr).Err()
	if err != nil {
		return err

	}

	return nil
}

func (s *RedisPubsub) Subscribe(ctx context.Context, prefixes []string, response chan<- concrnt.Event) error {
	if len(prefixes) == 0 {
		return nil
	}
	s.start()
	s.fan.add(ctx, prefixes, response)
	return nil
}

func (s *RedisPubsub) SubscribeAll(ctx context.Context, response chan<- concrnt.Event) error {
	s.start()
	s.fan.add(ctx, nil, response)
	return nil
}

// OnGap implements worker.PubSubGaps.
func (s *RedisPubsub) OnGap(fn func()) {
	s.fan.addGapHook(fn)
}

// start opens the replica's one subscription on first use; it lives as
// long as the process.
func (s *RedisPubsub) start() {
	s.once.Do(func() {
		go s.receive(context.Background())
	})
}

// receive reads the subscription and dispatches. go-redis drops a broken
// connection and dials again on the next read, re-issuing the PSUBSCRIBE;
// the Subscription reply that follows an error is how a gap is detected.
func (s *RedisPubsub) receive(ctx context.Context) {
	pubsub := s.rdb.PSubscribe(ctx, interop.EventChannelPrefix+"*")
	defer pubsub.Close()

	gapped := false
	for {
		reply, err := pubsub.ReceiveTimeout(ctx, receiveTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				// a quiet channel; the ping fails on a dead connection and
				// the next read reconnects
				if err := pubsub.Ping(ctx); err != nil {
					slog.Warn("pubsub: ping failed", slog.String("error", err.Error()))
				}
				continue
			}
			if !gapped {
				slog.Warn("pubsub: subscription lost, reconnecting", slog.String("error", err.Error()))
				gapped = true
			}
			time.Sleep(receiveBackoff)
			continue
		}

		switch msg := reply.(type) {
		case *redis.Subscription:
			if gapped {
				gapped = false
				slog.Info("pubsub: subscription re-established; events in between were lost")
				s.fan.gap()
			}
		case *redis.Message:
			var item concrnt.Event
			if err := json.Unmarshal([]byte(msg.Payload), &item); err != nil {
				slog.Error("failed to unmarshal event", slog.String("error", err.Error()))
				continue
			}
			s.fan.dispatch(strings.TrimPrefix(msg.Channel, interop.EventChannelPrefix), item)
		}
	}
}

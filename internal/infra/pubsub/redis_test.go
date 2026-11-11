package pubsub_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/infra/pubsub"
	"github.com/concrnt/concrnt/internal/testutil"
)

// One Redis subscription per replica serves every local subscriber, and a
// killed connection is re-established with the gap reported.
func TestRedisPubsubFanoutAndGap(t *testing.T) {
	rdb, cleanup := testutil.CreateRDB()
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ps := pubsub.NewRedisPubsub(rdb)
	gaps := make(chan struct{}, 4)
	ps.OnGap(func() { gaps <- struct{}{} })

	alice := make(chan concrnt.Event, 8)
	all := make(chan concrnt.Event, 8)
	if err := ps.Subscribe(ctx, []string{"cckv://alice/home"}, alice); err != nil {
		t.Fatal(err)
	}
	if err := ps.SubscribeAll(ctx, all); err != nil {
		t.Fatal(err)
	}
	waitForPubsubClients(t, rdb, 1)

	publish := func(channel string) {
		t.Helper()
		if err := ps.Publish(ctx, channel, concrnt.Event{Type: "created", URI: channel}); err != nil {
			t.Fatal(err)
		}
	}
	publish("cckv://alice/home/post1")
	publish("cckv://bob/home/post1")

	got := receive(t, alice, 1)
	if got[0].URI != "cckv://alice/home/post1" || got[0].Source != "cckv://alice/home/post1" {
		t.Fatalf("prefix subscriber got %+v", got[0])
	}
	if got := receive(t, all, 2); len(got) != 2 {
		t.Fatalf("subscribe-all got %d events", len(got))
	}
	select {
	case ev := <-alice:
		t.Fatalf("prefix subscriber received a foreign event %+v", ev)
	case <-time.After(200 * time.Millisecond):
	}

	// drop the subscription connection: go-redis dials again on the next
	// read and the hook runs once the PSUBSCRIBE is answered
	if err := rdb.Do(ctx, "CLIENT", "KILL", "TYPE", "pubsub").Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gaps:
	case <-time.After(10 * time.Second):
		t.Fatal("gap hook did not run after the connection was killed")
	}
	waitForPubsubClients(t, rdb, 1)

	publish("cckv://alice/home/post2")
	if got := receive(t, alice, 1); got[0].URI != "cckv://alice/home/post2" {
		t.Fatalf("after reconnect got %+v", got[0])
	}
}

func receive(t *testing.T, ch chan concrnt.Event, n int) []concrnt.Event {
	t.Helper()
	out := make([]concrnt.Event, 0, n)
	for len(out) < n {
		select {
		case ev := <-ch:
			out = append(out, ev)
		case <-time.After(5 * time.Second):
			t.Fatalf("received %d of %d events", len(out), n)
		}
	}
	return out
}

// waitForPubsubClients waits until Redis reports n pubsub connections.
func waitForPubsubClients(t *testing.T, rdb *redis.Client, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		list, err := rdb.Do(context.Background(), "CLIENT", "LIST", "TYPE", "pubsub").Text()
		if err != nil {
			t.Fatal(err)
		}
		if len(strings.Fields(strings.ReplaceAll(list, " ", "_"))) == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pubsub clients:\n%s\nwant %d", list, n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Every replica holds its own subscription on the shared Redis, so an event
// published once, by whichever replica committed, reaches each realtime
// session exactly once: on the replica the session is connected to, once
// per event however many of the session's prefixes match the channel.
func TestRedisPubsubTwoReplicasDeliverOnce(t *testing.T) {
	rdb, cleanup := testutil.CreateRDB()
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// a second client on the same Redis is a second replica
	other := redis.NewClient(rdb.Options())
	defer other.Close()
	replicas := []*pubsub.RedisPubsub{pubsub.NewRedisPubsub(rdb), pubsub.NewRedisPubsub(other)}

	const timeline = "cckv://alice/home-timeline"
	sessions := make([]chan concrnt.Event, 0, 4)
	for _, replica := range replicas {
		for range 2 {
			session := make(chan concrnt.Event, 64)
			// the timeline twice over: by itself and by a shorter prefix
			if err := replica.Subscribe(ctx, []string{timeline, "cckv://alice/home"}, session); err != nil {
				t.Fatal(err)
			}
			sessions = append(sessions, session)
		}
	}
	waitForPubsubClients(t, rdb, len(replicas))

	const likes = 20
	for i := range likes {
		// the commits land on either replica
		event := concrnt.Event{Type: "associated", URI: "cckv://alice/posts/1"}
		if err := replicas[i%len(replicas)].Publish(ctx, timeline, event); err != nil {
			t.Fatal(err)
		}
		// the target's own channel, which a timeline subscriber does not follow
		if err := replicas[i%len(replicas)].Publish(ctx, event.URI, event); err != nil {
			t.Fatal(err)
		}
	}

	for i, session := range sessions {
		receive(t, session, likes)
		select {
		case ev := <-session:
			t.Fatalf("session %d received more than one event per association: %+v", i, ev)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

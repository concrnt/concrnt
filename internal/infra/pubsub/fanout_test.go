package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/concrnt/concrnt"
)

func TestFanoutMatchesPrefixes(t *testing.T) {
	f := newFanout()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alice := make(chan concrnt.Event, 4)
	all := make(chan concrnt.Event, 4)
	f.add(ctx, []string{"cckv://alice/home", "cckv://alice/notify"}, alice)
	f.add(ctx, nil, all)

	f.dispatch("cckv://alice/home/post1", concrnt.Event{URI: "a"})
	f.dispatch("cckv://bob/home/post1", concrnt.Event{URI: "b"})
	f.dispatch("cckv://alice/notify/x", concrnt.Event{URI: "c"})

	if got := drain(alice); len(got) != 2 || got[0].URI != "a" || got[1].URI != "c" {
		t.Fatalf("prefix subscriber got %+v, want a and c", got)
	}
	if got := drain(all); len(got) != 3 {
		t.Fatalf("subscribe-all got %d events, want 3", len(got))
	}
}

func TestFanoutDropsWhenFullAndRemovesOnCancel(t *testing.T) {
	f := newFanout()
	ctx, cancel := context.WithCancel(context.Background())

	slow := make(chan concrnt.Event, 1)
	f.add(ctx, []string{"cckv://"}, slow)
	f.dispatch("cckv://a/1", concrnt.Event{URI: "first"})
	f.dispatch("cckv://a/2", concrnt.Event{URI: "second"}) // dropped, must not block

	if got := drain(slow); len(got) != 1 || got[0].URI != "first" {
		t.Fatalf("got %+v, want only the first event", got)
	}

	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for {
		f.mu.Lock()
		n := len(f.subs)
		f.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscriber not removed after cancel")
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.dispatch("cckv://a/3", concrnt.Event{URI: "third"})
	if got := drain(slow); len(got) != 0 {
		t.Fatalf("a cancelled subscriber received %+v", got)
	}
}

func TestFanoutGapHooks(t *testing.T) {
	f := newFanout()
	calls := 0
	f.addGapHook(func() { calls++ })
	f.addGapHook(func() { calls++ })
	f.gap()
	if calls != 2 {
		t.Fatalf("gap ran %d hooks, want 2", calls)
	}
}

func drain(ch chan concrnt.Event) []concrnt.Event {
	var out []concrnt.Event
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

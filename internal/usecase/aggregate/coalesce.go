package aggregate

import (
	"context"
	"sync"
)

// coalescer shares one read of a key between the callers that want it at
// the same moment. Clients re-read a target's counts when an event about it
// arrives, so a popular target is read by every session subscribed to it at
// once; without sharing, each of those reads holds a store connection for
// the same answer.
//
// At most one read per key is in flight. Callers that arrive while it runs
// do not take its result, which may predate a write they have already seen
// committed: they wait for the next read, which starts when the one in
// flight ends, and share that one. So a result is never older than the
// arrival of the caller it is given to, and a key costs at most one read
// per round trip however many callers ask.
type coalescer struct {
	mu   sync.Mutex
	keys map[string]*keyReads
}

// keyReads exists while a read of its key is in flight.
type keyReads struct {
	// next is the read that follows the one in flight, nil until a caller
	// asks for it.
	next *sharedRead
}

type sharedRead struct {
	// start is closed when the read before this one has ended.
	start chan struct{}
	// done is closed when entry and err are set.
	done  chan struct{}
	entry Counters
	err   error
}

func newCoalescer() *coalescer {
	return &coalescer{keys: map[string]*keyReads{}}
}

// do returns what read returns for key, from a call of its own or from the
// one call made for every caller that waited together. A shared result is
// the same value for all of them and must not be modified.
func (c *coalescer) do(ctx context.Context, key string, read func(ctx context.Context) (Counters, error)) (Counters, error) {
	c.mu.Lock()
	reads, busy := c.keys[key]
	if !busy {
		c.keys[key] = &keyReads{}
		c.mu.Unlock()
		defer c.ended(key)
		return read(ctx)
	}

	if shared := reads.next; shared != nil {
		c.mu.Unlock()
		select {
		case <-shared.done:
		case <-ctx.Done():
			return Counters{}, ctx.Err()
		}
		if shared.err != nil {
			// the failure may be the leading caller's own (its request was
			// cancelled): this caller reads for itself
			return read(ctx)
		}
		return shared.entry, nil
	}

	// lead the next read on behalf of everyone who joins before it starts
	shared := &sharedRead{start: make(chan struct{}), done: make(chan struct{})}
	reads.next = shared
	c.mu.Unlock()

	select {
	case <-shared.start:
	case <-ctx.Done():
		// the callers that joined still need the turn to pass on
		shared.err = ctx.Err()
		go func() {
			<-shared.start
			c.ended(key)
			close(shared.done)
		}()
		return Counters{}, shared.err
	}
	defer close(shared.done)
	defer c.ended(key)
	shared.err = context.Canceled // what the waiters see if read panics
	shared.entry, shared.err = read(ctx)
	return shared.entry, shared.err
}

// ended passes the turn to the read that waits for key, or forgets the key
// when none does.
func (c *coalescer) ended(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	reads := c.keys[key]
	if reads.next == nil {
		delete(c.keys, key)
		return
	}
	close(reads.next.start)
	reads.next = nil
}

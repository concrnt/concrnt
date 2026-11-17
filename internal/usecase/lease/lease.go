package lease

import "context"

type Lease interface {
	Acquire(ctx context.Context, name string) (release func(), err error)
}

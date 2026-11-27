package domain

import "testing"

func TestIngestLimitDomainLimit(t *testing.T) {
	limit := IngestLimit{Min: 30, Max: 3000}

	cases := []struct {
		name  string
		limit IngestLimit
		trust float64
		want  int
	}{
		{"no trust gets min", limit, 0, 30},
		{"full trust gets max", limit, 1, 3000},
		{"more than full trust gets max", limit, 1.5, 3000},
		// geometric: halfway in trust is the geometric mean, not the midpoint
		{"half trust", limit, 0.5, 300},
		{"a little trust lifts a little", limit, 0.1, 48},
		{"without max everyone gets min", IngestLimit{Min: 30}, 1, 30},
		{"max below min is ignored", IngestLimit{Min: 30, Max: 10}, 1, 30},
		{"unlimited min stays unlimited", IngestLimit{Max: 3000}, 0.5, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.limit.DomainLimit(c.trust); got != c.want {
				t.Fatalf("DomainLimit(%v) = %d, want %d", c.trust, got, c.want)
			}
		})
	}
}

package migrations

import (
	"context"
	"maps"
	"slices"
	"testing"
)

type fakeRepo struct{ state map[string]string }

func (r *fakeRepo) GetState(ctx context.Context, name string) (string, error) {
	return r.state[name], nil
}

func (r *fakeRepo) SetState(ctx context.Context, name string, state string) error {
	r.state[name] = state
	return nil
}

type fakeLease struct{}

func (fakeLease) Acquire(ctx context.Context, key string) (func(), error) {
	return func() {}, nil
}

func TestDoRunsMigrationsUpToTheRunningVersion(t *testing.T) {
	saved := maps.Clone(migrations)
	t.Cleanup(func() { migrations = saved })

	cases := []struct {
		name    string
		current string
		version string
		ran     []string
		state   string
	}{
		{"release below the migration", "v1.0.0", "v1.1.5", []string{"v1.1.0"}, "v1.1.0"},
		{"dev build below the pre-release", "v1.1.0", "v1.1.1-dev.3+gabcdef0", nil, "v1.1.0"},
		{"the pre-release itself", "v1.1.0", "v1.2.0-beta.0", []string{"v1.2.0-beta.0"}, "v1.2.0-beta.0"},
		{"dev build after the pre-release", "v1.1.0", "v1.2.0-beta.0.dev.2+gabcdef0", []string{"v1.2.0-beta.0"}, "v1.2.0-beta.0"},
		{"release skipping the pre-release", "v1.0.0", "v1.2.0", []string{"v1.1.0", "v1.2.0-beta.0"}, "v1.2.0-beta.0"},
		{"everything", "v1.0.0", "v1.3.0", []string{"v1.1.0", "v1.2.0-beta.0", "v1.3.0"}, "v1.3.0"},
		{"database ahead of the binary", "v1.3.0", "v1.1.5", nil, "v1.3.0"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var ran []string
			migrations = make(map[string]func(ctx context.Context) error)
			for _, v := range []string{"v1.1.0", "v1.2.0-beta.0", "v1.3.0"} {
				migrations[v] = func(ctx context.Context) error {
					ran = append(ran, v)
					return nil
				}
			}

			repo := &fakeRepo{state: map[string]string{VERSION_KEY: c.current}}
			err := NewMigrator(repo, fakeLease{}).Do(context.Background(), c.version)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(ran, c.ran) {
				t.Errorf("ran %v, want %v", ran, c.ran)
			}
			if repo.state[VERSION_KEY] != c.state {
				t.Errorf("state %q, want %q", repo.state[VERSION_KEY], c.state)
			}
		})
	}
}

package domain

type Config struct {
	FQDN         string         `yaml:"fqdn"`
	PrivateKey   string         `yaml:"privatekey"`
	Registration string         `yaml:"registration"` // open, invite, close
	Layer        string         `yaml:"layer"`
	CSID         string         `yaml:"csid"`
	Meta         map[string]any `yaml:"meta,omitempty"`
	Debug        bool           `yaml:"debug,omitempty"`
	IngestLimit  IngestLimit    `yaml:"ingestLimit,omitempty"`
}

// IngestLimit caps the commits accepted from other servers' users, in
// commits per minute. A zero entry leaves that bucket unlimited.
type IngestLimit struct {
	// Unknown is one bucket shared by every author this server holds no
	// entity for yet, whatever domain they claim: it bounds first contacts,
	// and with them the entity lookups a stranger can make this server do.
	Unknown int `yaml:"unknown"`
	// Min is the bucket each remote domain gets for its known users.
	Min int `yaml:"min"`
}

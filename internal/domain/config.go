package domain

import "math"

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
	// Min is the bucket a remote domain this server does not trust gets for
	// its known users.
	Min int `yaml:"min"`
	// Max is the bucket of a fully trusted remote domain. Domains in between
	// get a bucket between Min and Max by their trust. Without Max, or
	// without Trust.Seeds, every domain gets Min.
	Max   int         `yaml:"max"`
	Trust IngestTrust `yaml:"trust"`
}

// IngestTrust configures how remote domains are trusted; see the trust
// usecase.
type IngestTrust struct {
	// Seeds are the CCIDs of the local users trusted outright. Trust reaches
	// everyone else through whom they acknowledge.
	Seeds []string `yaml:"seeds"`
	// AckSchemas are the schemas of the acknowledgements that pass trust on.
	// Empty means every acknowledgement does.
	AckSchemas []string `yaml:"ackSchemas"`
}

// DomainLimit is the bucket of a remote domain trusted by trust (0 to 1).
// It runs from Min to Max geometrically, so that each step in trust
// multiplies the bucket rather than adding to it: a little trust lifts a
// domain only a little above Min.
func (l IngestLimit) DomainLimit(trust float64) int {
	if l.Min <= 0 || l.Max <= l.Min || trust <= 0 {
		return l.Min
	}
	if trust >= 1 {
		return l.Max
	}
	return int(math.Round(float64(l.Min) * math.Pow(float64(l.Max)/float64(l.Min), trust)))
}

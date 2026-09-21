package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/schemas"
	"github.com/gorilla/websocket"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
)

var tracer = otel.Tracer("client")

const (
	defaultTimeout = 3 * time.Second
	maxFailCount   = 23 // max 10 minutes
)

type Client struct {
	client          *http.Client
	cache           *cache.Cache
	lastFailed      map[string]time.Time
	failCount       map[string]int
	onlineMu        sync.RWMutex
	userAgent       string
	defaultResolver string
	remappings      map[string]*url.URL
}

func New(defaultResolver string) *Client {
	httpClient := http.Client{
		Timeout: defaultTimeout,
	}

	c := &Client{
		client:          &httpClient,
		cache:           cache.New(10*time.Minute, 15*time.Minute),
		lastFailed:      make(map[string]time.Time),
		failCount:       make(map[string]int),
		defaultResolver: defaultResolver,
		remappings:      make(map[string]*url.URL),
	}
	httpClient.Transport = otelhttp.NewTransport(c)
	// a process normally has exactly one federation client; a second one
	// (tests) must not panic on the duplicate collector, so ignore that error
	_ = prometheus.Register(newPeerOfflineCollector(c))
	go c.UpKeeper()
	return c
}

// offlineHosts lists the peer hosts currently considered offline.
func (c *Client) offlineHosts() []string {
	c.onlineMu.RLock()
	defer c.onlineMu.RUnlock()
	hosts := make([]string, 0, len(c.lastFailed))
	for host, lastFailed := range c.lastFailed {
		if !lastFailed.IsZero() {
			hosts = append(hosts, host)
		}
	}
	return hosts
}

func (c *Client) SetUserAgent(software, version string) {
	c.userAgent = fmt.Sprintf("%s/%s (Concrnt)", software, version)
}

func (c *Client) AddHostRemapping(host string, target string) {
	parsed, err := url.Parse(target)
	if err != nil {
		slog.Warn("Failed to parse remapping target "+target, "error", err)
		return
	}
	// url.Parse accepts values like "" or "concrnt:8000" (scheme "concrnt",
	// empty host) without error; installing such a remap breaks every request
	// to the remapped host
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		slog.Warn("Ignoring remapping target " + target + ": scheme must be http or https (e.g. http://concrnt:8000)")
		return
	}
	if parsed.Host == "" {
		slog.Warn("Ignoring remapping target " + target + ": host is empty")
		return
	}
	c.remappings[host] = parsed
}

func (c *Client) IsOnline(domain string) bool {
	c.onlineMu.RLock()
	defer c.onlineMu.RUnlock()

	lastFailed, ok := c.lastFailed[domain]
	if !ok {
		return true
	}
	if lastFailed.IsZero() {
		return true
	}
	return false
}

func (c *Client) UpKeeper() {
	ctx := context.Background()
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		c.onlineMu.RLock()
		domains := make(map[string]time.Time, len(c.lastFailed))
		maps.Copy(domains, c.lastFailed)
		c.onlineMu.RUnlock()

		for domain, lastFailed := range domains {
			c.onlineMu.Lock()
			if _, ok := c.failCount[domain]; !ok {
				c.failCount[domain] = 0
			}
			failCount := c.failCount[domain]
			c.onlineMu.Unlock()

			// exponential backoff (max 10 minutes)
			span := 0.5 * math.Pow(1.5, float64(min(failCount, maxFailCount)))
			if time.Since(lastFailed) > time.Duration(span)*time.Second {
				err := c.healthCheckDomain(ctx, domain)
				if err != nil {
					slog.Info(fmt.Sprintf("Domain %s is offline. Fail count: %d", domain, failCount))
					c.onlineMu.Lock()
					c.lastFailed[domain] = time.Now()
					c.failCount[domain]++
					c.onlineMu.Unlock()
				} else {
					slog.Info(fmt.Sprintf("Domain %s is back online :3", domain))
					c.onlineMu.Lock()
					delete(c.lastFailed, domain)
					delete(c.failCount, domain)
					c.onlineMu.Unlock()
				}
			}
		}
	}
}

func (c *Client) healthCheckDomain(ctx context.Context, domain string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://"+domain+"/.well-known/concrnt", nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get well-known concrnt from %s: status code %d", domain, resp.StatusCode)
	}

	var wkc concrnt.WellKnownConcrnt
	return json.NewDecoder(resp.Body).Decode(&wkc)
}

func (c *Client) markOffline(domain string) {
	c.onlineMu.Lock()
	defer c.onlineMu.Unlock()
	c.lastFailed[domain] = time.Now()
}

func (c *Client) markOfflineIfTimeout(domain string, action string, err error) {
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		slog.Warn("Mark domain "+domain+" as offline while "+action, "error", err)
		c.markOffline(domain)
	}
}

type Options struct {
	Resolver string
	NoCache  bool

	// SkipVerify skips the signed document's proof/signature verification
	// performed by GetRecord. Only use this for cases where strict
	// verification is unnecessary, e.g. resolving routing hints, where
	// the final data is verified separately once actually used.
	SkipVerify bool

	// AllowedProofTypes restricts which proof types the fetched document may
	// carry (nil = no restriction). E.g. CIP-13 requires subkey enact
	// documents to be ecrecover-direct signed, so the auth middleware fetches
	// them with []string{concrnt.ProofTypeEcrecover}.
	AllowedProofTypes []string
}

// QueryParams are the CIP-5 §3.1 query parameters. OrderBy "" or "createdAt"
// pages by Since/Until; OrderBy "key" (Parent only) pages by the SinceKey/
// UntilKey cckv cursors instead.
type QueryParams struct {
	Prefix   string
	Parent   string
	Schema   string
	Author   string
	OrderBy  string
	Since    *time.Time
	Until    *time.Time
	SinceKey string
	UntilKey string
	Limit    int
	Order    string
}

var ErrEndpointMissing = errors.New("concrnt endpoint missing")

func (c *Client) GetClient() *http.Client {
	return c.client
}

func (c *Client) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, span := tracer.Start(req.Context(), "HTTP "+req.Method)
	defer span.End()

	peerHost := req.Host
	start := time.Now()

	if remap, ok := c.remappings[req.Host]; ok {
		req.Host = remap.Host
		req.URL.Host = remap.Host
		req.URL.Scheme = remap.Scheme
	}

	req.Header.Set("User-Agent", c.userAgent)

	span.SetAttributes(attribute.String("http.method", req.Method))
	span.SetAttributes(attribute.String("http.url", req.URL.String()))

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := http.DefaultTransport.RoundTrip(req)
	code := 0
	if resp != nil {
		code = resp.StatusCode
	}
	observePeerRequest(peerHost, code, err, time.Since(start).Seconds())
	return resp, err
}

func (c *Client) resolveResolver(ctx context.Context, resolver string) (string, error) {
	ctx, span := tracer.Start(ctx, "Client.resolveResolver")
	defer span.End()

	span.SetAttributes(attribute.String("resolver", resolver))

	if resolver == "" {
		return c.defaultResolver, nil
	}

	if concrnt.IsCCID(resolver) {
		var entity concrnt.Document[schemas.Entity]
		err := c.GetRecord(
			ctx,
			concrnt.ComposeCCURI("cckv", resolver, ""),
			// This is only used to resolve a routing hint (which domain to
			// talk to); the record's authenticity isn't load-bearing here,
			// so strict signature verification is unnecessary.
			&Options{Resolver: c.defaultResolver, SkipVerify: true},
			&entity,
		)
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to get entity record for ccid %s", resolver), err)
			span.RecordError(err)
			return "", err
		}
		span.SetAttributes(attribute.String("entity_domain", entity.Value.Domain))
		return entity.Value.Domain, nil
	}

	if concrnt.IsCSID(resolver) {
		wkc, err := c.GetServer(ctx, resolver, nil)
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to get server for csid %s", resolver), err)
			span.RecordError(err)
			return "", err
		}
		span.SetAttributes(attribute.String("server_domain", wkc.Domain))
		return wkc.Domain, nil
	}

	return resolver, nil
}

func (c *Client) ResolveResourceHost(ctx context.Context, uri string) (string, error) {
	ctx, span := tracer.Start(ctx, "Client.ResolveResourceHost")
	defer span.End()

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		err := errors.Join(fmt.Errorf("invalid cc uri %s", uri), err)
		span.RecordError(err)
		return "", err
	}

	return c.resolveResolver(ctx, parsed.Owner)
}

func (c *Client) GetServer(ctx context.Context, domainOrCSID string, hint *string) (concrnt.WellKnownConcrnt, error) {
	ctx, span := tracer.Start(ctx, "Client.GetServer")
	defer span.End()

	cacheKey := "server:" + domainOrCSID

	x, found := c.cache.Get(cacheKey)
	if found {
		return x.(concrnt.WellKnownConcrnt), nil
	}

	if concrnt.IsCSID(domainOrCSID) {
		var wkc concrnt.WellKnownConcrnt
		resolver := c.defaultResolver
		if hint != nil {
			resolver = *hint
		}
		err := c.GetResource(
			ctx,
			"cckv://"+domainOrCSID,
			"application/json",
			&Options{Resolver: resolver},
			&wkc,
		)
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to get well-known concrnt for csid %s. resolver: %s", domainOrCSID, resolver), err)
			span.RecordError(err)
			return concrnt.WellKnownConcrnt{}, err
		}
		c.cache.Set(cacheKey, wkc, cache.DefaultExpiration)
		return wkc, nil
	} else {

		domain := domainOrCSID

		if !c.IsOnline(domain) {
			return concrnt.WellKnownConcrnt{}, fmt.Errorf("Domain is offline")
		}

		url := "https://" + domain + "/.well-known/concrnt"
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to create request for well-known concrnt at %s", url), err)
			span.RecordError(err)
			return concrnt.WellKnownConcrnt{}, err
		}
		resp, err := c.client.Do(req)
		if err != nil {
			c.markOfflineIfTimeout(domain, "getting well-known concrnt", err)
			err := errors.Join(fmt.Errorf("failed to perform request for well-known concrnt at %s", url), err)
			span.RecordError(err)
			return concrnt.WellKnownConcrnt{}, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			err := errors.Join(fmt.Errorf("failed to get well-known concrnt from %s", url), err)
			span.RecordError(err)
			c.markOffline(domain)
			return concrnt.WellKnownConcrnt{}, err
		}
		var wkc concrnt.WellKnownConcrnt
		err = json.NewDecoder(resp.Body).Decode(&wkc)
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to decode well-known concrnt from %s", url), err)
			span.RecordError(err)
			return concrnt.WellKnownConcrnt{}, err
		}
		c.cache.Set(cacheKey, wkc, cache.DefaultExpiration)
		return wkc, nil
	}
}

func (c *Client) ResolveResourceURI(ctx context.Context, uri string, opts *Options) (string, error) {
	ctx, span := tracer.Start(ctx, "Client.ResolveResourceURI")
	defer span.End()

	if opts == nil {
		opts = &Options{}
	}

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		err := errors.Join(fmt.Errorf("invalid cc uri %s", uri), err)
		span.RecordError(err)
		return "", err
	}

	endpoint := uri

	if parsed.Scheme != "http" {
		var info concrnt.WellKnownConcrnt
		if opts.Resolver != "" {
			info, err = c.GetServer(ctx, opts.Resolver, nil)
			if err != nil {
				err := errors.Join(fmt.Errorf("failed to get server for resolver %s", opts.Resolver), err)
				span.RecordError(err)
				return "", err
			}
		} else {
			domain, err := c.resolveResolver(ctx, parsed.Owner)
			if err != nil {
				err := errors.Join(fmt.Errorf("failed to resolve default resolver for owner %s", parsed.Owner), err)
				span.RecordError(err)
				return "", err
			}
			info, err = c.GetServer(ctx, domain, nil)
			if err != nil {
				err := errors.Join(fmt.Errorf("failed to get server for default resolver %s", domain), err)
				span.RecordError(err)
				return "", err
			}
		}

		desc, ok := info.Endpoints["net.concrnt.core.resolve"]
		if !ok {
			err := fmt.Errorf("resource endpoint not found in server %s", info.Domain)
			span.RecordError(err)
			return "", err
		}

		path, err := concrnt.RenderURITemplate(desc, map[string]string{
			"owner": parsed.Owner,
			"key":   parsed.Key,
			"uri":   url.QueryEscape(uri),
		})
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to render resource endpoint template for server %s", info.Domain), err)
			span.RecordError(err)
			return "", err
		}

		endpoint = "https://" + info.Domain + path
	}

	return endpoint, nil
}

func (c *Client) GetResource(ctx context.Context, uri string, accept string, opts *Options, result any) error {
	ctx, span := tracer.Start(ctx, "Client.GetResource")
	defer span.End()

	if opts == nil {
		opts = &Options{}
	}

	// ==== cache check =============
	cacheKey := "resource:" + uri + ":" + accept
	if !opts.NoCache {
		x, found := c.cache.Get(cacheKey)
		if found {
			resultBytes := x.([]byte)
			err := json.Unmarshal(resultBytes, &result)
			if err != nil {
				err := errors.Join(fmt.Errorf("failed to unmarshal cached resource for uri %s", uri), err)
				span.RecordError(err)
				return err
			}
			return nil
		}
	}
	// ==============================

	parsed, err := concrnt.ParseCCURI(uri)
	if err != nil {
		err := errors.Join(fmt.Errorf("invalid cc uri %s", uri), err)
		span.RecordError(err)
		return err
	}

	endpoint := uri

	if parsed.Scheme != "http" {
		var info concrnt.WellKnownConcrnt
		if opts.Resolver != "" {
			info, err = c.GetServer(ctx, opts.Resolver, nil)
			if err != nil {
				err := errors.Join(fmt.Errorf("failed to get server for resolver %s", opts.Resolver), err)
				span.RecordError(err)
				return err
			}
		} else {
			domain, err := c.resolveResolver(ctx, parsed.Owner)
			if err != nil {
				err := errors.Join(fmt.Errorf("failed to resolve default resolver for owner %s", parsed.Owner), err)
				span.RecordError(err)
				return err
			}
			info, err = c.GetServer(ctx, domain, nil)
			if err != nil {
				err := errors.Join(fmt.Errorf("failed to get server for default resolver %s", domain), err)
				span.RecordError(err)
				return err
			}
		}

		desc, ok := info.Endpoints["net.concrnt.core.resolve"]
		if !ok {
			err := fmt.Errorf("resource endpoint not found in server %s", info.Domain)
			span.RecordError(err)
			return err
		}

		path, err := concrnt.RenderURITemplate(desc, map[string]string{
			"owner": parsed.Owner,
			"key":   parsed.Key,
			"uri":   url.QueryEscape(uri),
		})
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to render resource endpoint template for server %s", info.Domain), err)
			span.RecordError(err)
			return err
		}

		endpoint = "https://" + info.Domain + path
	}

	endpointURL, err := url.Parse(endpoint)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to parse endpoint for resource %s", uri), err)
		span.RecordError(err)
		return err
	}
	domain := endpointURL.Hostname()
	if domain != "" && !c.IsOnline(domain) {
		return fmt.Errorf("Domain is offline")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to create request for resource %s", uri), err)
		span.RecordError(err)
		return err
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		c.markOfflineIfTimeout(domain, "getting resource", err)
		err := errors.Join(fmt.Errorf("failed to perform request for resource %s", uri), err)
		span.RecordError(err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("failed to get resource %s: status code %d", uri, resp.StatusCode)
		span.RecordError(err)
		return err
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to decode resource %s", uri), err)
		span.RecordError(err)
		return err
	}

	if !opts.NoCache {
		bytes, err := json.Marshal(result)
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to marshal resource for caching for uri %s", uri), err)
			span.RecordError(err)
			return err
		}
		c.cache.Set(cacheKey, bytes, cache.DefaultExpiration)
	}

	return nil
}

func (c *Client) GetRecord(ctx context.Context, uri string, opts *Options, result any) error {
	ctx, span := tracer.Start(ctx, "Client.GetRecord")
	defer span.End()

	var sd concrnt.SignedDocument
	err := c.GetResource(ctx, uri, "application/json", opts, &sd)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to get signed document for resource %s", uri), err)
		span.RecordError(err)
		return err
	}

	if opts == nil || !opts.SkipVerify {
		// The top-level proof-type restriction is a plain field check on the
		// document we already hold, so it is enforced here directly — the
		// verification cache below then only ever records unrestricted
		// verifications and stays valid for restricted and unrestricted
		// callers alike.
		if opts != nil && opts.AllowedProofTypes != nil && !slices.Contains(opts.AllowedProofTypes, sd.Proof.Type) {
			err := fmt.Errorf("proof type %s is not allowed for resource %s (allowed: %s)", sd.Proof.Type, uri, strings.Join(opts.AllowedProofTypes, ", "))
			span.RecordError(err)
			return err
		}
		// GetResource caches the raw (unverified) signed document, so cache
		// hits would re-pay signature verification on every call — remember
		// successful verifications separately, keyed by content hash so a
		// re-fetched document can never ride an older entry's verification.
		// Hot path: the auth middleware verifies the subkey document once
		// per authenticated request.
		verifiedKey := "verified:" + uri
		proofBytes, err := json.Marshal(sd.Proof)
		if err != nil {
			span.RecordError(err)
			return err
		}
		docHash := string(concrnt.GetHash(append([]byte(sd.Document), proofBytes...)))
		useCache := opts == nil || !opts.NoCache
		verified := false
		if useCache {
			if x, found := c.cache.Get(verifiedKey); found {
				hash, ok := x.(string)
				verified = ok && hash == docHash
			}
		}
		if !verified {
			err := sd.Verify(ctx, &optionsResolver{c: c, opts: opts})
			if err != nil {
				err := errors.Join(fmt.Errorf("signature verification failed for resource %s", uri), err)
				span.RecordError(err)
				return err
			}
			if useCache {
				c.cache.Set(verifiedKey, docHash, cache.DefaultExpiration)
			}
		}
	}

	err = json.Unmarshal([]byte(sd.Document), &result)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to decode document in signed document for resource %s", uri), err)
		span.RecordError(err)
		return err
	}

	return nil
}

// InvalidateResource drops any cached copy (and remembered verification) of
// the resource at uri, so the next fetch observes the latest version.
func (c *Client) InvalidateResource(uri string) {
	c.cache.Delete("resource:" + uri + ":application/json")
	c.cache.Delete("verified:" + uri)
}

// ResolveSignedDocument fetches the signed document at uri, satisfying
// concrnt.DocumentResolver so a *Client can be passed directly to
// SignedDocument.Verify.
func (c *Client) ResolveSignedDocument(ctx context.Context, uri string) (concrnt.SignedDocument, error) {
	return (&optionsResolver{c: c}).ResolveSignedDocument(ctx, uri)
}

// optionsResolver adapts a *Client plus per-call Options (e.g. a routing
// Resolver hint) to concrnt.DocumentResolver, for verification paths that
// need to honor the caller's Options while fetching referenced documents.
type optionsResolver struct {
	c    *Client
	opts *Options
}

func (r *optionsResolver) ResolveSignedDocument(ctx context.Context, uri string) (concrnt.SignedDocument, error) {
	var sd concrnt.SignedDocument
	err := r.c.GetResource(ctx, uri, "application/json", r.opts, &sd)
	if err != nil {
		return concrnt.SignedDocument{}, err
	}
	return sd, nil
}

func (c *Client) Query(ctx context.Context, resolver string, params QueryParams) (concrnt.QueryResult, error) {
	ctx, span := tracer.Start(ctx, "Client.Query")
	defer span.End()

	if params.Prefix != "" && params.Parent != "" {
		err := errors.New("prefix and parent cannot be specified at the same time")
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}

	if params.Order != "" && params.Order != "asc" && params.Order != "desc" {
		err := fmt.Errorf("invalid order parameter: %s", params.Order)
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}

	if params.OrderBy != "" && params.OrderBy != "createdAt" && params.OrderBy != "key" {
		err := fmt.Errorf("invalid orderby parameter: %s", params.OrderBy)
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}
	if params.OrderBy == "key" && params.Prefix != "" {
		err := errors.New("orderby=key is only available with parent")
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}

	domain, err := c.resolveResolver(ctx, resolver)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to resolve resolver %s", resolver), err)
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}
	if domain == "" {
		err := errors.New("resolver cannot be empty")
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}

	server, err := c.GetServer(ctx, domain, nil)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to get server for resolver %s", domain), err)
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}

	desc, ok := server.Endpoints["net.concrnt.core.query"]
	if !ok {
		err := errors.Join(fmt.Errorf("query endpoint not found in server %s", server.Domain), ErrEndpointMissing)
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}

	args := map[string]string{}
	if params.Prefix != "" {
		args["prefix"] = params.Prefix
	}
	if params.Parent != "" {
		args["parent"] = params.Parent
	}
	if params.Schema != "" {
		args["schema"] = params.Schema
	}
	if params.Author != "" {
		args["author"] = params.Author
	}
	if params.OrderBy == "key" {
		args["orderby"] = params.OrderBy
		if params.SinceKey != "" {
			args["since"] = params.SinceKey
		}
		if params.UntilKey != "" {
			args["until"] = params.UntilKey
		}
	} else {
		if params.OrderBy != "" {
			args["orderby"] = params.OrderBy
		}
		if params.Since != nil {
			args["since"] = params.Since.UTC().Format(time.RFC3339Nano)
		}
		if params.Until != nil {
			args["until"] = params.Until.UTC().Format(time.RFC3339Nano)
		}
	}
	if params.Limit > 0 {
		args["limit"] = fmt.Sprint(params.Limit)
	}
	if params.Order != "" {
		args["order"] = params.Order
	}

	path, err := concrnt.RenderURITemplate(desc, args)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to render query endpoint template for server %s", server.Domain), err)
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}
	if !c.IsOnline(server.Domain) {
		return concrnt.QueryResult{}, fmt.Errorf("Domain is offline")
	}
	url := "https://" + server.Domain + path

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to create request for query to %s", url), err)
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		c.markOfflineIfTimeout(server.Domain, "querying", err)
		err := errors.Join(fmt.Errorf("failed to perform query to %s", url), err)
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("failed to query %s: status code %d", url, resp.StatusCode)
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}

	var result concrnt.QueryResult
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to decode query response from %s", url), err)
		span.RecordError(err)
		return concrnt.QueryResult{}, err
	}

	return result, nil
}

// Call invokes a named concrnt API (an entry in the target server's
// /.well-known/concrnt Endpoints map, e.g. "net.concrnt.core.acknowledges")
// and decodes the JSON response into result.
func (c *Client) Call(ctx context.Context, resolver string, endpoint string, params map[string]string, opts *Options, result any) error {
	ctx, span := tracer.Start(ctx, "Client.Call")
	defer span.End()

	if opts == nil {
		opts = &Options{}
	}

	domain, err := c.resolveResolver(ctx, resolver)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to resolve resolver %s", resolver), err)
		span.RecordError(err)
		return err
	}
	if domain == "" {
		err := errors.New("resolver cannot be empty")
		span.RecordError(err)
		return err
	}

	server, err := c.GetServer(ctx, domain, nil)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to get server for resolver %s", domain), err)
		span.RecordError(err)
		return err
	}

	desc, ok := server.Endpoints[endpoint]
	if !ok {
		err := errors.Join(fmt.Errorf("endpoint %s not found in server %s", endpoint, server.Domain), ErrEndpointMissing)
		span.RecordError(err)
		return err
	}

	path, err := concrnt.RenderURITemplate(desc, params)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to render endpoint template %s for server %s", endpoint, server.Domain), err)
		span.RecordError(err)
		return err
	}
	url := "https://" + server.Domain + path

	// ==== cache check =============
	cacheKey := "call:" + url
	if !opts.NoCache {
		x, found := c.cache.Get(cacheKey)
		if found {
			resultBytes := x.([]byte)
			err := json.Unmarshal(resultBytes, &result)
			if err != nil {
				err := errors.Join(fmt.Errorf("failed to unmarshal cached call response for %s", url), err)
				span.RecordError(err)
				return err
			}
			return nil
		}
	}
	// ==============================

	if !c.IsOnline(server.Domain) {
		return fmt.Errorf("Domain is offline")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to create request for call to %s", url), err)
		span.RecordError(err)
		return err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		c.markOfflineIfTimeout(server.Domain, "calling "+endpoint, err)
		err := errors.Join(fmt.Errorf("failed to perform call to %s", url), err)
		span.RecordError(err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("failed to call %s: status code %d", url, resp.StatusCode)
		span.RecordError(err)
		return err
	}

	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to decode call response from %s", url), err)
		span.RecordError(err)
		return err
	}

	if !opts.NoCache {
		bytes, err := json.Marshal(result)
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to marshal call response for caching for %s", url), err)
			span.RecordError(err)
			return err
		}
		c.cache.Set(cacheKey, bytes, cache.DefaultExpiration)
	}

	return nil
}

func (c *Client) Commit(ctx context.Context, resolver string, sd concrnt.SignedDocument) error {
	ctx, span := tracer.Start(ctx, "Client.Commit")
	defer span.End()

	if resolver == "" || resolver == c.defaultResolver {
		resolver = c.defaultResolver
	} else {
		domain, err := c.resolveResolver(ctx, resolver)
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to resolve resolver %s", resolver), err)
			span.RecordError(err)
			return err
		}
		resolver = domain
	}

	if resolver == "" {
		err := fmt.Errorf("resolver cannot be empty")
		span.RecordError(err)
		return err
	}

	server, err := c.GetServer(ctx, resolver, nil)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to get server for resolver %s", resolver), err)
		span.RecordError(err)
		return err
	}

	desc, ok := server.Endpoints["net.concrnt.core.commit"]
	if !ok {
		err := fmt.Errorf("commit endpoint not found in server %s", server.Domain)
		span.RecordError(err)
		return err
	}

	path, err := concrnt.RenderURITemplate(desc, map[string]string{})
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to render commit endpoint template for server %s", server.Domain), err)
		span.RecordError(err)
		return err
	}
	if !c.IsOnline(server.Domain) {
		return fmt.Errorf("Domain is offline")
	}
	url := "https://" + server.Domain + path

	body, err := json.Marshal(sd)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to marshal signed document for commit to %s", url), err)
		span.RecordError(err)
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", url, io.NopCloser(bytes.NewReader(body)))
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to create request for commit to %s", url), err)
		span.RecordError(err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		c.markOfflineIfTimeout(server.Domain, "committing", err)
		err := errors.Join(fmt.Errorf("failed to perform request for commit to %s", url), err)
		span.RecordError(err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("failed to commit to %s: status code %d", url, resp.StatusCode)
		span.RecordError(err)
		return err
	}

	return nil
}

func (c *Client) Realtime(ctx context.Context, fqdn string) (*websocket.Conn, error) {
	_, span := tracer.Start(ctx, "Client.Realtime")
	defer span.End()

	server, err := c.GetServer(ctx, fqdn, nil)
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to get server for realtime connection to %s", fqdn), err)
		span.RecordError(err)
		return nil, err
	}

	desc, ok := server.Endpoints["net.concrnt.core.realtime"]
	if !ok {
		err := fmt.Errorf("realtime endpoint not found in server %s", server.Domain)
		span.RecordError(err)
		return nil, err
	}

	path, err := concrnt.RenderURITemplate(desc, map[string]string{})
	if err != nil {
		err := errors.Join(fmt.Errorf("failed to render realtime endpoint template for server %s", server.Domain), err)
		span.RecordError(err)
		return nil, err
	}
	domain := server.Domain

	if !c.IsOnline(domain) {
		return nil, fmt.Errorf("Domain is offline")
	}

	u := url.URL{Scheme: "wss", Host: domain, Path: path}
	dialer := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 10 * time.Second,
	}

	header := http.Header{}
	header.Set("User-Agent", c.userAgent)

	conn, _, err := dialer.DialContext(ctx, u.String(), header)
	if err != nil {
		slog.Warn("Failed to connect to websocket. Mark domain "+domain+" as offline", "error", err)
		c.markOffline(domain)
		span.RecordError(err)
		return nil, err
	}

	return conn, nil
}

// requests: map[requestID]map[key]url
// -> map[key]http.Response
//
// Domains are fetched concurrently and independently: an offline or failing
// domain only loses its own keys, the other domains' responses are still
// returned.
func (c *Client) BatchGet(ctx context.Context, requests map[string]map[string]string) (map[string]*http.Response, error) {
	ctx, span := tracer.Start(ctx, "Client.BatchGet")
	defer span.End()

	var responses = make(map[string]*http.Response)
	var mu sync.Mutex

	domains := make([]string, 0, len(requests))
	for domain := range requests {
		domains = append(domains, domain)
	}

	runBounded(batchDomainConcurrency, len(domains), func(i int) {
		domain := domains[i]
		reqs := requests[domain]

		if !c.IsOnline(domain) {
			span.RecordError(fmt.Errorf("Domain %s is offline", domain))
			return
		}

		info, err := c.GetServer(ctx, domain, nil)
		if err != nil {
			err := errors.Join(fmt.Errorf("failed to get server for domain %s", domain), err)
			span.RecordError(err)
			return
		}

		desc, ok := info.Endpoints["net.concrnt.core.batch"]
		if ok {
			path, err := concrnt.RenderURITemplate(desc, map[string]string{})
			if err != nil {
				err := errors.Join(fmt.Errorf("failed to render batch endpoint template for server %s", info.Domain), err)
				span.RecordError(err)
				return
			}
			endpoint := "https://" + info.Domain + path

			requests := make(map[string]*http.Request)
			for key, url := range reqs {
				req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
				if err != nil {
					err := errors.Join(fmt.Errorf("failed to create request for batch get to %s", url), err)
					span.RecordError(err)
					continue
				}
				requests[key] = req
			}

			responces, err := DoBatchRequestWithClient(ctx, c.client, endpoint, requests)
			if err != nil {
				err := errors.Join(fmt.Errorf("failed to perform batch get to %s", endpoint), err)
				span.RecordError(err)
				return
			}

			mu.Lock()
			maps.Copy(responses, responces)
			mu.Unlock()

		} else {
			keys := make([]string, 0, len(reqs))
			for key := range reqs {
				keys = append(keys, key)
			}

			runBounded(batchFallbackConcurrency, len(keys), func(i int) {
				key := keys[i]
				url := reqs[key]

				req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
				if err != nil {
					err := errors.Join(fmt.Errorf("failed to create request for get to %s", url), err)
					span.RecordError(err)
					return
				}
				resp, err := c.client.Do(req)
				if err != nil {
					c.markOfflineIfTimeout(domain, "batch get", err)
					err := errors.Join(fmt.Errorf("failed to perform get to %s", url), err)
					span.RecordError(err)
					return
				}
				mu.Lock()
				responses[key] = resp
				mu.Unlock()
			})
		}
	})

	return responses, nil
}

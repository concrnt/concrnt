package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/client"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/policy"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type GlobalParameters struct {
	FQDN string `json:"fqdn"`
}

type PolicyService struct {
	globalPolicy     policy.Policy
	globalParameters GlobalParameters
	client           *client.Client
	cache            *cache.Cache

	refreshMu  sync.Mutex
	refreshing map[string]bool
}

func NewPolicyService(
	globalPolicy policy.Policy,
	globalParameters GlobalParameters,
	client *client.Client,
) *PolicyService {
	return &PolicyService{
		globalPolicy:     globalPolicy,
		globalParameters: globalParameters,
		client:           client,
		cache:            cache.New(policyStaleFor, time.Hour),
		refreshing:       map[string]bool{},
	}
}

// policyAllowedAPIs is the in-code allowlist of named concrnt APIs that
// policy ConcrntCall expressions may invoke. Checked before any resolution
// or network I/O.
var policyAllowedAPIs = []string{
	"net.concrnt.core.acknowledges",
}

func (s *PolicyService) ConcrntCall(ctx context.Context, resolver string, api string, params map[string]string) (any, error) {
	ctx, span := tracer.Start(ctx, "Policy.Service.ConcrntCall")
	defer span.End()

	if !slices.Contains(policyAllowedAPIs, api) {
		err := fmt.Errorf("api %q is not allowed in policy evaluation", api)
		span.RecordError(err)
		return nil, err
	}

	var result any
	err := s.client.Call(ctx, resolver, api, params, nil, &result)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// List endpoints return the paged envelope (CIP-5 §3.2); policy
	// expressions like IsNotEmpty operate on the item list (CIP-12 §4.2.5).
	if envelope, ok := result.(map[string]any); ok {
		if items, ok := envelope["items"]; ok {
			return items, nil
		}
	}

	return result, nil
}

// A policy URL is fetched over HTTP and kept for policyStaleFor; past
// policyFreshFor it is served as is while one refresh runs in the
// background, so a commit, delivery or query never waits on the policy
// host once a replica has the document. Only the very first use of a URL
// on a replica fetches synchronously.
const (
	policyFreshFor = 10 * time.Minute
	policyStaleFor = 24 * time.Hour
)

var policyHTTP = &http.Client{Timeout: 10 * time.Second}

type cachedPolicy struct {
	policy    []byte
	fetchedAt time.Time
}

func (s *PolicyService) ResolvePolicyURL(ctx context.Context, policyURL string) (policy.Policy, error) {
	ctx, span := tracer.Start(ctx, "Policy.Service.ResolvePolicyURL")
	defer span.End()

	if x, found := s.cache.Get(policyURL); found {
		cached := x.(cachedPolicy)
		var policy20251223 policy.Policy
		if err := json.Unmarshal(cached.policy, &policy20251223); err != nil {
			span.RecordError(err)
			s.cache.Delete(policyURL) // remove corrupted cache
			return policy.Policy{}, err
		}
		if time.Since(cached.fetchedAt) > policyFreshFor {
			// refetch in the background, once at a time per URL; a failed
			// fetch keeps the stale copy and the next stale read retries
			s.refreshMu.Lock()
			refreshing := s.refreshing[policyURL]
			s.refreshing[policyURL] = true
			s.refreshMu.Unlock()
			if !refreshing {
				go s.refreshPolicyURL(policyURL)
			}
		}
		return policy20251223, nil
	}

	policyBytes, err := s.fetchPolicyURL(ctx, policyURL)
	if err != nil {
		span.RecordError(err)
		return policy.Policy{}, err
	}
	var policy20251223 policy.Policy
	if err := json.Unmarshal(policyBytes, &policy20251223); err != nil {
		span.RecordError(err)
		return policy.Policy{}, err
	}
	s.cache.Set(policyURL, cachedPolicy{policy: policyBytes, fetchedAt: time.Now()}, policyStaleFor)
	return policy20251223, nil
}

func (s *PolicyService) refreshPolicyURL(policyURL string) {
	defer func() {
		s.refreshMu.Lock()
		delete(s.refreshing, policyURL)
		s.refreshMu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), policyHTTP.Timeout)
	defer cancel()
	policyBytes, err := s.fetchPolicyURL(ctx, policyURL)
	if err != nil {
		slog.Warn("policy: refresh failed, keeping the stale copy", slog.String("url", policyURL), slog.String("error", err.Error()))
		return
	}
	s.cache.Set(policyURL, cachedPolicy{policy: policyBytes, fetchedAt: time.Now()}, policyStaleFor)
}

// fetchPolicyURL downloads the policy document and returns the JSON of the
// version this server evaluates.
func (s *PolicyService) fetchPolicyURL(ctx context.Context, policyURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, policyURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := policyHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("policy %s: status %d", policyURL, resp.StatusCode)
	}

	var policyDoc policy.PolicyDocument
	if err := json.NewDecoder(resp.Body).Decode(&policyDoc); err != nil {
		return nil, err
	}
	policyAny, ok := policyDoc.Versions["2025-12-23"]
	if !ok {
		return nil, fmt.Errorf("unsupported policy version in %s", policyURL)
	}
	return json.Marshal(policyAny)
}

func (s *PolicyService) resolvePolicyStack(ctx context.Context, stack []concrnt.Policy) (policy.PolicyStack, error) {
	ctx, span := tracer.Start(ctx, "Policy.Service.resolvePolicyStack")
	defer span.End()

	// CIP-12 §5.3: a virtual parent (the declaring resource's distribution
	// destinations) forms its own layer inserted immediately before the layer
	// that declares it — one layer per hierarchy level, never merged into an
	// ancestor's. Its Source is the declaring layer's parent path, so the
	// referenced policy's relative keys ('.', '*', './*') rewrite to a prefix
	// that matches the evaluated record key.
	resolved := make([]concrnt.Policy, 0, len(stack))
	for _, layer := range stack {
		if layer.VirtualParents != nil && len(*layer.VirtualParents) > 0 {
			split := strings.Split(layer.Source, "/")
			if len(split) == 0 {
				span.AddEvent("invalid policy source format", trace.WithAttributes(attribute.String("source", layer.Source)))
			} else {
				virtual := concrnt.Policy{Source: strings.Join(split[:len(split)-1], "/")}
				for _, parent := range *layer.VirtualParents {
					var doc concrnt.Document[any]
					// The virtual-parent policy record itself is not sensitive data
					// used for anything but building the evaluation stack, so strict
					// signature verification is unnecessary here. It is cached (10-min
					// resource TTL): every commit distributed to a timeline evaluates
					// this policy, so an uncached fetch here means one HTTP round trip
					// per distribution — bulk imports would otherwise hammer it (and,
					// via gateway remapping, the server itself). A timeline policy
					// change taking up to the cache TTL to apply is acceptable.
					err := s.client.GetRecord(ctx, parent, &client.Options{SkipVerify: true}, &doc)
					if err != nil {
						span.RecordError(err)
						virtual.Entries = append(virtual.Entries, concrnt.PolicyEntry{Errored: true})
						continue
					}

					if doc.Policy == nil {
						span.AddEvent("policy reference has no policies", trace.WithAttributes(attribute.String("ref", parent)))
						continue
					}

					virtual.Entries = append(virtual.Entries, doc.Policy.Entries...)
				}
				if len(virtual.Entries) > 0 {
					resolved = append(resolved, virtual)
				}
			}
		}
		resolved = append(resolved, layer)
	}

	result := policy.PolicyStack{}

	for _, layer := range resolved {
		policyLayer := []policy.EvaluationSet{}
		for _, p := range layer.Entries {

			if p.Errored {
				policyLayer = append(policyLayer, erroredEvaluationSet(p))
				continue
			}

			if p.URL != nil {
				pol, err := s.ResolvePolicyURL(ctx, *p.URL)
				if err != nil {
					span.RecordError(err)
					// mark this layer has errored policy
					policyLayer = append(policyLayer, erroredEvaluationSet(p))
					continue
				}

				for i := range pol.Statements {
					switch pol.Statements[i].Key {
					case ".": // this only
						pol.Statements[i].Key = layer.Source
					case "", "*": // this and all children
						pol.Statements[i].Key = layer.Source + "*"
					case "./*": // all children but not this
						pol.Statements[i].Key = layer.Source + "/*"
					}
				}

				// Entry-level defaults override the resolved policy's own,
				// so the referencing document decides the fallback conclusion.
				if defaults := entryDefaults(p); defaults != nil {
					if pol.Defaults == nil {
						pol.Defaults = defaults
					} else {
						for action, conclusion := range defaults {
							pol.Defaults[action] = conclusion
						}
					}
				}

				policyLayer = append(policyLayer, policy.EvaluationSet{
					Policy: pol,
					Params: p.Params,
				})
			}
		}
		result = append(result, policyLayer)
	}

	return result, nil
}

// entryDefaults converts a policy entry's per-action default conclusions
// (how the author wants evaluation to fall back when the referenced policy
// can't be resolved or doesn't decide) into evaluator form.
func entryDefaults(p concrnt.PolicyEntry) map[string]policy.Conclusion {
	if p.Defaults == nil {
		return nil
	}
	defaults := make(map[string]policy.Conclusion, len(*p.Defaults))
	for action, conclusion := range *p.Defaults {
		defaults[action] = policy.Conclusion(conclusion)
	}
	return defaults
}

// erroredEvaluationSet marks an unresolvable policy entry for evaluation:
// EvaluateStack folds it to the entry's declared default conclusion (or
// UNSET when the entry declares none), letting policy authors choose
// fail-open vs fail-closed per action instead of erroring out entirely.
func erroredEvaluationSet(p concrnt.PolicyEntry) policy.EvaluationSet {
	return policy.EvaluationSet{
		Errored: true,
		Policy:  policy.Policy{Defaults: entryDefaults(p)},
	}
}

func (s *PolicyService) Eval(ctx context.Context, req policy.RequestContext, stack []concrnt.Policy, action string, key string) error {
	ctx, span := tracer.Start(ctx, "Policy.Service.Eval")
	defer span.End()

	var policyStack policy.PolicyStack
	policyStack = append(policyStack, []policy.EvaluationSet{
		{
			Policy: s.globalPolicy,
		},
	})

	additionalStack, err := s.resolvePolicyStack(ctx, stack)
	if err != nil {
		span.RecordError(err)
		return err
	}

	policyStack = append(policyStack, additionalStack...)

	requestContext := policy.RequestContext{
		Requester: req.Requester,
		Self:      req.Self,
		Params:    req.Params,
		Globals:   s.globalParameters,
		Caller:    s,
	}

	conclusion, reason, error := policy.EvaluateStack(ctx, requestContext, policyStack, action, key)
	if error != nil {
		return error
	}

	switch conclusion {
	case policy.ALLOW, policy.OK:
		return nil
	default:
		return domain.PermissionError{Reason: "action denied by policy: " + reason}
	}
}

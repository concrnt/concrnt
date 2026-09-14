![wordmark](https://worldfile.cc/CC2d97694D850Df2089F48E639B4795dD95D2DCE2E/f696009d-f1f0-44f8-83fe-6387946f1b86)
### Concrnt: Makes social media accounts your internet identities.

[日本語](README-ja.md)

## What is Concrnt:
Concrnt is a distributed microblogging platform.

## Why Concrnt:
Using a social media account as a user identity, it's untenable to rely on centralized social media platforms that are prone to third-party censorship and irreversible account suspension. It's unacceptable for a carefully nurtured account to be suddenly frozen and rendered irrecoverable.

However, as long as servers physically exist, their operators must abide by the laws of the countries they are located in. Since operators are human, it's unrealistic to expect that accounts can never be frozen.

Concrnt resolves this dilemma. It uses a unique protocol designed to allow account migration. This means that even if your account is frozen on the server where it was initially created, you can move your account to another server and resume using it there, maintaining all past posts and friend connections just as before the freeze.

## How Concrnt solves the problem:
Concrnt uses public-key cryptography to verify user identities. Every piece of content you publish is a signed document, so anyone can verify that it really came from you — no matter which server happens to be storing it. It doesn't share any confidential information with servers, and users are responsible for managing the security of their own accounts.

## Highlights:
- **Account migration by design** — Your identity is your key pair, not a row in someone's database. Export your entire history of signed documents and replay it on a new server, and your account lives on with all posts and relationships intact.
- **Community timelines across servers** — Create any number of topic-based community timelines on each server. Unlike "local timelines" walled inside a single server, users from other servers can view and join them.
- **A small, composable protocol** — The protocol is defined as a set of small, independent specifications ([CIPs — Concrnt Improvement Proposals](https://github.com/concrnt/CIPs-translated)). It is deliberately kept simple enough for third parties to reimplement: a compliant server can be as minimal as a static file host paired with a small writer (even a serverless function).
- **Programmable permissions** — Access control is expressed with a policy engine, so communities and users can define fine-grained, customizable rules about who can read, write, or join, instead of being limited to a fixed set of visibility options.
- **Extensible via modules** — Additional features run as separate services that register themselves with the server at runtime, so a server can grow capabilities without forking the core.

## Cool! Where can I join?
You can experience the world of Concrnt through one of its web client implementations, available at [concrnt.world](https://concrnt.world)!

# Comparison

## Centralized (Twitter):
There's a fear of unjust suspension, and if an account is suspended, it signifies the death of that internet identity.

## Activitypub (Mastodon, Misskey):
While there is an account migration feature, if your account is suspended before migration, you're out of options. Additionally, major ActivityPub-supported social media platforms have a concept known as local timelines, which are not accessible or joinable by external users, ultimately forcing users to create new accounts on each server. Concrnt, however, allows for the creation of any number of topic-based community timelines on each server, which can be viewed and joined by users from other servers.

## nostr:
Nostr is a fantastic mechanism for proving one's identity using a private key, but it requires careful selection of relay servers. There is no guarantee that a relay server will retain or delete your data. (It works very well for use cases that do not require such assurances.) Due to its fundamentally decentralized nature, it seems unlikely to implement non-essential but convenient features (such as visibility control for non-encrypted messages).

## Bluesky:
Bluesky shares many similarities with Concrnt. Bluesky appears to be created with a mission similar to Twitter's, aimed at "making all the information in the world shareable." In contrast, Concrnt's mission focuses on "centering around communities and loosely connecting with the world." This difference in mission leads Bluesky to adopt an architecture that builds a massive index server and generates feeds, whereas Concrnt uses a single server to lightly and in real-time collect information from nearby sources.

## The problem of the Concrnt?:
The architectural strategy adopted by Concrnt may not scale to an ultra-large system where countless users can follow as many others as they like, similar to Twitter. This is because, rather than constructing a single, massive home timeline, it is designed with the assumption that users will create and switch between several moderately sized lists.

This approach is because Concrnt does not primarily aim to intensely connect with the world but to build relationships based on community timelines, optimizing for this purpose. This allows each domain to control information independently without the need for a massive server like an index server.

Furthermore, Concrnt embraces the concept of "protecting one's identity by oneself." However, in reality, this requires a certain level of knowledge and expertise, meaning there is an inherent complexity and difficulty in use.

In this sense, centralized social networks, where one relies on others for protection while trying to stay in the good graces of the administration to avoid being frozen, might be more reassuring for the average person.

# For geeks
## How to launch own server
look at detailed documentation: [concrnt square](https://square.concrnt.net/getting-started/hosting/)

To try a full stack locally (server + Postgres + Redis + memcached + web UI):

```sh
docker compose up
```

## Protocol specification
The protocol is specified as a collection of small documents called [CIPs (Concrnt Improvement Proposals)](https://github.com/concrnt/CIPs-translated).

## Contributing
When creating a PR, we generally recommend creating an issue first and reaching a consensus on whether or not to proceed. (Concrnt is currently being heavily developed, and there may be changes that cannot be made due to its policy.)

## Development

### Build / test

```sh
go build ./cmd/concrnt      # the server
go build ./cmd/conctl       # admin CLI (account creation, key generation, v1->v2 migration, repairs)
go build ./cmd/k8s-elector  # optional Kubernetes sidecar for leader election / peer discovery

go vet ./...
go test ./...                                    # repository-layer tests spin up Postgres and the Firestore emulator via dockertest (needs a Docker daemon)
go test ./policy/... ./chunkline/... ./client/... # pure-logic packages, no Docker required
go test ./... -run TestName -v                   # a single test
```

`go build` / `go vet` / `go test` are the tools of record; there is no Makefile or lint
config. `cmd/concrnt/Dockerfile` is the canonical build recipe used by CI
(`.github/workflows/docker-publish.yml`), which publishes multi-arch images on `v*.*.*`
tags and pushes to `main`.

Tags also run `.github/workflows/release.yml`, which uses GoReleaser (`.goreleaser.yaml`)
to attach linux amd64/arm64 tarballs (concrnt + conctl + k8s-elector) and `concrnt`
deb/rpm packages (server + CLI, systemd unit, `/etc/concrnt/config/config.yaml`) to the
GitHub Release. Releases created from the GitHub UI keep their hand-written notes; only
the assets are added. `goreleaser release --snapshot --clean` reproduces the artifacts
locally under `dist/`.

### Architecture

The server (`cmd/concrnt/main.go`) is a single Echo HTTP process wired up by hand in
`main()` (no DI framework). Layering, outside-in:

```
present/rest (Echo handlers) -> usecase (business logic, interfaces for deps) -> infra/* (Postgres, Redis, outbound HTTP)
                                       |
                                       v
                                    domain (entities, Config, sentinel errors)
```

- **`internal/domain`** — core types with no persistence/transport concerns: `Entity`,
  `Record`, `Config`, and sentinel errors (`NotFoundError`, `PermissionError`,
  `RedirectError`, `ValidationError`) matched with `errors.Is`.
- **`internal/usecase`** — one subpackage per bounded concern (`record`, `residence`,
  `server`, `notification`, `subscription`, `chunkline`, `abuse`), each exposing
  `Usecase`/`New` plus the repository/gateway interfaces only it needs; infra packages
  satisfy these interfaces structurally. The parent package holds only the ports shared
  across concerns (`JobQueue`, `KVS`). `record` is the hub: `residence` builds on it (its
  `Repository` embeds `record.EntityRepository`), and `record` depends on `server` (remote
  server resolution) and `chunkline` (removed-item advertisements). Inside `record`,
  `commit.go` is the write entry point (`Commit` dispatches on document kind) and each
  target object has its own file (`entity.go`, `record.go`, `association.go`, `ack.go`,
  `delete.go`); `read.go`/`query.go` are the read paths, `deliver.go` the federation
  delivery job, `commitlog.go` dump/import, and `usecase.go` the ports and constructor.
- **`internal/infra`** — adapters behind the usecase interfaces: `repository/postgres`
  (GORM) and `repository/firestore` (Cloud Firestore), selected by `repository.Open`
  from `backends.database`, with the shared contract suites in `repository/repotest`;
  `gateway` (outbound HTTP to other servers, e.g. chunkline federation), `pubsub`
  (Redis realtime pub/sub), `kvs` and `jobqueue` (Redis), `push` (Web Push), `cluster`
  (leader election / peer discovery via the elector HTTP protocol), `database` and
  `config` (connections and YAML loading).
- **`internal/present/rest`** — `Handler` registers the CCAPI routes; `wellknown.go`
  serves `/.well-known/concrnt`; `proxy.go` reverse-proxies to registered modules;
  `middleware/` holds auth (subkey and ecrecover verification); `presenter/` maps
  domain types to wire-format responses.
- **`internal/service`** — cross-cutting singletons: `ModuleManager` (polls configured
  `services` for a `/cc-info` endpoint map and merges it with the base REST endpoints —
  this is how modules add API routes without being compiled in) and `PolicyService`
  (wraps the `policy` evaluator with the global policy and parameters).
- **`internal/worker`** — background loops: the realtime subscriber (leader/worker split
  across replicas) and `NotificationReactor` (Web Push, enabled when VAPID keys are
  configured).
- **`policy/`** — the CIP-12 policy engine: policies are stacks of layers of statements
  (`action`, glob `key`, boolean `Condition`, `emit`), folded with `UNSET`/`ALLOW`/`DENY`
  combining logic.
- **`chunkline/`** — the timeline data structure: a `Manifest` describes chunked,
  paginated timeline storage, and `Client` merges multiple remote timelines into a single
  time-ordered stream via a heap, so timelines federate across domains without loading
  everything into memory.
- **`client/`** — outbound client for other concrnt servers (signed documents, CCKV/CCFS
  URI resolution, realtime websocket subscriptions).
- **`cdid/`** — CDID, the sortable base32 25-byte ID scheme used for records, timelines
  and chunks (`KindTime` embeds a timestamp, `KindHash` derives from content).
- **`impl/tags`**, **`impl/interop`** — the entity tag-string parser, and the types shared
  with modules that plug in via the `/cc-info` protocol.
- **Root package `concrnt`** — the wire-level protocol types shared across every layer:
  `SignedDocument`/`Document[T]`/`Proof` (the CIP-1 envelope and its ecrecover / subkey /
  document-reference proofs), `Policy`, `Event`, `CCURI` (`cckv://` and `ccfs://`), and
  secp256k1 signing/verification.

### Config

`internal/infra/config` loads YAML; see `config.example.yaml` for the full shape
(`concrnt` domain identity and registration mode, `backends` for the database
(`database: postgres | firestore`, see `deploy/firestore/README.md`) plus Redis/Memcached,
`observability` tracing, `integrations` for captcha/VAPID keys, `meta` for
instance branding served via well-known, `services` for pluggable modules).
`domain.Config` is the resulting in-process representation.

### Migration

`conctl migrate v1-to-v2` (backed by `legacy/`) exports a v1 identity's full log and
re-imports it into a v2 server through the commit-log import path.

---

> [!IMPORTANT]
> The specification has been significantly updated since v1.10.0. If you are running an older version of the server, please follow the [migration guide](https://square.concrnt.net/operator/migration/) to migrate.

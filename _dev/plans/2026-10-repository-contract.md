# リポジトリ層の契約化とマルチバックエンド化 — 作業計画書

対象リポジトリ: `github.com/concrnt/concrnt`（ベースブランチ `develop`）
作成日: 2026-10-28
読者: この計画を実行する作業エージェント / レビューする人間

---

## 0. この文書の使い方

- 本書は **フェーズ1**（`support-firestore` のバックエンド中立部分の取り込み）、**フェーズ2**（契約の正式化と Postgres の書き直し、SQLite 追加）、**フェーズ3**（固まった契約に対する Firestore バックエンドの移植）の3段構成。前のフェーズが `develop` にマージされるまで次に着手しない。**フェーズ1・2には Firestore の実装コードは一切含めない**。契約がフェーズ2で変わるため、先に移植すると二度手間になる。
- 各タスクには「成果物」「受け入れ条件」「検証コマンド」がある。受け入れ条件を満たさない状態で次のタスクに進まない。
- **判断が必要な箇所は §8「未決事項」に集約**してある。それ以外は本書の決定に従い、作業者の判断で設計を変えない。§8 の項目はいずれも推奨案を書いてあるので、ユーザーから別の指示がなければ推奨案で進める。
- 作業者が本書と現コードの矛盾を見つけたら、コードを正とし、本書の該当箇所を修正して PR に含める。

### 前提環境

- Go `1.25.1`（`go.mod` の `go` ディレクティブ）。
- リポジトリ層のテストは `github.com/ory/dockertest` で Postgres コンテナを起動する（フェーズ3以降は Firestore エミュレータも）。**Docker デーモンが必要**。Docker なしで動くのは `./policy/... ./chunkline/... ./client/...` などの純ロジックパッケージのみ。
- 現在 CI（`.github/workflows/docker-publish.yml`, `release.yml`）は **テストを実行していない**。`go build` / `go vet` / `go test` を手元で回すこと。フェーズ1でテスト用ワークフローを追加する（T1.7）。
- 既存の生成物に触らない: `schemas/*.go` は生成コード。
- コミットは小さく。`develop` への PR は本書のタスク単位（T1.1, T1.2 …）を目安に分ける。force-push しない。

### 用語

- **契約（contract）**: `internal/usecase/*` が宣言する `Repository` インターフェース群と、それに付随する不変条件（§2）。バックエンドはすべてこの契約だけを実装する。
- **バックエンド**: 契約の実装。現状 `internal/infra/repository/postgres`。`origin/support-firestore` に `firestore` 実装があるが、取り込むのはフェーズ3。
- **repotest**: バックエンド非依存の準拠テストスイート（`internal/infra/repository/repotest`、`support-firestore` ブランチにある）。

---

## 1. 背景と目的

### 現状

- `develop` のリポジトリ層は GORM + Postgres 専用。`ON CONFLICT … DO UPDATE … WHERE`、`SELECT … FOR UPDATE`、セッションレベル `pg_advisory_lock`、部分インデックス、`bigserial` + `ON DELETE CASCADE`、起動時 `AutoMigrate` に依存している（`internal/infra/database/postgres.go`, `internal/infra/database/models/*.go`, `internal/infra/repository/postgres/record.go`）。
- リクエスト1回で逐次 3〜5 往復の DB アクセスが発生する（`GetRecordKeyByURI` → メンバー取得 → `Preload("Record")` → `Preload("Record.Document")` など）。
- リアクション数・フォロワー数は読み取り時に `GROUP BY` で集計する（`record.go` の `GetAssociatedRecordCountsBySchema` / `ByVariant` / `GetAcknowledgeRecordCounts`）。
- `origin/support-firestore`（2026-09-10〜14、3コミット、`develop` から 22 コミット遅れ）に Firestore バックエンドの完全な実装がある。ビルドは通る（ベース時点）。ここに含まれる **バックエンド中立の改善**（`RunInTx`、`repository.Set`/`Open`、`MaintenanceRepository`、`repotest`、`Inspector`）がフェーズ1の取り込み対象。**Firestore 実装そのもの（`internal/infra/repository/firestore/*` など）はフェーズ1・2では取り込まない**。

### 目的

1. **移植性**: 契約を「KV + 順序付き範囲走査 + 楽観的トランザクション + 事前集計」の形に固定し、Postgres / Firestore / 将来の Spanner・CockroachDB・SQLite が同じ契約と同じ準拠テストで動くようにする。
2. **急上昇耐性と保守性**: 往復回数の削減と集計の事前計算により、どのバックエンドでも読み取り増幅を抑える。これは Postgres 単体でも効く。
3. **データ移行の統一**: バックエンド間の移行は常に「コミットログのダンプ → 再生」。テーブルコピーは作らない。

### 非目的（本計画の範囲外）

- ariake 本番の DB 切り替え。
- `activitypub` / `atproto` ブリッジ（別リポジトリ）の対応。
- Redis の役割分離（ジョブキュー / pubsub / dedup / カウンタの抽象化）。別計画。
- バッチ API（`handle-batch-request` ブランチ）。別計画。
- Spanner / CockroachDB バックエンドの実装。契約が整った後（フェーズ3の Firestore と同じ手順で追加する）。

---

## 2. 契約の原則（決定事項）

以下は **契約と usecase 層に課す不変条件**。バックエンド実装の内部で何を使うかは自由（Postgres が内部で FK CASCADE を使うのは可）。ただし契約の意味論がそれに依存してはならない。

| # | 原則 | 意味 | 根拠 |
|---|---|---|---|
| C1 | **JOIN を前提にしない** | 契約の読み取りメソッドは「1コレクションの等価フィルタ + 高々1つの範囲条件 + 並び順」または「ID による単発/複数取得」で満たせる形にする。必要なフィールドはキー側に非正規化する。 | Firestore / Dynamo / Spanner で同一形状が使える。Postgres では往復削減になる。 |
| C2 | **集計はキャッシュ** | 件数などの集計値は、一次データから常に再計算できる **派生キャッシュ** として扱う。リクエスト経路では集計層（`AggregateStore`）を読み、ミス時だけ一次データから計算して格納する。書き込み時の加算は「エントリが既にある場合の最適化」に過ぎず、正しさの根拠にしない。集計ロジックの変更時は全エントリを手動で削除すればよく、保存先の移転やデータ消失もログ再生を要さない。 | 読み取り増幅の排除と、集計の「使い捨て可能性」の両立。 |
| C3 | **自動採番に依存しない** | 契約上の識別子は CDID、URI のハッシュ、クライアント生成のランダム ID（gen）のみ。`bigserial` の値が契約に漏れてはならない。 | 分散 DB のホットスポット回避、バックエンド間移行。 |
| C4 | **トランザクションは `RunInTx(ctx, fn)`** | `fn` は再実行されうる（競合リトライ）。`fn` 内で外部副作用を起こさない。1トランザクションの書き込み数は上限（Firestore 500）を超えない設計にし、大量削除はジョブキューへ。 | Firestore / Spanner / CRDB の楽観的・直列化可能トランザクションに合わせる。 |
| C5 | **クエリ形状はレジストリで宣言** | バックエンドが発行するクエリ形状は `shapes` に列挙し、各バックエンドはその形状を満たすインデックスを持つことをテストで検証する。 | インデックス定義の機械化、ドリフト検出。 |
| C6 | **一意制約は明示** | 契約上の一意性（association の `unique`、ack の `(from,to,schema)`）は、バックエンドが一意インデックスで実装してもよいが、契約の意味論は「同一キーの2件目は no-op / 拒否」として repotest で検証する。 | |
| C7 | **準拠テストが契約の定義** | `repotest` を通らないバックエンドは契約を満たしていない。契約を変えるときは repotest を先に変える。 | |
| C8 | **移行はログ再生** | `conctl op dump-commitlog` / `import-commitlog` が全バックエンドで動くこと。 | |
| C9 | **リポジトリの分離境界 = 実体を別にできる境界** | 2つのリポジトリを別のインターフェースにしてよいのは、「一方を Postgres、他方を Firestore に載せても意味が成立する」場合だけ。同じテーブル／コレクションを前提に動くものは **1つのストア** にまとめる。現状の実体は `Record`（record + residence の entity 参照 + chunkline + 旧 maintenance。`commit_logs` / `records` / `record_keys` / `entities` / acks / associations を共有）、`EntityMeta`（`entity_metas`。ccid キーのみで JOIN されず、登録状態の読み書きだけなので別実体に載せられる）、`Server`（`servers`）、`Notification`（`subscriptions`）、`Abuse`（`abuse_reports`）の5つ。PR #114 で確定。usecase 向けの **狭いインターフェース**（`chunkline.Repository` など）は残してよいが、`repository.Set` と `repotest.Backend` はストア単位で束ね、同じストアの各インターフェースは同じオブジェクトが実装する。 | 「record は Postgres、maintenance は Firestore」のような無意味な構成を型で排除する。 |
| C10 | **テスト専用コードは本番パッケージに置かない** | テストからしか使わないコードは `_test.go` に閉じ込める。パッケージ間で共有が必要な場合だけ、名前が `test` で終わるパッケージ（`repotest`, `testutil`, `storetest`）に置く。これらのパッケージは非テストファイルから import しない（`go list -deps ./cmd/...` に `repotest` / `testutil` / `ory/dockertest` が現れないことをテストで検査。testify 自体は `cosmos-sdk` → `cometbft/types` 経由で既に本番バイナリにリンクされているので検査対象にしない）。インターフェース型は `_test.go` に置けないので `repotest` に置き、各バックエンドの実装は自パッケージの `inspect_test.go` に置く。 | 本番バイナリに testify 等のテスト依存をリンクしない。用途が置き場所と名前から読める。 |

---

## 3. フェーズ1 — `support-firestore` のバックエンド中立部分を `develop` に取り込む（Postgres のみ）

### 方針

- **ブランチをそのままマージしない。** `develop` が 22 コミット進んでおり、両側で `cmd/concrnt/main.go`, `cmd/conctl/gc-commitlog.go`, `internal/infra/repository/postgres/record.go`, `internal/usecase/record/commit.go`, `internal/usecase/record/usecase.go`, `internal/infra/config/config.go`, `go.mod` が変更されている。`origin/support-firestore` を **参照実装**として、`develop` 起点の新ブランチに **スライス単位で移植**する。
- 作業ブランチ: `feat/repository-contract-phase1`（PR は下記タスク単位で切ってよい。その場合はスタックせず、順番にマージ）。
- 参照コマンド: `git show origin/support-firestore:<path>`、`git diff $(git merge-base origin/support-firestore origin/develop) origin/support-firestore -- <path>`。

### T1.1 `RunInTx` への移行（契約変更、Postgres のみ）

成果物
- `internal/usecase/record/usecase.go`: `BeginTx` を削除し `RunInTx(ctx, fn func(tx RepositoryTx) error) error` を追加。`RepositoryTx` は `IsRepositoryTx()` マーカーのみ（`Commit`/`Rollback` を削除）。ドキュメントコメントは `support-firestore` のものを採用。
- `internal/usecase/record/commit.go`: `support-firestore` の差分どおり `applyCommit` を `RunInTx` クロージャ内で実行し、no-op 時は `errCommitNoop` でロールバック。**`develop` 側の後続変更（`update entity updation`, `update remote entity updation`, `fix resolve aliased ccuri` 等）を失わないこと**。3-way で慎重に当てる。
- `internal/usecase/record/*.go` の他の `BeginTx` 利用箇所（delete, ack, association, entity, replication など）を同様に書き換える。`grep -rn "BeginTx\|\.Commit(ctx)\|\.Rollback(ctx)" internal/usecase` がゼロになること。
- `internal/infra/repository/postgres/record.go`: `RunInTx` 実装。`db.Transaction` ベースで、`SQLSTATE 40001`（serialization_failure）/ `40P01`（deadlock_detected）のときクロージャを再実行（上限 8 回、指数バックオフ）。それ以外のエラーはそのまま返す。
- usecase の単体テストのスタブ（`internal/usecase/record/*_test.go` の fake repository）を新契約に合わせる。

受け入れ条件
- `go build ./... && go vet ./...` 成功。
- `go test ./internal/usecase/... ./internal/infra/repository/...` 成功（Docker 必要）。
- 挙動の差分なし: 既存の `commit_test.go` の期待値は変えない（`support-firestore` の `commit_test.go` 差分は `RunInTx` への機械的追従のみであることを確認して採用）。

### T1.2 `repository.Set` / `repository.Open` と配線

成果物
- `internal/infra/repository/repository.go`（`support-firestore` から移植）。`Set` はストア単位（C9）: `Core`（`record.Repository` / `residence.Repository` / `chunkline.Repository` を同じオブジェクトが実装する。フィールドは `Record`, `Residence`, `Chunkline` のまま置いてよいが、同じ実体を指すことをコメントと生成コードで保証する）、`Server`, `Notification`, `Abuse`、および `Name, Collectors, Ready, Close, SQL`。`Open` は `conf.Database` が `""`/`"postgres"` のとき Postgres を開き、それ以外は「未知のバックエンド」エラーを返す。`switch` を用意しておくだけで、他のケースは書かない。
- `internal/infra/config/config.go`: `Backends.Database string` を追加（既定 `postgres`）。Firestore 固有の設定はフェーズ3。既存キーは不変。`config.example.yaml` にコメント付きで追記。
- `cmd/concrnt/main.go`: `support-firestore` の差分どおり `repository.Open` 経由に。`client.New` の生成順が前に移動することに注意（Deps に渡すため）。`/ready` は `repos.Ready`、メトリクスは `repos.Collectors`。
- `cmd/conctl/operation.go`: `repository.Open` を使い、SQL が必要な既存コマンド（`repair-*`, `migrate-*`, `local-entity`, `delete-user`, `create-account`）は `Set.SQL` を使う。`Set.SQL == nil` のときは「このコマンドは Postgres 専用」と明示的にエラーを返す。

受け入れ条件
- `go build ./...` 成功。`docker compose up`（`compose.yaml`）で起動し、`/health` `/ready` が 200。
- `conctl` の各サブコマンドが `--help` と、Postgres 接続下での既存動作を維持（手動確認でよい。確認したコマンドを PR 本文に列挙）。

### T1.3 保守用メソッドの `record.Repository` への統合と `Inspector`

成果物
- `internal/usecase/record/usecase.go`: `CommitLogEntry`, `CommitLogPage` と、保守用メソッド `CountGcCandidates`, `ListGcCandidates(ctx, cutoffID, afterID, limit)`, `DeleteCommitLogs`, `ListCommitLogs` を **`record.Repository` に直接追加**する（C9: 同じ `commit_logs` を前提にするので別インターフェースにしない）。`support-firestore` にある別立ての `MaintenanceRepository` は採用しない。usecase の fake は `record.Repository` を埋め込んで未使用メソッドの実装を省く。
- `internal/infra/repository/postgres/record_maintenance.go`: 上記メソッドの Postgres 実装（ファイルは分けてよいが、レシーバは `RecordRepository`）。
- `internal/infra/repository/repotest/inspect.go`: `Inspector` インターフェースと `CommitState` 等の状態構造体（C10: 共有が必要な型なので `repotest` に置く）。
- `internal/infra/repository/postgres/inspect_test.go`: Postgres の `Inspector` 実装。**`_test.go` に置く**。本番の `postgres` パッケージには inspector のコードもテスト依存も入らない。
- `cmd/conctl/gc-commitlog.go`, `dump-commitlog.go`: `op.Repos.Record` の保守用メソッド経由に書き換え。**`develop` 側の `add gc backup`（a8a7c68）の挙動を保持**（`CommitLogEntry` に `IP` / `CDate` を含める）。

受け入れ条件
- `go test ./cmd/conctl/...` 成功。
- `go list -deps ./cmd/concrnt ./cmd/conctl | grep -E "repotest|testutil|ory/dockertest"` が空（testify は cometbft 経由で既存のため対象外）。
- `conctl op gc-commitlog --dry-run` 相当の出力が移植前後で一致（件数ベースで手動確認）。

### T1.4 `repotest` 準拠スイートの導入（Postgres で先に通す）

成果物
- `internal/infra/repository/repotest/{repotest,record,ack,chunkline,residence}.go`（`support-firestore` から）。`RunRecordSuite`, `RunAckSuite`, `RunChunklineSuite`, `RunResidenceSuite`。
- `internal/infra/repository/postgres/suite_test.go`（同）。`repotest.Backend` は `Core`（record / residence / chunkline を兼ねる1オブジェクト）、`Inspect` の形にし、`Maintenance` フィールドは持たない（C9）。
- `develop` で増えた契約メソッドのテストを repotest に追加: `QueryByParentOrderByKey`（キー順、プレースホルダ行、`since/until` 包含）、`QueryCommitLogs`（`c_date,id` 順、owner フィルタ、ページング）。期待値は現行 Postgres の挙動を正とする。
- 既存の `postgres/*_test.go` のうち repotest と重複するものは削除してよいが、Postgres 固有（インデックス存在検査 `postgres_test.go` など）は残す。

受け入れ条件
- `go test ./internal/infra/repository/postgres/ -run 'Suite' -count=1` 成功。
- repotest は `postgres` パッケージを import しない（バックエンド非依存であること）。`go list -deps ./internal/infra/repository/repotest | grep repository/postgres` が空。

### T1.5 CI にテストを追加

成果物
- `.github/workflows/test.yml`: `push`（develop）と `pull_request` で `go vet ./...` と `go test ./... -count=1` を実行。GitHub ホステッドランナーは Docker が使えるので dockertest はそのまま動く。`timeout-minutes: 20`。Go モジュールキャッシュを有効化。
- `README.md` のテスト節に CI の存在を追記。

受け入れ条件
- PR 上でワークフローが緑。

### T1.6 ドキュメント

成果物
- `README.md`: バックエンド選択（`backends.database`、現時点では `postgres` のみ）、テストの回し方。
- `internal/infra/repository/CONTRACT.md`: §2 の原則を転記し、「契約を変えるときの手順（repotest → 各バックエンド → shapes）」を書く。フェーズ2で肉付けする。

### PR #114 への追加指示（2026-10-28）

実装中に出た判断の記録。計画本文は上記のとおり更新済み。

- `inspect` パッケージを廃し、`Inspector` インターフェースは `repotest` に、Postgres 実装は `postgres/inspect_test.go` に移す（C10）。
- `record.MaintenanceRepository` を廃し、メソッドを `record.Repository` に統合する（C9）。`repository.Set.RecordMaintenance` と `repotest.Backend.Maintenance` も消す。
- `CONTRACT.md` の C2 を本書 §2 の最新文言に合わせる（書き込み時にエントリが無ければ再計算）。C9 / C10 を追記する。
- `.github/workflows/test.yml` の push トリガーに `feat/repository-contract` を追加する。
- `import-commitlog` の再試行パスで、拒否された行のアドレスに当たる delete が先行パスで受理されると削除済みレコードが復活しうる。フェーズ2の往復テスト（§4 完了条件）までに、delete を同じ再試行パスへ持ち越すか、`--help` に制約として明記するかを決める。

### フェーズ1 完了条件

- 上記すべてが `develop` にマージ済み。
- `develop` で `go test ./...` が CI で緑。
- `backends.database: postgres`（既定）で起動し、repotest 4 スイートを Postgres が通す。
- リポジトリに Firestore 実装コードが含まれていない（`ls internal/infra/repository/firestore` が存在しない）。
- `internal/infra/repository/inspect` パッケージが存在せず、`postgres` の本番ファイルに inspector がない（C10）。
- `record.MaintenanceRepository` 型が存在しない（C9）。

---

## 4. フェーズ2 — 契約の正式化と Postgres の書き直し

### 方針

- 契約の変更は **repotest → Postgres →（T2.5 以降は）SQLite** の順。全バックエンドが通るまで `develop` にマージしない。`support-firestore` のコードは **設計の参照** としてのみ読む（`git show origin/support-firestore:<path>`）。
- Postgres の内部スキーマは変えてよいが、**既存データの移行経路を必ず用意する**（起動時マイグレーション、または `conctl op` の backfill）。ariake のデータは失わない。
- 各タスクは独立した PR にできる粒度にしてある。順序は T2.1 → T2.2 → T2.3 → T2.4 → T2.5 → T2.6 を推奨（T2.5, T2.6 は並行可）。

### T2.1 `CONTRACT.md` の完成と契約の棚卸し

成果物
- `internal/infra/repository/CONTRACT.md` に、現行 `record.Repository` の全メソッドを列挙し、各メソッドを §2 の原則で分類した表を書く:
  - そのまま維持
  - シグネチャ変更（理由）
  - 廃止 / 置換（理由と置換先）
- 分類結果は以下を出発点にする（作業者は検証して修正する）:
  - `GetAssociatedRecordCountsBySchema`, `GetAssociatedRecordCountsByVariant`, `GetAcknowledgeRecordCounts` → **C2 違反**。T2.2 で `Compute*`（集計層専用の高コスト計算）に改名し、リクエスト経路は `aggregate` 層経由に。
  - `QueryByPrefix`（`uri LIKE prefix%`）→ `support-firestore` の実装は `ancestors` 配列 `array-contains` で代替している（参照のみ）。契約の意味を「指定キーの子孫のうち記録を持つもの」と定義し、Postgres も `ancestors` 相当（親チェーン）を持つか、`LIKE` を内部実装として残すかは **バックエンド内部の自由**。契約文書には意味論だけを書く。
  - `QueryRecordSubtree`、`GetHierarchicalRecordPolicies` → 同上。
  - `GetSignedDocument` → T2.4 で複数取得版を追加。
  - `BeginTx` 系 → フェーズ1で完了。

受け入れ条件
- 表に全メソッドが載っており、各行に判断理由がある。レビュー承認。

### T2.2 集計キャッシュ層 `AggregateStore`（C2）

目的: 集計値を **一次データから再計算可能な派生キャッシュ** として扱い、(a) 読み取り時も書き込み時も、エントリが無ければその場で一次データから計算、(b) ロジック変更は全エントリの手動削除だけ、(c) 保存先の差し替えが可能、(d) 書き込み時の加算は「エントリがある場合の近道」に留める、を満たす。

#### 構成

```
usecase/aggregate          集計定義 + read-through ロジック（バックエンド非依存）
  ├─ AggregateStore        キャッシュの保存先インターフェース（差し替え可能）
  │     postgres (table) / redis / memcached / inmemory   （firestore はフェーズ3）
  └─ record.Repository     一次データからの高コスト計算（Compute* メソッド群）
```

- `internal/usecase/aggregate/` を新設。`Definition` は `{Name, Compute(ctx, target) (Value, error), Apply(Value, Delta) Value, TTL}` を持つ。
- **キー** = `Name + ":" + targetKey`。バージョンは持たない。集計ロジックを変えたときは `conctl op aggregate-invalidate --all`（または `--def name`）で全エントリを削除し、以後の読み書きで再生成させる。
- `AggregateStore` インターフェース（最小）:
  ```go
  type Entry struct { Value []byte; AsOf time.Time; Deltas int }
  type Store interface {
      Get(ctx, key) (*Entry, bool, error)
      // PutIfAbsent は既存エントリを上書きしない（遅延生成の勝者決定）
      PutIfAbsent(ctx, key, Entry, ttl) (won bool, err error)
      // Apply は存在するエントリにだけ差分を適用し、無ければ false を返す
      Apply(ctx, key, delta []byte, applied func(old []byte) []byte) (bool, error)
      Invalidate(ctx, key) error
      // InvalidateAll は name プレフィックス（"" なら全部）に一致するエントリを削除する。ロジック変更時の手動全削除に使う。
      InvalidateAll(ctx, namePrefix string) error
  }
  ```
  実装は `internal/infra/aggregate/{postgres,redis,memcached,inmemory}`（Firestore 実装はフェーズ3）。**Redis / memcached 実装は揮発でよい**（消えたら再計算）。
- `record.Repository` に **高コスト計算メソッド** を追加し、既存のカウント系メソッドはリポジトリから **削除** する:
  - `ComputeAssociationCountsBySchema(ctx, targetURI) (map[string]int64, error)`
  - `ComputeAssociationCountsByVariant(ctx, targetURI, schema) (*utils.OrderedKVMap[int64], error)`（既存の順序規則を維持: 最初の association の `createdAt` 順）
  - `ComputeAcknowledgeCounts(ctx, from, to, schema) (map[string]int64, error)`
  これらは Postgres では `GROUP BY` で実装してよい（将来の Firestore では走査。**集計層と検証ツール以外から呼ばない**。`CONTRACT.md` に明記し、ハンドラ層からの直接呼び出しを `go vet` 相当の簡易チェック（`grep` ベースのテスト）で禁止する）。
- ハンドラ（`internal/present/rest/handler.go` の 866, 872, 1074 行付近）は `aggregate.Usecase.Get(ctx, def, target)` を呼ぶ。

#### read-through の手順

1. `Store.Get(key)` がヒット → 返す。
2. ミス → `def.Compute(ctx, target)` を実行（高コスト許容）。`AsOf = 計算開始時刻`。
3. `Store.PutIfAbsent(key, entry, ttl)`。負けた場合は相手の値を `Get` し直して返す。

#### 書き込み時の処理（エントリがあれば差分適用、無ければ再計算）

- `CreateAssociation` / `DeleteAssociation` / `Acknowledge` 系の **usecase** 側で、`RunInTx` が成功した **後**（postProcess）に `aggregate.Usecase.Touch(ctx, def, target, delta, itemReceivedAt)` を呼ぶ。トランザクション内では呼ばない（C4: クロージャ内に外部副作用を置かない。集計層は別ストアになりうる）。
- `Touch` の手順:
  1. `Store.Apply(key, delta)` を試す。エントリがあれば適用して終了。
  2. 無ければ **その場で `def.Compute(ctx, target)` を実行**する（高コスト許容）。自分の書き込みは既にコミット済みなので、計算結果には自分の項目が含まれる。`AsOf = 計算開始時刻`。
  3. `Store.PutIfAbsent(key, entry, ttl)`。勝てば終了。
  4. 負けた場合（並行する別の書き込みか読み取りが先に生成した）: 相手の `AsOf` が自分の項目の受信時刻 `itemReceivedAt` より前なら、相手の計算が自分の項目を見ていない可能性があるので `Apply(delta)` する。そうでなければ何もしない。
- 読み取りと書き込みのどちらが先にエントリを作っても結果は同じ。読み取りの read-through（上記）と `Touch` は同じ `computeAndPut` を共有する。
- `Apply` は差分の数 `Deltas` を数え、`def.MaxDeltas` を超えたら `Invalidate` して次回の読み書きで再計算させる。
- **複雑な操作は差分を作らず `Invalidate`**: `DeleteRecordByKey` のカスケード、GC、`import-commitlog`、`conctl` の修復コマンド。無効化後、次の読み書きが再計算する。
- コールドキーへの書き込み集中（例: 新規投稿への最初のリアクション群）では複数の書き込みが同時に `Compute` を走らせ、1 つだけが `PutIfAbsent` に勝つ。計算は 1 ターゲット単位で有界なので許容する。問題になれば `Store.SetNX` 相当の短命ロック（「計算中」マーカー、数秒）で 1 回に絞る拡張を入れる（任意）。
- 厳密性の扱い:
  - Postgres 実装: `Apply` と `PutIfAbsent` は同一トランザクションで `pg_advisory_xact_lock(hash(key))` を取るので、同一キーに対して直列化される。
  - Redis / memcached（将来の Firestore も）実装: 手順 4 の `AsOf` 比較はサーバ時刻に依存するため、レプリカ間のクロック差程度の競合窓が残る。**これは TTL による再計算で収束する結果整合として許容する**。集計値はキャッシュであり正本ではない、が設計の前提。
- `AsOf` は「この値はいつ時点の計算か」を保持し、`Apply` 時は更新しない（計算時点を表すため）。API レスポンスには出さない。

#### 運用ツール

- `conctl op aggregate-verify --sample N [--def name]`: ランダムに N ターゲットを選び、キャッシュ値と `Compute` の結果を比較して差分を報告。差分があったキーは `Invalidate`。
- `conctl op aggregate-invalidate (--all | --def name [--target uri])`: 明示無効化。**集計ロジックを変更したリリースでは `--all` を手動で実行する**。手順を `MIGRATION.md` の運用節に書く。
- **`rebuild-counters` のような全件再計算コマンドは作らない**。削除すれば次の読み書きが再計算する。

#### 設定

```yaml
backends:
  aggregate:
    store: postgres | redis | memcached | inmemory   # 既定: database と同じバックエンド（firestore はフェーズ3で追加）
    ttl: 24h
    maxDeltas: 10000
```

受け入れ条件
- `internal/usecase/aggregate` の単体テストが `inmemory` ストアで Docker なしに通り、以下を検証する: 読み取りミス → Compute → Put、ヒット時に Compute が呼ばれない、書き込み時にエントリが無ければ Compute → Put（結果に自分の項目が含まれる）、エントリがあれば Apply のみで Compute が呼ばれない、`PutIfAbsent` で負けたとき `AsOf` と受信時刻の比較で Apply の有無が決まる、`MaxDeltas` 超過で `Invalidate`、`InvalidateAll` 後の読み書きで再計算。
- `AggregateStore` の **準拠テスト**（`internal/infra/aggregate/storetest`）を 4 実装すべてが通す。
- repotest に `Compute*` のケースを追加し、既存のカウント系メソッドの期待値（`ByVariant` の順序を含む）をそのまま引き継ぐ。
- 既存 DB の集計データは **移行しない**（初回読み取りで生成される）。PR 本文にその旨と、ariake 規模での初回生成コストの見積りを書く。
- `internal/present/rest` から `record.Repository` のカウント系メソッド呼び出しが消えている。
- `aggregate-verify --sample 1000` を Postgres ストアと Redis ストアで実行し、Postgres は差分ゼロ、Redis は差分が TTL 内に収束することを確認（手順と結果を PR に添付）。

### T2.3 クエリ形状レジストリをバックエンド中立化し、インデックス検証を各バックエンドに（C5）

設計
- `support-firestore` の `internal/infra/repository/firestore/shapes.go` にある `queryShape` 定義を手本に、`internal/infra/repository/shapes/shapes.go` を新設し、**論理フィールド名**（`parent`, `recordCreatedAt`, `schema` …）で宣言する。各バックエンドは論理名 → 物理列/フィールド名のマップを持つ。
- Postgres: 各形状に対し「等価列 + 範囲列（並び順）」を前置一致で含むインデックスが `pg_indexes` に存在することをテストで検証する（`postgres_test.go` にある手書き検査を一般化）。GORM タグのインデックス定義は **形状から導出できるものに限り削除し、`shapes` から DDL を生成**する（`postgres/indexes.go`: 起動時に `CREATE INDEX IF NOT EXISTS` を発行）。形状に表れない内部インデックス（PK、unique）はモデルタグに残す。
- マイグレーションの直列化: セッションレベル `pg_advisory_lock` を廃し、トランザクション内 `pg_advisory_xact_lock` に変更（CockroachDB 互換、§2 の将来対応）。

受け入れ条件
- `go test ./internal/infra/repository/shapes/...` が Docker なしで通る。
- 形状を1つ追加してインデックスを足さないと、Postgres（と SQLite）のテストが失敗する。
- 既存 DB に対して起動し、`pg_indexes` の集合が移行前と同じかスーパーセット（削除は意図したもののみ。PR に差分一覧を添える）。

### T2.4 往復削減（C1）

設計
- 契約に複数取得を追加: `GetSignedDocuments(ctx, uris []string) (map[string]*concrnt.SignedDocument, error)`（見つからないものは欠落、エラーにしない）。`GetDistributions`, `GetHierarchicalRecordPolicies` のうち連鎖的に呼ばれているものを usecase 側で束ねる。
- chunkline: `LoadLocalBody` が `Record` / `Document` を Preload している箇所を、`record_keys` への非正規化（`redirect`, `recordCreatedAt`）で置換。設計は `support-firestore` の `firestore/chunkline.go` を参照。Postgres は `record_keys` に `redirect text` 列を追加し、`CreateRecord` 時に書く。既存行は起動時マイグレーションで `records` から backfill（1回、冪等）。
- `GetRecordKeyByURI` → 本体クエリの 2 往復を 1 往復にできる箇所（`parent_id` を URI から直接引けないため Postgres では難しい）は無理に潰さない。**目標は「1リクエスト = 高々 2 往復 + 1 回の複数取得」**。
- 計測: OpenTelemetry のスパン数で検証する。`internal/present/rest` のハンドラテスト（既存の httptest）に、代表リクエスト（document 取得、chunkline body、associations 一覧）あたりのリポジトリ呼び出し回数を数えるテストを追加（fake repository でカウント）。

受け入れ条件
- 上記カウントテストが、現行値を上限として固定され、改修後は目標値以下。PR 本文に before/after を表で記載。
- 両バックエンドで repotest 緑。

### T2.5 SQLite バックエンド（移植性の強制力、Docker 不要のテスト）

設計
- `internal/infra/repository/sqlite/`。**GORM を使わず** `database/sql` + `modernc.org/sqlite`（pure Go、cgo 不要）。これを「GORM に依存しない新規バックエンドの書き方」の参照実装とする。
- 単一ライタ前提（`_journal_mode=WAL`, `busy_timeout`）。`RunInTx` は `BEGIN IMMEDIATE` で直列化し、`SQLITE_BUSY` でリトライ。
- `backends.database: sqlite`, `backends.sqlite.path`。`:memory:` を許可（テスト用）。
- `suite_test.go` は repotest 4 スイートを実行。Docker 不要。

受け入れ条件
- `go test ./internal/infra/repository/sqlite/ -count=1` が Docker なしで緑。
- `docker compose` なしで `concrnt` を単体起動できる手順を README に追記（Redis / memcached は引き続き必要。その抽象化は範囲外と明記）。

### T2.6 Postgres 実装の整理

設計
- `internal/infra/repository/postgres` から契約外のものを除く: `BeginTx` 残骸、使われない Preload。`GROUP BY` は `Compute*` の内部実装としてのみ残る（T2.2）。
- GORM の扱いは **維持**（§8-D）。ただし `AutoMigrate` の対象からインデックス生成を外し（T2.3）、スキーマ変更は `postgres/migrations.go` に明示的な手順として積む（番号付き、冪等、`schema_migrations` テーブルで管理）。
- `internal/infra/database/models/*.go` の各 struct コメント（インデックス一覧）を shapes への参照に置換。

受け入れ条件
- `go vet`, repotest 緑。`conctl op` の Postgres 専用コマンドが引き続き動作。

### フェーズ2 完了条件

- `CONTRACT.md` の表に「違反」行が残っていない。
- Postgres / SQLite の 2 バックエンドが同じ repotest を通す。
- カウント系 API が全バックエンドでキャッシュヒット時 O(1) 読み取り。ミス時の `Compute*` は集計層からしか呼ばれない。
- `AggregateStore` の保存先を設定だけで切り替えられ（例: postgres → redis）、切り替え後や `aggregate-invalidate --all` 後に、ログ再生なしで読み書き双方から値が再生成される。
- 代表 3 リクエストのリポジトリ呼び出し回数が目標以下。
- `dump-commitlog` → `import-commitlog` で Postgres → SQLite → Postgres の往復をし、`dump-commitlog` の出力が一致する（§2 C8 の実証）。手順を `MIGRATION.md` に追記。

---

## 5. フェーズ3 — 固まった契約に対する Firestore バックエンドの移植

フェーズ2完了後に着手。`origin/support-firestore` の `internal/infra/repository/firestore/*` を出発点にするが、契約はフェーズ2で変わっているので **コピーではなく書き直し** になる箇所が多い。独立した計画書を起こしてよいが、最低限の範囲を書いておく。

### 取り込むもの

- `internal/infra/database/firestore.go`、`internal/infra/repository/firestore/*.go`、`internal/infra/repository/firestore_open.go`、`internal/testutil/firestore.go`、`deploy/firestore/*`。
- `internal/infra/config/config.go`: `Backends.Firestore{ProjectID, DatabaseID}`。`repository.Open` に `firestore` ケース。
- `go.mod`: `cloud.google.com/go/firestore`、`golang.org/x/sync`、`google.golang.org/api` 等。`go mod tidy` の結果だけをコミットし、無関係なメジャー更新を混ぜない。

### 契約差分の吸収

- `develop` 側で増え、`support-firestore` が持たないメソッド: `QueryByParentOrderByKey`（CIP-5 `orderby=key`）、`QueryCommitLogs`（`c_date, id` 順のレプリケーション走査）。`shapes` に形状を追加し、`QueryCommitLogs` は `commits` を `cDate, __name__` で範囲走査する。
- フェーズ2で変わった契約: カウント系は `Compute*`（走査実装）に、`GetSignedDocuments` 複数取得、`record_keys` への `redirect` 非正規化、`MaintenanceRepository`。
- Firestore の `Core` ストアは1オブジェクトで record / residence / chunkline を実装し（C9）、`Inspector` 実装は `firestore/inspect_test.go` に置く（C10）。
- `AggregateStore` の Firestore 実装（`internal/infra/aggregate/firestore`）を追加し、`storetest` を通す。`Apply` は `FieldValue.Increment` 相当。
- `shapes` の Firestore コンシューマ: `firestore.indexes.json` の生成と、`shapes_test.go` によるドリフト検出、起動時の `VerifyIndexes` プローブ。

### テストと CI

- `internal/infra/repository/firestore/suite_test.go` で repotest 4 スイートを実行（エミュレータは dockertest で起動、イメージ `gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators`）。
- `.github/workflows/test.yml` にエミュレータ付きジョブを追加（`timeout-minutes: 30`、イメージキャッシュ）。

### ドキュメント

- `deploy/firestore/README.md` が参照する `MIGRATION.md` を書く（ダンプ → 起動 → `import-commitlog --rejected-out` → `import-subscriptions` → 差分比較）。
- README にバックエンド選択の節を追加。

### 完了条件

- `go test ./internal/infra/repository/firestore/ -count=1` と `./internal/infra/aggregate/firestore/` が緑。
- `shapes` に形状を1つ足して `indexes.json` を更新しない状態で `shapes_test.go` が **失敗する**。
- `backends.database: firestore` + `FIRESTORE_EMULATOR_HOST` で `concrnt` が起動し `/ready` が 200。
- Postgres → Firestore → Postgres のログ再生往復で `dump-commitlog` が一致。

---

## 6. 作業ルール

- ブランチ名: `feat/repository-contract-phase1`, `feat/repository-contract-<タスク名>`。
- 1 PR = 1 タスク（T1.1 など）を目安。巨大化するなら更に分ける。PR 本文には「成果物」「受け入れ条件の確認結果」「検証コマンドの出力要約」を書く。
- 必ず回すコマンド（Docker あり）:
  ```sh
  go build ./... && go vet ./...
  go test ./... -count=1
  ```
  Docker なしで回せる最低限:
  ```sh
  go test ./policy/... ./chunkline/... ./client/... ./internal/usecase/... -count=1
  ```
- `support-firestore` からの移植は `git show origin/support-firestore:<path>` で取り、**`develop` の変更を上書きしない**。両側変更ファイル（§3 方針に列挙）は手で 3-way マージし、PR 本文にそのファイル名を明記する。
- 契約（`internal/usecase/*/Repository`）を変えるときは、同じ PR で repotest と **全バックエンド**を更新する。片方だけ通る状態をマージしない。
- テスト専用コードは `_test.go` へ（C10）。やむを得ず共有する型は `repotest` / `testutil` / `storetest` に置き、非テストファイルから import しない。
- 新しいリポジトリインターフェースを切る前に C9 を適用する: 既存のテーブルを共有するなら既存のストアにメソッドを足す。
- 生成物（`schemas/*.go`、フェーズ3以降は `deploy/firestore/firestore.indexes.json`）は手で編集しない。生成コマンドを PR 本文に書く。
- 既存の挙動変更が避けられない場合は PR タイトルに `behavior:` を付け、本文に差分を書く。
- 本書の §8 以外の設計判断を変えたいときは、変える前にユーザーに確認する。

---

## 7. リスク

| リスク | 兆候 | 対処 |
|---|---|---|
| `support-firestore` と `develop` の 3-way マージで `develop` 側の修正を落とす | `commit_test.go` や replication のテストが失敗 / 期待値を書き換えたくなる | 期待値は変えない。`git log -p origin/develop -- <file>` で develop 側の意図を読んでから当てる。 |
| 集計キャッシュと実データのドリフト | `aggregate-verify` に差分 | 設計上は TTL で収束する。差分が TTL 内に消えない場合は `Apply` 側の差分符号・キー生成の誤り。複雑経路（カスケード、GC、import）が `Invalidate` を呼んでいるか確認。 |
| （フェーズ3）Firestore 単一エントリの書き込み上限 | 人気投稿へのリアクション集中で `Apply` が `Aborted` 連発 | `Apply` は最適化なので **失敗しても無視して `Invalidate`** に落とす。ホットキーはそれで自然に read-through に戻る。 |
| 初回アクセスの計算コスト（コールドスタート） | `aggregate-invalidate --all` 直後や保存先切り替え直後に `Compute*` が集中 | 計算は 1 ターゲット単位で有界。必要なら人気ターゲットの事前ウォーム（`conctl op aggregate-warm --top N`）を任意で用意。 |
| （フェーズ3）エミュレータ起動で CI が遅い/不安定 | タイムアウト | イメージをキャッシュ、`-run` でスイート分割、最後は Firestore テストを別ジョブに。 |
| Postgres 起動時マイグレーションの多重実行 | 複数レプリカ同時起動で DDL 競合 | `pg_advisory_xact_lock` + `IF NOT EXISTS` + `schema_migrations` で冪等に。 |
| 既存 DB への列追加 backfill が長い | `record_keys.redirect` backfill で起動が遅延 | バッチ分割、起動をブロックしないバックグラウンド実行、未 backfill 行は `records` へフォールバック。 |

---

## 8. 未決事項（推奨案つき。指示がなければ推奨案で進める）

- **A. 集計キャッシュの既定保存先と TTL** — 推奨: 保存先は `database` と同じバックエンド（追加ミドルウェアなしで動く）、TTL 24h、`maxDeltas` 10000。ariake では後から Redis に切り替えてよい（設定変更のみ）。
- **A2. Redis / memcached（将来 Firestore）ストアでの結果整合を許容するか** — 推奨: 許容する。集計はキャッシュであり、TTL で収束する。厳密性が必要になった時点で Postgres 実装相当のロック付きストアを選ぶ。
- **B. Postgres の `record_keys.id`（bigserial）を gen 文字列に置換するか** — 推奨: **フェーズ2ではやらない**。契約に漏れていない限り内部実装の自由（C3）。Spanner/CRDB バックエンドを書く段で、その実装側が独自に gen を持てばよい。
- **C. SQLite バックエンド（T2.5）をフェーズ2に含めるか** — 推奨: 含める。Docker なしで契約テストが回ることの価値が大きく、GORM 非依存バックエンドの雛形にもなる。
- **D. Postgres 実装の GORM 継続** — 推奨: 継続。書き換えコストに対して得るものが少ない。新規バックエンドは `database/sql` 直で書く（T2.5 が手本）。
- **E. `QueryByPrefix` の Postgres 実装を `ancestors` 方式に合わせるか** — 推奨: 合わせない。`LIKE` は Postgres では速く、契約は意味論のみを規定する。
- **F. （フェーズ3）CI の Firestore エミュレータを必須にするか** — 推奨: 必須（PR ブロッカー）。不安定なら別ジョブに分離するが、落としたままマージはしない。

---

## 9. 完了チェックリスト（全体）

- [ ] T1.1 `RunInTx` 契約、usecase 全面移行、Postgres 実装（リトライ付き）
- [ ] T1.2 `repository.Set/Open`、`main.go` / `conctl` 配線、`backends.database` 設定
- [ ] T1.3 保守用メソッドを `record.Repository` に統合、`Inspector`（interface は repotest、実装は `inspect_test.go`）、`gc-commitlog`/`dump-commitlog` 移行
- [ ] T1.4 repotest 導入、`QueryByParentOrderByKey` / `QueryCommitLogs` のテスト追加、Postgres で緑
- [ ] T1.5 `.github/workflows/test.yml`
- [ ] T1.6 README / `CONTRACT.md` 初版
- [ ] T2.1 契約棚卸し表
- [ ] T2.2 `AggregateStore` 層（5 実装 + storetest）、`Compute*` 契約、read-through と書き込み時再計算、ハンドラ移行、`aggregate-verify` / `aggregate-invalidate`
- [ ] T2.3 shapes 中立化、両バックエンドのインデックス検証、`pg_advisory_xact_lock`
- [ ] T2.4 複数取得、chunkline 非正規化、往復回数テスト
- [ ] T2.5 SQLite バックエンド
- [ ] T2.6 Postgres 整理、明示マイグレーション
- [ ] ログ再生による Postgres → SQLite → Postgres 往復の一致確認
- [ ] （フェーズ3）Firestore バックエンド移植、不足メソッド、`AggregateStore` Firestore 実装、`indexes.json`、エミュレータ CI、`MIGRATION.md`

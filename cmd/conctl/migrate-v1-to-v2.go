package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/concrnt/concrnt"
	cdidv2 "github.com/concrnt/concrnt/cdid"
	pgrepo "github.com/concrnt/concrnt/internal/infra/repository/postgres"
	"github.com/concrnt/concrnt/jwt"
	cdidv1 "github.com/concrnt/concrnt/legacy/cdid"
	"github.com/concrnt/concrnt/legacy/core"
	"github.com/concrnt/concrnt/schemas"
)

var WritePublicPolicyURL = "https://policy.concrnt.world/t/write-public.json"

var entityCache = make(map[string]core.Entity)

var mappingCache = make(map[string]string)

var (
	fromDsn   string
	fromFQDN  string
	fromCSID  string
	destDsn   string
	destFQDN  string
	ignoreIDs []string = []string{}
	token     string

	oneShotID string
)

type ReplyMessageSchema struct {
	ReplyToMessageId     string `json:"replyToMessageId"`
	ReplyToMessageAuthor string `json:"replyToMessageAuthor"`
	TargetURI            string `json:"targetURI,omitempty"`
	Body                 string `json:"body"`
	Emojis               any    `json:"emojis,omitempty"`
	ProfileOverride      any    `json:"profileOverride,omitempty"`
}

type RerouteMessageSchema struct {
	RerouteMessageId     string `json:"rerouteMessageId"`
	RerouteMessageAuthor string `json:"rerouteMessageAuthor"`
	TargetURI            string `json:"targetURI,omitempty"`
	Body                 string `json:"body,omitempty"`
	Emojis               any    `json:"emojis,omitempty"`
	ProfileOverride      any    `json:"profileOverride,omitempty"`
}

type ReplyAssociationSchema struct {
	TargetURI       string `json:"targetURI,omitempty"`
	MessageId       string `json:"messageId,omitempty"`
	MessageAuthor   string `json:"messageAuthor,omitempty"`
	ProfileOverride any    `json:"profileOverride,omitempty"`
}

type RerouteAssociationSchema struct {
	TargetURI       string `json:"targetURI,omitempty"`
	MessageId       string `json:"messageId,omitempty"`
	MessageAuthor   string `json:"messageAuthor,omitempty"`
	ProfileOverride any    `json:"profileOverride,omitempty"`
}

type MigrationInfo struct {
	Name   string `json:"name" gorm:"type:text;primaryKey"`
	Seeker string `json:"seeker" gorm:"type:text"`
}

type MigrationTable struct {
	V1ID string `json:"v1_id" gorm:"primaryKey"`
	V2ID string `json:"v2_id"`
}

func SaveMigrationTable(db *gorm.DB, v1id string, v2id string) error {
	table := MigrationTable{
		V1ID: v1id,
		V2ID: v2id,
	}
	result := db.Save(&table)
	return result.Error
}

func ResolveMigrationTable(db *gorm.DB, v1id string) (string, error) {
	var table MigrationTable
	result := db.First(&table, "v1_id = ?", v1id)
	if result.Error != nil {
		return "", result.Error
	}
	return table.V2ID, nil
}

// v1のmessage IDからv2の実キーを引く。このDBで移行済みのmessageはサブプロファイルを含む
// 実キーに解決できる。ソースDBに存在しないリモートのmessageはmainプロファイルで合成する。
func resolvePostKey(destDB *gorm.DB, author string, messageID string) string {
	key, err := ResolveMigrationTable(destDB, messageID)
	if err == nil {
		return key
	}
	return fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/posts/%s", author, messageID)
}

func LoadMigrationInfo(db *gorm.DB, name string) (*MigrationInfo, error) {
	var info MigrationInfo
	result := db.First(&info, "name = ?", name)
	if result.Error != nil {
		return nil, result.Error
	}
	return &info, nil
}

func SaveMigrationInfo(db *gorm.DB, info *MigrationInfo) error {
	result := db.Save(info)
	return result.Error
}

var actionMap = map[string]string{
	"timeline.message.read": "net.concrnt.core.resolve",
}

func convertDomain(domain string) string {
	if domain == fromFQDN {
		return destFQDN
	}
	if domain == fromCSID {
		return destFQDN
	}
	return domain
}

func convertTimeline(timeline string) string {
	split := strings.Split(timeline, "@")
	if len(split) != 2 {
		return timeline
	}
	switch split[0] {
	case "world.concrnt.t-home":
		{
			return fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/home-timeline", split[1])
		}
	case "world.concrnt.t-notify":
		{
			return fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/notify-timeline", split[1])
		}
	case "world.concrnt.t-assoc":
		{
			return fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/activity-timeline", split[1])
		}
	default:
		{

			if strings.HasPrefix(split[0], "world.concrnt.t-subhome.") {
				after := strings.TrimPrefix(split[0], "world.concrnt.t-subhome.")
				profileID := after
				return fmt.Sprintf("cckv://%s/concrnt.world/profiles/%s/home-timeline", convertDomain(split[1]), profileID)
			} else {
				return fmt.Sprintf("cckv://%s/concrnt.world/communities/%s", convertDomain(split[1]), split[0])
			}

		}
	}
}

// ImportResult is the per-line failure report returned by POST /api/v2/repository.
// The response array contains only the lines that failed to import.
type ImportResult struct {
	Document string `json:"document,omitempty"`
	Error    string `json:"error,omitempty"`
}

func commit(body string) ([]ImportResult, error) {

	request, err := http.NewRequest("POST", fmt.Sprintf("https://%s/api/v2/repository", destFQDN), strings.NewReader(body))
	if err != nil {
		fmt.Println("failed to create request: ", err)
		return nil, err
	}

	request.Header.Set("Content-Type", "text/plain")

	var tmpToken string
	if token != "" {
		_, claims, err := jwt.Parse(token)
		if err != nil {
			fmt.Println("failed to parse token: ", err)
			return nil, err
		}

		if claims.ExpirationTime != "" {
			expUnix, err := strconv.ParseInt(claims.ExpirationTime, 10, 64)
			if err != nil {
				fmt.Println("failed to parse token expiration time: ", err)
				return nil, err
			}
			expTime := time.Unix(expUnix, 0)
			if time.Until(expTime) < 1*time.Minute {
				token = generateToken("system", 1*time.Hour)
				fmt.Println("token is expiring soon, generated new token")
				tmpToken = token
			} else {
				tmpToken = token
			}
		} else {
			tmpToken = token
		}
	} else {
		token = generateToken("system", 1*time.Hour)
		fmt.Println("no token provided, generating new token")
		tmpToken = token
	}

	request.Header.Set("Authorization", "Bearer "+tmpToken)

	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		fmt.Println("failed to post document: ", err)
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("failed to read response body: ", err)
		return nil, err
	}

	if traceID := resp.Header.Get("trace-id"); traceID != "" {
		fmt.Println("traceID: ", traceID)
	}

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("unexpected status code %d from %s: %s", resp.StatusCode, request.URL, truncateForLog(string(respBody), 500))
		fmt.Println(err)
		return nil, err
	}

	var results []ImportResult
	err = json.Unmarshal(respBody, &results)
	if err != nil {
		err := fmt.Errorf("failed to parse import response (is the destination really a concrnt v2 server?): %w: %s", err, truncateForLog(string(respBody), 500))
		fmt.Println(err)
		return nil, err
	}

	return results, nil
}

func truncateForLog(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// reportImportErrors prints per-line import failures and returns their count.
func reportImportErrors(results []ImportResult) int {
	const maxShown = 20
	for i, result := range results {
		if i >= maxShown {
			fmt.Printf("  ... and %d more errors\n", len(results)-maxShown)
			break
		}
		fmt.Printf("  import error: %s (document: %s)\n", result.Error, truncateForLog(result.Document, 200))
	}
	return len(results)
}

func convertTimelines(timelines []string) []string {

	distributes := make([]string, 0, len(timelines))
	for _, timeline := range timelines {
		target := convertTimeline(timeline)
		if target != "" {
			distributes = append(distributes, target)
		}
	}
	return distributes
}

func hasSubprofileTimelien(timelines []string) (string, bool) {

	for _, timeline := range timelines {
		split := strings.Split(timeline, "@")
		if len(split) != 2 {
			continue
		}
		if strings.HasPrefix(split[0], "world.concrnt.t-subhome.") {
			after := strings.TrimPrefix(split[0], "world.concrnt.t-subhome.")
			profileID := after
			return profileID, true
		}
	}
	return "", false
}

func convertPolicy(policyURL string, policyParamsStr string, policyDefaultsStr string) *concrnt.Policy {

	entries := &[]concrnt.PolicyEntry{}
	if policyURL != "" {

		entry := concrnt.PolicyEntry{
			URL: &policyURL,
		}

		if policyParamsStr != "" {
			var policyParams map[string]any
			err := json.Unmarshal([]byte(policyParamsStr), &policyParams)
			if err != nil {
				fmt.Println("failed to unmarshal policy params: ", err)
			} else {
				entry.Params = &policyParams
			}
		}

		if policyDefaultsStr != "" {
			var policyDefaults map[string]string
			err := json.Unmarshal([]byte(policyDefaultsStr), &policyDefaults)
			if err != nil {
				fmt.Println("failed to unmarshal policy defaults: ", err)
			} else {
				defaults := make(map[string]string)
				for k, v := range policyDefaults {
					if action, ok := actionMap[v]; ok {
						defaults[k] = action
					} else {
						defaults[k] = v
					}
				}
				entry.Defaults = &defaults
			}
		}

		entries = &[]concrnt.PolicyEntry{entry}
	}

	return &concrnt.Policy{
		Entries: *entries,
	}
}

func transferMetas(fromDB, toDB *gorm.DB) {

	var v1metas []core.EntityMeta
	fromDB.
		Find(&v1metas)
	fmt.Println("total metas: ", len(v1metas))

	var v2metas []pgrepo.EntityMeta
	for _, v1meta := range v1metas {
		v2meta := pgrepo.EntityMeta{
			ID:      v1meta.ID,
			Inviter: v1meta.Inviter,
			Info:    v1meta.Info,
		}
		v2metas = append(v2metas, v2meta)
	}

	toDB.Save(&v2metas)

	// v1ではmetaを持たないローカル所属(CLIでのオーバーライド作成など)が存在しうるが、
	// v2はローカルentityのimportにmetaの存在(=ドメインへの登録)を要求する。
	// v1のentityが該当ドメインで作成されている時点で正当な登録であることは確認できて
	// いるので、migrationが自動でactivateしたことを示すmetaを補完してから
	// entityのimportに進む。
	metaExists := make(map[string]bool, len(v1metas))
	for _, m := range v1metas {
		metaExists[m.ID] = true
	}

	var localEntities []core.Entity
	fromDB.Where("domain in ?", []string{fromFQDN, fromCSID}).Find(&localEntities)

	var supplemented []pgrepo.EntityMeta
	for _, entity := range localEntities {
		if metaExists[entity.ID] || slices.Contains(ignoreIDs, entity.ID) {
			continue
		}
		supplemented = append(supplemented, pgrepo.EntityMeta{
			ID:   entity.ID,
			Info: `{"note":"automatically activated by v1-to-v2 migration (entity had no meta in v1)"}`,
		})
	}

	if len(supplemented) > 0 {
		toDB.Save(&supplemented)
		fmt.Println("auto-activated local entities without v1 meta: ", len(supplemented))
	}
}

func getEntity(db *gorm.DB, id string) (core.Entity, error) {
	if entity, ok := entityCache[id]; ok {
		return entity, nil
	}

	var entity core.Entity
	result := db.Where("id = ?", id).First(&entity)
	if result.Error != nil {
		return core.Entity{}, result.Error
	}

	entityCache[id] = entity

	return entity, nil
}

func isLocalEntity(entity core.Entity) bool {
	return entity.Domain == fromFQDN || entity.Domain == fromCSID
}

// keyOwnerIsLocal reports whether the owner segment of a cckv:// key is
// this server (destFQDN) or a locally-registered entity.
// 外部ユーザーのnamespace配下へのrecordはdest側のpolicyで拒否されるため、
// 生成段階でスキップするための判定に使う。
func keyOwnerIsLocal(db *gorm.DB, key string) bool {
	uri, err := concrnt.ParseCCURI(key)
	if err != nil {
		return false
	}
	if uri.Owner == destFQDN {
		return true
	}
	entity, err := getEntity(db, uri.Owner)
	if err != nil {
		return false
	}
	return isLocalEntity(entity)
}

func transferEntities(db *gorm.DB, dest_db *gorm.DB) error {

	var seeker time.Time
	info, err := LoadMigrationInfo(dest_db, "entities")
	if err != nil {
		fmt.Println("no existing migration info found, starting fresh")
	} else {
		fmt.Println("existing migration info found, seeker: ", info.Seeker)
		t, err := time.Parse(time.RFC3339, info.Seeker)
		if err != nil {
			panic("failed to parse seeker time: " + err.Error())
		} else {
			seeker = t
		}
	}

	var entities []core.Entity
	// NOTE: gormは空スライスの `not in ?` を `NOT IN (NULL)`(常に偽)に展開して
	// しまうため、ignoreIDs が空のときは条件を付けてはいけない
	q := db.Session(&gorm.Session{})
	if len(ignoreIDs) > 0 {
		q = q.Where("id not in ?", ignoreIDs)
	}

	if !seeker.IsZero() {
		q = q.Where("c_date >= ?", seeker)
	}

	q.Find(&entities)

	fmt.Println("total entities: ", len(entities))

	var batch string

	for _, entity := range entities {

		key := fmt.Sprintf("cckv://%s", entity.ID)

		domain := convertDomain(entity.Domain)

		// dest FQDNを本番と変えてテストする場合、v1にキャッシュされている
		// destドメイン所属の外部entityがdest側で「未登録のローカルユーザー」として
		// 拒否される。dest自身のユーザーをv1視点からimportする必要はないのでスキップ。
		if domain == destFQDN && !isLocalEntity(entity) {
			fmt.Printf("skipping entity %s: belongs to dest domain %s but is not local to %s\n", entity.ID, entity.Domain, fromFQDN)
			continue
		}

		v2doc := &concrnt.Document[any]{
			Kind: "entity",
			Key:  key,
			Value: map[string]any{
				"domain": domain,
			},
			Author:    entity.ID,
			Schema:    schemas.EntityURL,
			CreatedAt: entity.CDate,
		}

		serializedDoc, err := json.Marshal(v2doc)
		if err != nil {
			fmt.Println("failed to serialize v2 document: ", err)
			continue
		}

		sd := concrnt.SignedDocument{
			Document: string(serializedDoc),
			Proof: concrnt.Proof{
				Type: "none",
			},
		}

		line, err := json.Marshal(sd)
		if err != nil {
			fmt.Println("failed to serialize signed document: ", err)
			continue
		}

		batch += string(line) + "\n"
	}

	results, err := commit(batch)
	if err != nil {
		return fmt.Errorf("failed to commit entity batch: %w", err)
	}

	if failed := reportImportErrors(results); failed > 0 {
		// seekerを保存せずに中断する: 原因を直して再実行すれば同じentityを再送できる
		return fmt.Errorf("%d of %d entities failed to import", failed, len(entities))
	}

	err = SaveMigrationInfo(dest_db, &MigrationInfo{
		Name:   "entities",
		Seeker: time.Now().Format(time.RFC3339),
	})
	if err != nil {
		panic("failed to save migration info: " + err.Error())
	} else {
		fmt.Println("migration info saved with seeker: ", time.Now().Format(time.RFC3339))
	}

	return nil
}

func convertRecord(
	db *gorm.DB,
	destDB *gorm.DB,
	commit core.CommitLog) (string, error) {
	var v1doc core.DocumentBase[any]
	err := json.Unmarshal([]byte(commit.Document), &v1doc)
	if err != nil {
		fmt.Println("failed to unmarshal document base: ", err)
		return "", err
	}

	if slices.Contains(ignoreIDs, v1doc.Signer) {
		return "", nil
	}

	hash := core.GetHash([]byte(commit.Document))
	hash10 := [10]byte{}
	copy(hash10[:], hash[:10])
	signedAt := v1doc.SignedAt
	cdidBase := cdidv1.New(hash10, signedAt).String()

	v1Author, err := getEntity(db, v1doc.Signer)
	if err != nil {
		fmt.Printf("failed to get author entity for commit id %d: %s\n", commit.ID, err)
		return "", err
	}

	var v2doc *concrnt.Document[any]

	switch v1doc.Type {
	case "message":
		{

			var v1msg core.MessageDocument[any]
			err := json.Unmarshal([]byte(commit.Document), &v1msg)
			if err != nil {
				fmt.Println("failed to unmarshal message document: ", err)
				return "", err
			}

			key := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/posts/m%s", v1msg.Signer, cdidBase)
			subprofileID, hasSubprofile := hasSubprofileTimelien(v1msg.Timelines)
			if hasSubprofile {
				key = fmt.Sprintf("cckv://%s/concrnt.world/profiles/%s/posts/m%s", v1msg.Signer, subprofileID, cdidBase)
			}
			SaveMigrationTable(destDB, "m"+cdidBase, key)

			distributes := convertTimelines(v1msg.Timelines)

			pol := convertPolicy(v1msg.Policy, v1msg.PolicyParams, v1msg.PolicyDefaults)

			body := v1msg.Body

			switch v1msg.Schema {
			case "https://schema.concrnt.world/m/reply.json":
				var replyDoc core.MessageDocument[ReplyMessageSchema]
				err := json.Unmarshal([]byte(commit.Document), &replyDoc)
				if err != nil {
					fmt.Println("failed to unmarshal reply message document: ", err)
					return "", err
				}
				replyDoc.Body.TargetURI = resolvePostKey(destDB, replyDoc.Body.ReplyToMessageAuthor, replyDoc.Body.ReplyToMessageId)
				body = replyDoc.Body
			case "https://schema.concrnt.world/m/reroute.json":
				var rerouteDoc core.MessageDocument[RerouteMessageSchema]
				err := json.Unmarshal([]byte(commit.Document), &rerouteDoc)
				if err != nil {
					fmt.Println("failed to unmarshal reroute message document: ", err)
					return "", err
				}
				rerouteDoc.Body.TargetURI = resolvePostKey(destDB, rerouteDoc.Body.RerouteMessageAuthor, rerouteDoc.Body.RerouteMessageId)
				body = rerouteDoc.Body
			}

			v2doc = &concrnt.Document[any]{
				Kind:        "record",
				Key:         key,
				Value:       body,
				Author:      v1msg.Signer,
				Schema:      v1msg.Schema,
				CreatedAt:   v1msg.SignedAt,
				Distributes: &distributes,
				Policy:      pol,
			}

			lines := ""

			serializedDoc, err := json.Marshal(v2doc)
			if err != nil {
				fmt.Println("failed to serialize v2 document: ", err)
				return "", err
			}

			sd := concrnt.SignedDocument{
				Document: string(serializedDoc),
				Proof: concrnt.Proof{
					Type: "none",
				},
			}

			if isLocalEntity(v1Author) {
				line, err := json.Marshal(sd)
				if err != nil {
					fmt.Println("failed to serialize signed document: ", err)
					return "", err
				}
				lines += string(line) + "\n"
			}

			// mappingCacheへの登録は外部authorでも行う(association変換でのkey解決に必要)
			mappingKey := fmt.Sprintf("cckv://%s/concrnt.world/v1/m%s", v1msg.Signer, cdidBase)
			mappingCache[mappingKey] = key

			if isLocalEntity(v1Author) {
				mappingDoc := concrnt.Document[schemas.Reference]{
					Kind: "record",
					Key:  mappingKey,
					Value: schemas.Reference{
						Href:   key,
						Schema: &v1msg.Schema,
					},
					Author:    v1msg.Signer,
					Schema:    schemas.ReferenceURL,
					CreatedAt: v1msg.SignedAt,
				}

				mappingBytes, err := json.Marshal(mappingDoc)
				if err != nil {
					fmt.Println("failed to serialize mapping document: ", err)
					return "", err
				}

				mappingSD := concrnt.SignedDocument{
					Document: string(mappingBytes),
					Proof: concrnt.Proof{
						Type: "document-reference",
						Href: &key,
					},
					References: map[string]concrnt.SignedDocument{
						key: sd,
					},
				}

				line, err := json.Marshal(mappingSD)
				if err != nil {
					fmt.Println("failed to serialize mapping signed document: ", err)
					return "", err
				}
				lines += string(line) + "\n"
			}

			for _, timeline := range distributes {

				hash := concrnt.GetHash(serializedDoc)
				hash10 := [10]byte{}
				copy(hash10[:], hash[:10])
				documentID := cdidv2.New(hash10, v1msg.SignedAt).String()

				distKey := timeline + "/" + documentID

				authorURI := fmt.Sprintf("cckv://%s", v1msg.Signer)
				domainURI := fmt.Sprintf("cckv://%s", destFQDN)
				if !strings.HasPrefix(distKey, authorURI) && !strings.HasPrefix(distKey, domainURI) {
					continue // skip distributing to author's own timeline
				}
				if !keyOwnerIsLocal(db, distKey) {
					continue // 外部ユーザーのtimelineへの書き込みはpolicyで拒否されるためスキップ
				}

				distDoc := concrnt.Document[schemas.Reference]{
					Kind: "record",
					Key:  distKey,
					Value: schemas.Reference{
						Href:   key,
						Schema: &v1msg.Schema,
					},
					Author:    v1msg.Signer,
					Schema:    schemas.ReferenceURL,
					CreatedAt: v1msg.SignedAt,
				}
				docBytes, err := json.Marshal(distDoc)
				if err != nil {
					return "", err
				}
				distSD := concrnt.SignedDocument{
					Document: string(docBytes),
					Proof: concrnt.Proof{
						Type: "document-reference",
						Href: &key,
					},
					References: map[string]concrnt.SignedDocument{
						key: sd,
					},
				}

				lineBytes, err := json.Marshal(distSD)
				if err != nil {
					return "", err
				}

				lines += string(lineBytes) + "\n"

			}

			return lines, nil
		}
	case "profile":
		{
			var v1prof core.ProfileDocument[any]
			err := json.Unmarshal([]byte(commit.Document), &v1prof)
			if err != nil {
				fmt.Println("failed to unmarshal profile document: ", err)
				return "", err
			}

			key := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main", v1prof.Signer)

			v1id := "p" + cdidBase
			if v1prof.SemanticID != "" {
				v1id = v1prof.SemanticID
			} else if v1prof.ID != "" {
				v1id = v1prof.ID
			}
			SaveMigrationTable(destDB, v1id, key)

			if v1prof.SemanticID != "world.concrnt.p" {
				key = fmt.Sprintf("cckv://%s/concrnt.world/profiles/%s", v1prof.Signer, v1id)
			}

			v2doc = &concrnt.Document[any]{
				Kind:      "record",
				Key:       key,
				Value:     v1prof.Body,
				Author:    v1prof.Signer,
				Schema:    v1prof.Schema,
				CreatedAt: v1prof.SignedAt,
			}

		}
	case "association":
		{
			var v1ass core.AssociationDocument[any]
			err := json.Unmarshal([]byte(commit.Document), &v1ass)
			if err != nil {
				fmt.Println("failed to unmarshal association document: ", err)
				return "", err
			}

			assOwner, err := getEntity(db, v1ass.Owner)
			if err != nil {
				fmt.Printf("failed to get association owner entity for commit id %d: %s\n", commit.ID, err)
				return "", err
			}

			associateKey := fmt.Sprintf("cckv://%s/concrnt.world/v1/%s", v1ass.Owner, v1ass.Target)

			distributes := convertTimelines(v1ass.Timelines)
			pol := convertPolicy(v1ass.Policy, v1ass.PolicyParams, v1ass.PolicyDefaults)

			var variant *string
			if v1ass.Variant != "" {
				variant = &v1ass.Variant
			}

			body := v1ass.Body
			switch v1ass.Schema {
			case "https://schema.concrnt.world/a/reply.json":
				var replyAssoc core.AssociationDocument[ReplyAssociationSchema]
				err := json.Unmarshal([]byte(commit.Document), &replyAssoc)
				if err != nil {
					fmt.Println("failed to unmarshal reply association document: ", err)
					return "", err
				}
				replyAssoc.Body.TargetURI = resolvePostKey(destDB, replyAssoc.Body.MessageAuthor, replyAssoc.Body.MessageId)
				body = replyAssoc.Body
			case "https://schema.concrnt.world/a/reroute.json":
				var rerouteAssoc core.AssociationDocument[RerouteAssociationSchema]
				err := json.Unmarshal([]byte(commit.Document), &rerouteAssoc)
				if err != nil {
					fmt.Println("failed to unmarshal reroute association document: ", err)
					return "", err
				}
				rerouteAssoc.Body.TargetURI = resolvePostKey(destDB, rerouteAssoc.Body.MessageAuthor, rerouteAssoc.Body.MessageId)
				body = rerouteAssoc.Body
			}

			v2doc = &concrnt.Document[any]{
				Kind:        "association",
				Value:       body,
				Author:      v1ass.Signer,
				Schema:      v1ass.Schema,
				CreatedAt:   v1ass.SignedAt,
				Distributes: &distributes,
				Policy:      pol,

				Associate:          &associateKey,
				AssociationVariant: variant,
			}

			lines := ""

			serializedDocBytes, err := json.Marshal(v2doc)
			if err != nil {
				fmt.Println("failed to serialize v2 document: ", err)
				return "", err
			}

			sd := concrnt.SignedDocument{
				Document: string(serializedDocBytes),
				Proof: concrnt.Proof{
					Type: "none",
				},
			}

			hash := concrnt.GetHash([]byte(sd.Document))
			hash10 := [10]byte{}
			copy(hash10[:], hash[:10])
			documentID := cdidv2.New(hash10, v1ass.SignedAt).String()

			ccfs := concrnt.ComposeCCFSURI(v1ass.Owner, concrnt.CCFSTypeConcrnt, documentID)

			SaveMigrationTable(destDB, "a"+cdidBase, ccfs)

			if isLocalEntity(assOwner) {
				line, err := json.Marshal(sd)
				if err != nil {
					fmt.Println("failed to serialize signed document: ", err)
					return "", err
				}
				lines += string(line) + "\n"

				mappingKey, ok := mappingCache[associateKey]
				if !ok {
					var recordKey pgrepo.RecordKey
					err = destDB.
						Preload("Record").
						Preload("Record.Document").
						Where("uri = ?", associateKey).
						Take(&recordKey).Error
					if err != nil {
						fmt.Printf("failed to find record key for uri: %s, error: %s\n", associateKey, err)
						return "", fmt.Errorf("failed to find record key for uri: %s, error: %w", associateKey, err)
					}

					if recordKey.RecordID == nil {
						fmt.Printf("record key for uri: %s has no associated record\n", associateKey)
						return "", fmt.Errorf("record key for uri: %s has no associated record", associateKey)
					}

					var reference concrnt.Document[schemas.Reference]
					err = json.Unmarshal([]byte(recordKey.Record.Document.Document), &reference)
					if err != nil {
						fmt.Printf("failed to unmarshal record document for uri: %s, error: %s\n", associateKey, err)
						return "", fmt.Errorf("failed to unmarshal record document for uri: %s, error: %w", associateKey, err)
					}

					mappingKey = reference.Value.Href
				}

				assoc := &concrnt.Document[any]{
					Kind:        "association",
					Value:       body,
					Author:      v1ass.Signer,
					Schema:      v1ass.Schema,
					CreatedAt:   v1ass.SignedAt,
					Distributes: &distributes,
					Policy:      pol,

					Associate:          &mappingKey,
					AssociationVariant: variant,
				}

				assocBytes, err := json.Marshal(assoc)
				if err != nil {
					fmt.Println("failed to serialize association document: ", err)
					return "", err
				}

				// 実キー宛てassociationのccfsも登録し、delete変換時に両方消せるようにする
				assocHash := concrnt.GetHash(assocBytes)
				assocHash10 := [10]byte{}
				copy(assocHash10[:], assocHash[:10])
				assocDocumentID := cdidv2.New(assocHash10, v1ass.SignedAt).String()
				assocCcfs := concrnt.ComposeCCFSURI(v1ass.Owner, concrnt.CCFSTypeConcrnt, assocDocumentID)
				SaveMigrationTable(destDB, "a"+cdidBase+"#real", assocCcfs)

				assocSD := concrnt.SignedDocument{
					Document: string(assocBytes),
					Proof: concrnt.Proof{
						Type: "none",
					},
					References: map[string]concrnt.SignedDocument{
						mappingKey: sd,
					},
				}

				assocLine, err := json.Marshal(assocSD)
				if err != nil {
					fmt.Println("failed to serialize association signed document: ", err)
					return "", err
				}
				lines += string(assocLine) + "\n"

			}

			for _, timeline := range distributes {

				distKey := timeline + "/" + documentID
				authorURI := fmt.Sprintf("cckv://%s", v1ass.Signer)
				ownerURI := fmt.Sprintf("cckv://%s", v1ass.Owner)
				domainURI := fmt.Sprintf("cckv://%s", destFQDN)
				if !strings.HasPrefix(distKey, authorURI) && !strings.HasPrefix(distKey, domainURI) && !strings.HasPrefix(distKey, ownerURI) {
					continue
				}
				if !keyOwnerIsLocal(db, distKey) {
					continue // 外部ユーザーのtimelineへの書き込みはpolicyで拒否されるためスキップ
				}

				distDoc := concrnt.Document[schemas.Reference]{
					Kind: "record",
					Key:  distKey,
					Value: schemas.Reference{
						Href:   ccfs,
						Schema: &v1ass.Schema,
					},
					Author:    v1ass.Signer,
					Schema:    schemas.ReferenceURL,
					CreatedAt: v1ass.SignedAt,
				}
				docBytes, err := json.Marshal(distDoc)
				if err != nil {
					return "", err
				}
				distSD := concrnt.SignedDocument{
					Document: string(docBytes),
					Proof: concrnt.Proof{
						Type: "document-reference",
						Href: &ccfs,
					},
					References: map[string]concrnt.SignedDocument{
						ccfs: sd,
					},
				}

				lineBytes, err := json.Marshal(distSD)
				if err != nil {
					return "", err
				}

				lines += string(lineBytes) + "\n"

			}

			return lines, nil

		}
	case "timeline":
		{
			var v1tl core.TimelineDocument[any]
			err := json.Unmarshal([]byte(commit.Document), &v1tl)
			if err != nil {
				fmt.Println("failed to unmarshal timeline document: ", err)
				// continue
				return "", nil
			}

			owner := v1tl.Owner
			if owner == "" {
				owner = v1tl.Signer
			}
			if v1tl.DomainOwned {
				owner = destFQDN
			}

			id := "t" + cdidBase
			tlid := id /* + "@" + owner*/

			if v1tl.ID != "" {
				tlid = v1tl.ID
			}
			if v1tl.SemanticID != "" {
				tlid = v1tl.SemanticID
			}

			split := strings.Split(tlid, "@")
			if len(split) == 1 {
				tlid = tlid + "@" + owner
			}

			if strings.HasPrefix(tlid, "world.concrnt.t-ap") {
				return "", nil // skip old activitypub timelines
			}

			key := convertTimeline(tlid)
			//fmt.Println("converted timeline key: ", key)

			pol := convertPolicy(v1tl.Policy, v1tl.PolicyParams, v1tl.PolicyDefaults)

			if strings.Contains(key, "communities") {
				pol.Entries = append(pol.Entries, concrnt.PolicyEntry{
					URL: &WritePublicPolicyURL,
				})
			}

			// v1 created profile timelines as t/empty.json (subprofile homes as
			// t/subprofile.json), but native v2 clients create all of them as
			// t/user.json — normalize so schema filters recognize migrated ones
			schema := v1tl.Schema
			if strings.HasSuffix(key, "/home-timeline") ||
				strings.HasSuffix(key, "/notify-timeline") ||
				strings.HasSuffix(key, "/activity-timeline") {
				schema = "https://schema.concrnt.world/t/user.json"
			}

			v2doc = &concrnt.Document[any]{
				Kind:      "record",
				Key:       key,
				Value:     v1tl.Body,
				Author:    v1tl.Signer,
				Schema:    schema,
				CreatedAt: v1tl.SignedAt,
				Policy:    pol,
			}

			v1id := "t" + cdidBase
			if v1tl.ID != "" {
				v1id = id
			}

			SaveMigrationTable(destDB, v1id, key)
		}
	case "subscription":
		// {"owner":"con1khzfsjl2prkfa2c7ckfsyk7hk9nd84872hvve8","signer":"con1khzfsjl2prkfa2c7ckfsyk7hk9nd84872hvve8","type":"subscription","schema":"https://schema.concrnt.world/s/list.json","body":{"name":"Home"},"signedAt":"2025-06-03T15:03:41.221Z","indexable":false}
		{
			var v1sub core.SubscriptionDocument[any]
			err := json.Unmarshal([]byte(commit.Document), &v1sub)
			if err != nil {
				fmt.Println("failed to unmarshal subscription document: ", err)
				// continue
				return "", nil
			}

			id := "s" + cdidBase
			if v1sub.ID != "" {
				id = v1sub.ID
			}

			key := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/lists/%s", v1sub.Signer, id)

			v2doc = &concrnt.Document[any]{
				Kind:      "record",
				Key:       key,
				Value:     v1sub.Body,
				Author:    v1sub.Signer,
				Schema:    v1sub.Schema,
				CreatedAt: v1sub.SignedAt,
			}

			v1id := "s" + cdidBase
			if v1sub.ID != "" {
				v1id = id
			}
			SaveMigrationTable(destDB, v1id, key)
		}
	case "subscribe":
		// {"signer":"con1t0tey8uxhkqkd4wcp4hd4jedt7f0vfhk29xdd2","type":"subscribe","target":"tv9x2a976tp31yt6s06b9p2axz4@ariake.concrnt.net","subscription":"sqaspcetf6xaf5hdg067y1rga3g","signedAt":"2025-05-13T08:26:16.746Z","keyID":"cck1x9ee0xf4s7qrjze4n85malrdkreqtujfzq8jqv"}
		{
			var v1sub core.SubscribeDocument[any]
			err := json.Unmarshal([]byte(commit.Document), &v1sub)
			if err != nil {
				fmt.Println("failed to unmarshal subscribe document: ", err)
				// continue
				return "", nil
			}

			target := convertTimeline(v1sub.Target)
			targetHash := cdidv2.MakeHash([]byte(target))

			key := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/lists/%s/%s", v1sub.Signer, v1sub.Subscription, targetHash.String())

			schema := "https://schema.concrnt.world/t/community.json"
			if strings.HasPrefix(v1sub.Target, "world") {
				schema = "https://schema.concrnt.world/t/user.json"
			}

			v2doc = &concrnt.Document[any]{
				Kind: "record",
				Key:  key,
				Value: schemas.Reference{
					Href:   target,
					Schema: &schema,
				},
				Author:    v1sub.Signer,
				Schema:    schemas.ReferenceURL,
				CreatedAt: v1sub.SignedAt,
			}
		}
	case "unsubscribe":
		// {"signer":"con1t0tey8uxhkqkd4wcp4hd4jedt7f0vfhk29xdd2","type":"unsubscribe","target":"world.concrnt.t-home@con17hzd8gfpugmex6waxakrx3r42r2c33p6rgftna","subscription":"sg4gfzh4bqew8j7fp067v1wp9rr","signedAt":"2025-06-12T07:10:03.611Z","keyID":"cck1x9ee0xf4s7qrjze4n85malrdkreqtujfzq8jqv"}

		var v1unsub core.SubscribeDocument[any]
		err := json.Unmarshal([]byte(commit.Document), &v1unsub)
		if err != nil {
			fmt.Println("failed to unmarshal unsubscribe document: ", err)
			// continue
			return "", nil
		}

		target := convertTimeline(v1unsub.Target)
		targetHash := cdidv2.MakeHash([]byte(target))

		key := fmt.Sprintf("cckv://%s/concrnt.world/profiles/main/lists/%s/%s", v1unsub.Signer, v1unsub.Subscription, targetHash.String())

		v2doc = &concrnt.Document[any]{
			Kind:      "delete",
			Value:     key,
			Author:    v1unsub.Signer,
			CreatedAt: v1unsub.SignedAt,
		}

	case "delete":
		{
			var v1del core.DeleteDocument
			err := json.Unmarshal([]byte(commit.Document), &v1del)
			if err != nil {
				fmt.Println("failed to unmarshal delete document: ", err)
				// continue
				return "", nil
			}

			targetKey, err := ResolveMigrationTable(destDB, v1del.Target)
			if err != nil {
				fmt.Printf("skipping delete document with unknown target ID: %s\n", v1del.Target)
				return "", nil
			}

			targetKeys := []string{targetKey}

			// associationは互換キー宛てと実キー宛ての2つを発行しているので、
			// 実キー側の登録があればそちらのdeleteも発行する
			realKey, err := ResolveMigrationTable(destDB, v1del.Target+"#real")
			if err == nil {
				targetKeys = append(targetKeys, realKey)
			}

			lines := ""
			for _, key := range targetKeys {
				delDoc := &concrnt.Document[any]{
					Kind:      "delete",
					Value:     key,
					Author:    v1del.Signer,
					CreatedAt: v1del.SignedAt,
				}

				serializedDoc, err := json.Marshal(delDoc)
				if err != nil {
					fmt.Println("failed to serialize v2 document: ", err)
					return "", err
				}

				sd := concrnt.SignedDocument{
					Document: string(serializedDoc),
					Proof: concrnt.Proof{
						Type: "none",
					},
				}

				line, err := json.Marshal(sd)
				if err != nil {
					fmt.Println("failed to serialize signed document: ", err)
					return "", err
				}
				lines += string(line) + "\n"
			}

			return lines, nil
		}
	case "ack", "unack", "enact", "affiliation", "event":
		// continue // skip these types for now
		return "", nil
	default:
		{
			fmt.Printf("skipping document with unsupported type: %s\n", v1doc.Type)
			// continue
			return "", nil
		}
	}

	// 外部ユーザーのnamespace配下のkeyを持つdocument(外部ユーザーのprofile等)は
	// dest側で拒否されるため生成しない
	if v2doc.Key != "" && !keyOwnerIsLocal(db, v2doc.Key) {
		return "", nil
	}

	serializedDoc, err := json.Marshal(v2doc)
	if err != nil {
		fmt.Println("failed to serialize v2 document: ", err)
		return "", err
	}

	sd := concrnt.SignedDocument{
		Document: string(serializedDoc),
		Proof: concrnt.Proof{
			Type: "none",
		},
	}

	line, err := json.Marshal(sd)
	if err != nil {
		fmt.Println("failed to serialize signed document: ", err)
		return "", err
	}

	return string(line), nil
}

func transferRecords(db *gorm.DB, dest_db *gorm.DB) error {

	totalImportErrors := 0

	lastKey := uint(0)
	info, err := LoadMigrationInfo(dest_db, "records")
	if err != nil {
		fmt.Println("no existing migration info found, starting fresh")
	} else {
		fmt.Println("existing migration info found, seeker: ", info.Seeker)
		t, err := strconv.ParseUint(info.Seeker, 10, 64)
		if err != nil {
			panic("failed to parse seeker key: " + err.Error())
		} else {
			lastKey = uint(t)
		}
	}

	var latestCommit core.CommitLog
	db.Order("id desc").First(&latestCommit)
	fmt.Println("latest commit ID in source database: ", latestCommit.ID)

	startCommitID := lastKey
	lastCommitID := latestCommit.ID

	pageSize := 512

	for {

		progress := float64(lastKey-startCommitID) / float64(lastCommitID-startCommitID) * 100
		fmt.Printf("progress: %.2f%%\n", progress)

		var commits []core.CommitLog
		db.Where("id > ?", lastKey).
			Order("id asc").
			Limit(pageSize).
			Find(&commits)
		if len(commits) == 0 {
			break
		}

		lastKey = commits[len(commits)-1].ID

		var batch string

		for _, commit := range commits {

			lines, err := convertRecord(db, dest_db, commit)
			if err != nil {
				fmt.Printf("failed to convert record with commit id %d: %s\n", commit.ID, err)
				continue
			}

			batch += lines + "\n"
		}

		results, err := commit(batch)
		if err != nil {
			return fmt.Errorf("failed to commit batch: %w", err)
		}

		totalImportErrors += reportImportErrors(results)

		fmt.Println("indexed until -> ", lastKey)

		err = SaveMigrationInfo(dest_db, &MigrationInfo{
			Name:   "records",
			Seeker: strconv.FormatUint(uint64(lastKey), 10),
		})
		if err != nil {
			panic("failed to save migration info: " + err.Error())
		} else {
			fmt.Println("migration info saved with seeker: ", lastKey)
		}

		if len(commits) < pageSize { // no more commits
			break
		}
	}

	// 個々のdocumentのimportエラーは完走扱い(exit 0)にする。
	// commit()自体の失敗(HTTPエラー等)は途中で中断しているのでエラーを返す。
	if totalImportErrors > 0 {
		fmt.Printf("warning: %d records failed to import (see errors above). seeker has advanced past them; use --one-shot <document_id> to retry individual documents\n", totalImportErrors)
	}

	return nil
}

var migrateV1toV2Cmd = &cobra.Command{
	Use:   "v1-to-v2",
	Short: "Migrate data from a v1 server to a v2 server",
	Run: func(cmd *cobra.Command, args []string) {

		fromDB, err := gorm.Open(postgres.Open(fromDsn), &gorm.Config{})
		if err != nil {
			panic("failed to connect database")
		}

		destDB, err := gorm.Open(postgres.Open(destDsn), &gorm.Config{})
		if err != nil {
			panic("failed to connect destination database")
		}

		if oneShotID != "" {
			var commitLog core.CommitLog
			result := fromDB.Where("document_id = ?", oneShotID).First(&commitLog)
			if result.Error != nil {
				fmt.Printf("failed to find commit with id %s: %s\n", oneShotID, result.Error)
				return
			}

			lines, err := convertRecord(fromDB, destDB, commitLog)
			if err != nil {
				fmt.Printf("failed to convert record with commit id %d: %s\n", commitLog.ID, err)
				return
			}

			fmt.Println(string(lines))

			results, err := commit(string(lines) + "\n")
			if err != nil {
				fmt.Println("failed to commit document: ", err)
				return
			}
			if failed := reportImportErrors(results); failed > 0 {
				fmt.Println("failed to import document")
				os.Exit(1)
			}

		} else {
			destDB.AutoMigrate(&MigrationInfo{}, &MigrationTable{})

			transferMetas(fromDB, destDB)

			err = transferEntities(fromDB, destDB)
			if err != nil {
				fmt.Println("entity migration failed: ", err)
				fmt.Println("aborting before record migration. fix the cause and re-run.")
				os.Exit(1)
			}

			err = transferRecords(fromDB, destDB)
			if err != nil {
				fmt.Println("record migration failed: ", err)
				os.Exit(1)
			}
		}
	},
}

func init() {
	migrateCmd.AddCommand(migrateV1toV2Cmd)
	migrateV1toV2Cmd.Flags().StringVar(&fromDsn, "from-dsn", fromDsn, "PostgreSQL DSN of the v1 server to migrate from")
	migrateV1toV2Cmd.Flags().StringVar(&destDsn, "dest-dsn", destDsn, "PostgreSQL DSN of the v2 server to migrate to")
	migrateV1toV2Cmd.Flags().StringSliceVar(&ignoreIDs, "ignore-ccids", ignoreIDs, "List of entity IDs to ignore during migration")
	migrateV1toV2Cmd.Flags().StringVar(&fromFQDN, "from-fqdn", fromFQDN, "Domain of the v1 server to migrate from")
	migrateV1toV2Cmd.Flags().StringVar(&fromCSID, "from-csid", fromCSID, "CSID of the v1 server to migrate from")
	migrateV1toV2Cmd.Flags().StringVar(&destFQDN, "dest-fqdn", destFQDN, "Domain of the v2 server to migrate to")
	migrateV1toV2Cmd.Flags().StringVar(&token, "token", token, "Authentication token for the v2 server")

	migrateV1toV2Cmd.Flags().StringVar(&oneShotID, "one-shot", oneShotID, "If set, only migrate the record with the specified commit ID (for testing/debugging)")

	migrateV1toV2Cmd.MarkFlagRequired("from-dsn")
	migrateV1toV2Cmd.MarkFlagRequired("dest-dsn")
	migrateV1toV2Cmd.MarkFlagRequired("from-fqdn")
	migrateV1toV2Cmd.MarkFlagRequired("from-csid")
	migrateV1toV2Cmd.MarkFlagRequired("dest-fqdn")
}

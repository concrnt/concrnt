package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/concrnt/concrnt"
	"github.com/concrnt/concrnt/internal/domain"
	"github.com/concrnt/concrnt/internal/service"
	"github.com/concrnt/concrnt/internal/usecase/record"
	"github.com/concrnt/concrnt/internal/usecase/residence"
	"github.com/concrnt/concrnt/internal/usecase/server"
	"github.com/concrnt/concrnt/policy"
)

var (
	createAccountAlias   string
	createAccountInfo    string
	createAccountInviter string
	createAccountPrivkey string
)

type nopSignal struct{}

func (nopSignal) Publish(context.Context, string, concrnt.Event) error {
	return nil
}

type nopPolicy struct{}

func (nopPolicy) Eval(context.Context, policy.RequestContext, []concrnt.Policy, string, string) error {
	return nil
}

// nopJobQueue discards background jobs: account creation via conctl is a
// local, one-shot operation with no federation distribution expected.
type nopJobQueue struct{}

func (nopJobQueue) Enqueue(context.Context, string, any) error {
	return nil
}

func (nopJobQueue) RegisterHandler(string, func(context.Context, json.RawMessage) error) {}

var createAccountCmd = &cobra.Command{
	Use:   "create-account",
	Short: "Create a new local account and commit its entity document",
	RunE: withOperationContext(func(cmd *cobra.Command, args []string, op *operationContext) error {
		info, err := normalizeCreateAccountInfo(createAccountInfo)
		if err != nil {
			return err
		}

		alias := normalizeCreateAccountAlias(createAccountAlias)
		inviter, err := normalizeCreateAccountInviter(createAccountInviter)
		if err != nil {
			return err
		}

		var identity generatedIdentity
		if createAccountPrivkey != "" {
			identity, err = identityFromPrivateKey(createAccountPrivkey)
		} else {
			identity, err = generateIdentityMaterial()
		}
		if err != nil {
			return err
		}

		req, err := buildCreateAccountRequest(identity, op.GlobalConfig.FQDN, alias, info, time.Now().UTC())
		if err != nil {
			return err
		}

		// conctlはDB直結の信頼経路なので、registrationモード(invite/close)による
		// 登録制限は適用しない
		op.GlobalConfig.Registration = "open"

		residenceRepo := op.Repos.Residence
		serverUC := server.New(op.Repos.Server, &op.GlobalConfig, concrnt.SoftwareInfo{}, service.NewModuleManager(map[string]string{}, nil), op.Client)
		recordUC := record.New(op.Repos.Record, residenceRepo, serverUC, &op.GlobalConfig, op.Client, nopSignal{}, nopPolicy{}, nopJobQueue{}, nil)
		residenceUC := residence.New(residenceRepo, recordUC, &op.GlobalConfig)

		if err := residenceUC.Register(cmd.Context(), "", req); err != nil {
			return fmt.Errorf("failed to create account: %w", err)
		}

		// Registerはinviterをinviteトークンからのみ導出するため、管理者が
		// 明示した--inviterはここで直接metaへ反映する(conctlはDB直結の信頼経路)
		if inviter != nil {
			if err := residenceRepo.SaveMeta(cmd.Context(), domain.EntityMeta{
				ID:      identity.CCID,
				Inviter: inviter,
				Info:    info,
			}); err != nil {
				return fmt.Errorf("failed to set inviter on entity meta: %w", err)
			}
		}

		fmt.Println("domain:\t\t", op.GlobalConfig.FQDN)
		if alias != nil {
			fmt.Println("alias:\t\t", *alias)
		}
		if inviter != nil {
			fmt.Println("inviter:\t", *inviter)
		}
		fmt.Println("ccid:\t\t", identity.CCID)
		if identity.Mnemonic != "" {
			fmt.Println("mnemonic:\t", identity.Mnemonic)
		}
		fmt.Println("privatekey:\t", identity.PrivateKey)
		fmt.Println("publickey:\t", identity.PublicKey)

		return nil
	}),
}

func init() {
	operationCmd.AddCommand(createAccountCmd)

	createAccountCmd.Flags().StringVar(&createAccountAlias, "alias", "", "Optional alias to bind to the account")
	createAccountCmd.Flags().StringVar(&createAccountInfo, "info", "null", "JSON value to store in entity meta.info")
	createAccountCmd.Flags().StringVar(&createAccountInviter, "inviter", "", "Optional inviter CCID to store in entity meta")
	createAccountCmd.Flags().StringVar(&createAccountPrivkey, "privatekey", "", "Use an existing secp256k1 private key (hex) instead of generating a new identity")
}

package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var identityCmd = &cobra.Command{
	Use:   "identity",
	Short: "Generate a new concrnt identity",
	RunE: func(cmd *cobra.Command, args []string) error {
		identity, err := generateIdentityMaterial()
		if err != nil {
			return err
		}

		fmt.Println("ccid:\t\t", identity.CCID)
		fmt.Println("mnemonic:\t", identity.Mnemonic)
		fmt.Println("privatekey:\t", identity.PrivateKey)
		fmt.Println("publickey:\t", identity.PublicKey)
		return nil
	},
}

func init() {
	generateCmd.AddCommand(identityCmd)
}

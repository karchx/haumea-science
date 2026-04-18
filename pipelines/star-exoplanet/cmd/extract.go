package cmd

import (
	"github.com/karchx/haumea-science/pipelines/exoplanets"
	"github.com/spf13/cobra"
)

// extractCmd represents the extract command
var extractCmd = &cobra.Command{
	Use:   "extract",
	Short: "",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		minioClient, err := initMinioClient()
		if err != nil {
			return err
		}

		amqpConn, err := initAMQPConnection()
		if err != nil {
			return err
		}
		defer amqpConn.Close()
		exoplanets.RunExtract(minioClient, amqpConn)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(extractCmd)
}

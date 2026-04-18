package cmd

import (
	"fmt"
	"os"

	log "github.com/gothew/l-og"
	"github.com/karchx/haumea-science/pipelines/exoplanets"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/cobra"
)

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:   "load",
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
		exoplanets.RunLoad(minioClient, amqpConn)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(loadCmd)
}

func initMinioClient() (*minio.Client, error) {
	endpoint := "minio.platform:9000"
	accessKey := os.Getenv("AWS_ACCESS_KEY")
	secretKey := os.Getenv("AWS_SECRET_KEY")

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("Failed to initialize MinIO client: %v", err)
		return nil, err
	}

	return minioClient, nil
}

func initAMQPConnection() (*amqp.Connection, error) {
	pass := os.Getenv("RABBITMQ_PASS")
	username := os.Getenv("RABBITMQ_USER")
	amqpURL := fmt.Sprintf("amqp://%s:%s@rabbitmq-cluster.platform:5672/", username, pass)
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
		return nil, err
	}
	return conn, nil
}

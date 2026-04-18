package exoplanets

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	log "github.com/gothew/l-og"
	"github.com/minio/minio-go/v7"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ref: https://exoplanetarchive.ipac.caltech.edu/docs/API_PS_columns.html
// ref_tables: https://exoplanetarchive.ipac.caltech.edu/docs/TAP/usingTAP.html#examples
const BASE_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"

type JobResult struct {
	Catalog string
	Data    []byte
	Hash    string
	Err     error
}

type BatchMessage struct {
	Catalog string            `json:"catalog"`
	Data    []PlanetarySystem `json:"data"`
	Hash    string            `json:"hash"`
}

func RunLoad(minioClient *minio.Client, amqpConn *amqp.Connection) {
	var wg sync.WaitGroup
	idleTimeout := 5 * time.Second
	idleTimer := time.NewTimer(idleTimeout)

	log.Infof("Starting to consume messages from RabbitMQ...")

	queueName := "exoplanets_queue"
	ch, err := amqpConn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()
	err = ch.Qos(
		10,
		0,
		false,
	)
	if err != nil {
		log.Fatalf("Failed to set QoS: %v", err)
	}

	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)

consumerLoop:
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				log.Infof("Message channel closed, exiting consumer loop")
				break consumerLoop
			}

			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(idleTimeout)

			wg.Add(1)

			go func(m amqp.Delivery) {
				defer wg.Done()
				processData(m, minioClient)
			}(msg)

		case <-idleTimer.C:
			log.Infof("No messages received for %v, shutting down consumer...", idleTimeout)
			break consumerLoop
		}
	}

	log.Info("Waiting for ongoing message processing to complete...")
	wg.Wait()
	log.Info("All messages processed, exiting.")
}

func RunExtract(minioClient *minio.Client, amqpConn *amqp.Connection) {
	results := make(chan JobResult, len(Catalogs))
	var wg sync.WaitGroup
	ch, err := amqpConn.Channel()
	defer ch.Close()

	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	_, err = ch.QueueDeclare(
		"exoplanets_queue", // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	for _, catalog := range Catalogs {
		query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", joinColumns(catalog.Columns), catalog.Name, catalog.OrderBy)
		wg.Add(1)
		go fetchData(catalog.Name, query, &wg, results)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		log.Infof("Received data for catalog %s (size: %d bytes)", res.Catalog, len(res.Data))
		if res.Err != nil {
			log.Errorf("Error processing catalog %s: %v", res.Catalog, res.Err)
			continue
		}

		// idempotency check can be implemented here using res.Hash
		publishInBatch(ch, "exoplanets_queue", res.Catalog, res.Data)
		log.Infof("Successfully fetched data for catalog %s (size: %d bytes)", res.Catalog, len(res.Data))
	}
}

func fetchData(catalog, query string, wg *sync.WaitGroup, ch chan<- JobResult) {
	defer wg.Done()

	urlParse, err := url.Parse(BASE_URL)
	if err != nil {
		ch <- JobResult{Catalog: catalog, Err: fmt.Errorf("error parsing URL: %v", err)}
		return
	}
	q := urlParse.Query()
	q.Set("query", query)
	q.Set("format", "json")
	urlParse.RawQuery = q.Encode()
	apiUrl := urlParse.String()

	resp, err := http.Get(apiUrl)
	if err != nil {
		ch <- JobResult{Catalog: catalog, Err: fmt.Errorf("error fetching data: %v", err)}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		ch <- JobResult{Catalog: catalog, Err: fmt.Errorf("unexpected status code: %d", resp.StatusCode)}
		return
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		ch <- JobResult{Catalog: catalog, Err: fmt.Errorf("error decoding JSON: %v", err)}
		return
	}

	log.Infof("Fetched data for catalog %s (size: %d bytes)", catalog, len(data))
	ch <- JobResult{
		Catalog: catalog,
		Data:    data,
		Err:     nil,
	}
}

func joinColumns(cols []string) string {
	return strings.Join(cols, ", ")
}

func publishInBatch(ch *amqp.Channel, queueName, catalog string, rawData []byte) {
	var records []PlanetarySystem
	if err := json.Unmarshal(rawData, &records); err != nil {
		log.Errorf("Error unmarshaling data for catalog %s: %v", catalog, err)
		return
	}

	batchSize := 1000
	total := len(records)

	for i := 0; i < total; i += batchSize {
		end := i + batchSize
		if end > total {
			end = total
		}

		batch := records[i:end]
		batchBytes, err := json.Marshal(batch)
		if err != nil {
			log.Errorf("Error marshaling batch for catalog %s: %v", catalog, err)
			continue
		}

		hash := sha256.Sum256(batchBytes)
		batchHash := hex.EncodeToString(hash[:])

		payload := map[string]any{
			"catalog": catalog,
			"data":    batch,
			"hash":    batchHash,
		}
		payloadBytes, err := json.Marshal(payload)

		err = ch.Publish(
			"",        // exchange
			queueName, // routing key (queue name)
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        payloadBytes,
			})

		if err != nil {
			log.Errorf("Failed to publish batch to RabbitMQ for catalog %s: %v", catalog, err)
		}
	}

	log.Infof("Published %d records in batches for catalog %s", total, catalog)
}

func processData(msg amqp.Delivery, minioClient *minio.Client) {
	var batch BatchMessage
	ctx := context.Background()

	if err := json.Unmarshal(msg.Body, &batch); err != nil {
		log.Errorf("Error unmarshaling message: %v", err)
		msg.Nack(false, false) // Reject the message without requeueing
		return
	}

	jsonData, err := json.Marshal(batch.Data)
	if err != nil {
		fmt.Println("Error marshaling data:", err)
		return
	}
	objName := fmt.Sprintf("%s/%s/%s/exoplanets_%d.json", "raw", batch.Catalog, batch.Hash, time.Now().Unix())

	// Upload the JSON data to MinIO
	info, err := minioClient.PutObject(
		ctx,
		"gaia-source",
		objName,
		bytes.NewReader(jsonData),
		int64(len(jsonData)),
		minio.PutObjectOptions{ContentType: "application/json"},
	)

	if err != nil {
		log.Errorf("Failed to upload Parquet file to MinIO: %v", err)
		return
	}

	log.Infof("Successfully uploaded Parquet file to MinIO: %s (size: %d bytes)", info.Key, info.Size)
	msg.Ack(false) // Acknowledge the message after successful processing
}

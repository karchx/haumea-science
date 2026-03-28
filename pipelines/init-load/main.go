package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/csv"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/google/uuid"

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	log "github.com/gothew/l-og"
)

type PartitionTable struct {
	PartitionKey   string `json:"partition_key"`
	PartitionValue string `json:"partition_value"`
}

type TablesConfig struct {
	TableName  string           `json:"table_name"`
	Source     string           `json:"source"`
	Dest       string           `json:"dest"`
	Format     string           `json:"format"`
	Partitions []PartitionTable `json:"partitions"`
}

const bucketName = "gaia-source"

func main() {
	endpoint := "localhost:9000"
	accessKey := "aqS77seDQ3DC9wTiE9zA"
	secretKey := "GwImjA0vi0bSIW5BZUEdJjsjUE9sWT50bxk9vtnP"
	useSSL := false

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		log.Fatalf("Failed to create MinIO client: %v", err)
	}

	configData, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var tablesConfig []TablesConfig
	if err := json.Unmarshal(configData, &tablesConfig); err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}

	pool := memory.NewGoAllocator()
	ctx := context.Background()

	for _, tbl := range tablesConfig {
		if strings.ToLower(tbl.Format) != "csv" {
			continue
		}

		for i := range tbl.Partitions {
			part := &tbl.Partitions[i]
			if part.PartitionKey == "year" && part.PartitionValue == "current_year" {
				part.PartitionValue = time.Now().Format("2006")
			}
			if part.PartitionKey == "month" && part.PartitionValue == "current_month" {
				part.PartitionValue = time.Now().Format("01")
			}
			if part.PartitionKey == "day" && part.PartitionValue == "current_day" {
				part.PartitionValue = time.Now().Format("02")
			}
		}
		log.Infof("Processing table: %s", tbl.TableName)
		processCSVToParquet(ctx, tbl, minioClient, pool)
	}
}

func processCSVToParquet(ctx context.Context, config TablesConfig, minioClient *minio.Client, pool memory.Allocator) {
	table, err := readFileRemote(ctx, minioClient, config.Source, pool)
	if err != nil {
		log.Errorf("Failed to read source file from MinIO: %v", err)
	}

	var buf bytes.Buffer

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)

	pqWriter, err := pqarrow.NewFileWriter(table.Schema(), &buf, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		log.Errorf("Failed to create Parquet writer: %v", err)
	}

	if err := pqWriter.WriteTable(table, table.NumRows()); err != nil {
		log.Errorf("Failed to write record to Parquet: %v", err)
	}
	pqWriter.Close()

	// generate partions
	partitionPath := fmt.Sprintf("%s/%s/", config.Dest, config.TableName)
	for _, part := range config.Partitions {
		partitionPath += fmt.Sprintf("%s=%s/", part.PartitionKey, part.PartitionValue)
	}
	partitionPath += fmt.Sprintf("part-%s.parquet", uuid.New().String())

	info, err := minioClient.PutObject(ctx, bucketName, partitionPath, bytes.NewReader(buf.Bytes()), int64(buf.Len()), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		log.Errorf("Failed to upload Parquet file to MinIO: %v", err)
	}

	log.Infof("Successfully uploaded Parquet file to MinIO: %s (size: %d bytes)", info.Key, info.Size)
}

func readFileRemote(ctx context.Context, minioClient *minio.Client, objectName string, pool memory.Allocator) (arrow.Table, error) {
	object, err := minioClient.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from MinIO: %w", err)
	}
	defer object.Close()

	reader := csv.NewInferringReader(
		object,
		csv.WithAllocator(pool),
		csv.WithChunk(10000),
		csv.WithHeader(true),
		csv.WithComma(','),
		csv.WithComment('#'),
		csv.WithNullReader(true, "NULL", "null", ""),
	)
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("%w", err)
		}
		return nil, fmt.Errorf("no data found in CSV file: %s", objectName)
	}

	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}

	if reader.Err() != nil {
		return nil, fmt.Errorf("error reading CSV file: %w", reader.Err())
	}

	table := array.NewTableFromRecords(reader.Schema(), records)
	defer table.Release()

	// free records
	for _, rec := range records {
		rec.Release()
	}

	return table, nil
}

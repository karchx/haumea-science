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
	"github.com/apache/arrow/go/v17/parquet/file"
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
	accessKey := os.Getenv("AWS_ACCESS_KEY")
	secretKey := os.Getenv("AWS_SECRET_KEY")
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

		if strings.ToLower(tbl.Format) == "csv" {
			log.Infof("Processing table: %s format: %s", tbl.TableName, tbl.Format)
			processCSVToParquet(ctx, tbl, minioClient, pool)
		} else if strings.ToLower(tbl.Format) == "parquet" {
			log.Infof("Processing table: %s format: %s", tbl.TableName, tbl.Format)
			processParquetToParquet(ctx, tbl, minioClient, pool)
		} else {
			log.Infof("Table format %s not support", tbl.Format)
			return
		}

		log.Infof("Process complete")
	}
}

func processCSVToParquet(ctx context.Context, config TablesConfig, minioClient *minio.Client, pool memory.Allocator) {
	table, err := readFileRemote(ctx, minioClient, config.Source, pool)
	if err != nil {
		log.Errorf("Failed to read source file from MinIO: %v", err)
		return
	}
	defer table.Release()

	var buf bytes.Buffer

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDataPageVersion(parquet.DataPageV1),
		parquet.WithVersion(parquet.V1_0),
		parquet.WithDictionaryDefault(true),
	)

	pqWriter, err := pqarrow.NewFileWriter(table.Schema(), &buf, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		log.Errorf("Failed to create Parquet writer: %v", err)
		return
	}

	if err := pqWriter.WriteTable(table, table.NumRows()); err != nil {
		log.Errorf("Failed to write record to Parquet: %v", err)
		return
	}
	if err := pqWriter.Close(); err != nil {
		log.Errorf("Failed to close Parquet writer: %v", err)
		return
	}

	finalData := buf.Bytes()
	dataLen := int64(buf.Len())

	// generate partions
	partitionPath := createPathPartition(config)

	info, err := minioClient.PutObject(ctx, bucketName, partitionPath, bytes.NewReader(finalData), dataLen, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		log.Errorf("Failed to upload Parquet file to MinIO: %v", err)
		return
	}

	log.Infof("Successfully uploaded Parquet file to MinIO: %s (size: %d bytes)", info.Key, info.Size)
}

func processParquetToParquet(ctx context.Context, config TablesConfig, minioClient *minio.Client, pool memory.Allocator) {
	table, err := readFileRemote(ctx, minioClient, config.Source, config.Format, pool)
	if err != nil {
		log.Errorf("Failed to read source file from MinIO: %v", err)
		return
	}
	defer table.Release()

	var buf bytes.Buffer

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDataPageVersion(parquet.DataPageV1),
		parquet.WithVersion(parquet.V1_0),
		parquet.WithDictionaryDefault(true),
	)

	pqWriter, err := pqarrow.NewFileWriter(table.Schema(), &buf, writerProps, pqarrow.DefaultWriterProps())
	if err != nil {
		log.Errorf("Failed to create Parquet writer: %v", err)
		return
	}

	if err := pqWriter.WriteTable(table, table.NumRows()); err != nil {
		log.Errorf("Failed to write record to Parquet: %v", err)
		return
	}
	if err := pqWriter.Close(); err != nil {
		log.Errorf("Failed to close Parquet writer: %v", err)
		return
	}

	finalData := buf.Bytes()
	dataLen := int64(buf.Len())

	// generate partions
	partitionPath := createPathPartition(config)

	info, err := minioClient.PutObject(ctx, bucketName, partitionPath, bytes.NewReader(finalData), dataLen, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		log.Errorf("Failed to upload Parquet file to MinIO: %v", err)
		return
	}

	log.Infof("Successfully uploaded Parquet file to MinIO: %s (size: %d bytes)", info.Key, info.Size)
}

func readFileRemote(ctx context.Context, minioClient *minio.Client, objectName, format string, pool memory.Allocator) (arrow.Table, error) {
	var table arrow.Table
	object, err := minioClient.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from MinIO: %w", err)
	}
	defer object.Close()

	if format == "csv" {
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

		var records []arrow.Record
		for reader.Next() {
			rec := reader.Record()
			rec.Retain()
			records = append(records, rec)
		}

		if reader.Err() != nil {
			return nil, fmt.Errorf("error reading CSV file: %w", reader.Err())
		}

		if len(records) == 0 {
			return nil, fmt.Errorf("no records found in CSV file")
		}

		table = array.NewTableFromRecords(reader.Schema(), records)

		// free records
		for _, rec := range records {
			rec.Release()
		}
	} else if format == "parquet" {
		parquetReader, err := file.NewParquetReader(object)
		if err != nil {
			return nil, fmt.Errorf("Reader parquet err %v", err)
		}
		table = array.NewTableFromRecords(parquetReader.MetaData().Schema, parquetReader)

	}

	return table, nil
}

func createPathPartition(config TablesConfig) string {
	var builder strings.Builder
	// reservation memory string
	builder.Grow(len(config.Dest) + len(config.TableName) + (len(config.Partitions) * 30) + 45)

	builder.WriteString(config.Dest)
	builder.WriteByte('/')
	builder.WriteString(config.TableName)
	builder.WriteByte('/')

	for _, part := range config.Partitions {
		builder.WriteString(part.PartitionKey)
		builder.WriteByte('=')
		builder.WriteString(part.PartitionValue)
		builder.WriteByte('/')
	}
	builder.WriteString("part-")
	builder.WriteString(uuid.New().String())
	builder.WriteString(".parquet")
	return builder.String()
}

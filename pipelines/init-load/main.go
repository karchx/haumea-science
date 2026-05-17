package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
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
	endpoint := "minio.platform:9000"
	// endpoint := "localhost:9000"
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	useSSL := false

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		log.Fatalf("Failed to create MinIO client: %v", err)
		return
	}

	configData, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
		return
	}

	var tablesConfig []TablesConfig
	if err := json.Unmarshal(configData, &tablesConfig); err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
		return
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
	finalFile, err := readFileRemoteParquet(ctx, minioClient, config.Source, pool)
	defer os.Remove(finalFile)
	if err != nil {
		log.Errorf("Failed to read source file from MinIO: %v", err)
		return
	}
	// generate partions
	partitionPath := createPathPartition(config)

	info, err := minioClient.FPutObject(ctx, bucketName, partitionPath, finalFile, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		log.Errorf("Failed to upload Parquet file to MinIO: %v", err)
		return
	}

	log.Infof("Successfully uploaded Parquet file to MinIO: %s (size: %d bytes)", info.Key, info.Size)
}

func readFileRemote(ctx context.Context, minioClient *minio.Client, objectName string, pool memory.Allocator) (arrow.Table, error) {
	var table arrow.Table

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

	return table, nil
}

func readFileRemoteParquet(ctx context.Context, minioClient *minio.Client, objectName string, pool memory.Allocator) (string, error) {
	prefix := objectName + "/"
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}
	finalFile, err := os.CreateTemp("", "gaia-consolidated-*.parquet")
	if err != nil {
		return "", fmt.Errorf("Error create temp file %w", err)
	}
	defer finalFile.Close()

	var writer *pqarrow.FileWriter
	var schema *arrow.Schema

	for objectInfo := range minioClient.ListObjects(ctx, bucketName, opts) {
		if objectInfo.Err != nil {
			return "", fmt.Errorf("List objects err: %w", objectInfo.Err)
		}

		if objectInfo.Size == 0 || strings.HasSuffix(objectInfo.Key, "/") {
			continue
		}
		err := func() error {
			object, err := minioClient.GetObject(ctx, bucketName, objectInfo.Key, minio.GetObjectOptions{})
			if err != nil {
				return fmt.Errorf("failed to get object %s: %w", objectInfo.Key, err)
			}
			defer object.Close()
			log.Infof("Read object: %s", objectInfo.Key)

			tmpFile, err := os.CreateTemp("", "gaia-part-*.parquet")
			if err != nil {
				return fmt.Errorf("error creating temp file: %w", err)
			}
			defer os.Remove(tmpFile.Name())
			defer tmpFile.Close()

			if _, err := io.Copy(tmpFile, object); err != nil {
				return fmt.Errorf("error copying to temp disk: %w", err)
			}

			if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("error seeking temp file: %w", err)
			}

			parquetReader, err := file.NewParquetReader(tmpFile)
			if err != nil {
				return fmt.Errorf("Reader parquet err %v", err)
			}
			defer parquetReader.Close()

			numRowGroups := parquetReader.NumRowGroups()
			rowGroups := make([]int, numRowGroups)

			for i := range numRowGroups {
				rowGroups[i] = i
			}

			arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 10000}, pool)
			if err != nil {
				return fmt.Errorf("File reader pqarrow %v", err)
			}

			reader, err := arrowReader.GetRecordReader(ctx, nil, rowGroups)
			if err != nil {
				return fmt.Errorf("Get record %v", err)
			}

			defer reader.Release()

			for reader.Next() {
				rec := reader.Record()

				newCols := make([]arrow.Array, rec.NumCols())
				for i := 0; i < int(rec.NumCols()); i++ {
					col := rec.Column(i)
					if col.DataType().ID() == arrow.STRING {
						col.Retain()
						newCols[i] = col
					} else {
						datum := compute.NewDatum(col)
						castOptions := &compute.CastOptions{
							ToType: arrow.BinaryTypes.String,
						}
						castResult, err := compute.CallFunction(ctx, "cast", castOptions, datum)
						if err != nil {
							return fmt.Errorf("Error kernel cast column %s %w", rec.Schema().Field(i).Name, err)
						}

						newCols[i] = castResult.(*compute.ArrayDatum).MakeArray()
						castResult.Release()
						datum.Release()
					}
				}

				if schema == nil {
					fields := make([]arrow.Field, rec.Schema().NumFields())
					for i := 0; i < rec.Schema().NumFields(); i++ {
						f := rec.Schema().Field(i)
						fields[i] = arrow.Field{
							Name:     f.Name,
							Type:     arrow.BinaryTypes.String,
							Nullable: true,
						}
					}
					schema = arrow.NewSchema(fields, nil)

					writer, err = pqarrow.NewFileWriter(schema, finalFile, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
					if err != nil {
						return fmt.Errorf("error creating parquet writer: %w", err)
					}
				}

				bronzeRec := array.NewRecord(schema, newCols, rec.NumRows())

				if err := writer.Write(bronzeRec); err != nil {
					return fmt.Errorf("error writing record %w", err)
				}

				bronzeRec.Release()
				for _, col := range newCols {
					col.Release()
				}
			}

			if err := reader.Err(); err != nil && err != io.EOF {
				return fmt.Errorf("iter arrow reader %w", err)
			}
			return nil
		}()

		if err != nil {
			return "", err
		}
	}

	if writer != nil {
		if err := writer.Close(); err != nil {
			return "", fmt.Errorf("error closing parquet writer %w", err)
		}
	}

	return finalFile.Name(), nil
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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioCredentials struct {
	accessKey string
	secretKey string
	endpoint  string
}

type ResponseAPI struct {
	Strain []struct {
		Url string `json:"url"`
	} `json:"strain"`
}

func connectionMinio(creds MinioCredentials) (*minio.Client, error) {
	return minio.New(creds.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(creds.accessKey, creds.secretKey, ""),
		Secure: false,
	})
}

func streamToMinio(minioClient *minio.Client) {
	ctx := context.Background()

	startGps := 1126259446
	endGps := 1126259478
	apiUrl := fmt.Sprintf("https://gwosc.org/archive/links/O1/H1/%d/%d/json/", startGps, endGps)

	resp, err := http.Get(apiUrl)
	if err != nil {
		log.Fatalf("an error %v", err)
	}
	defer resp.Body.Close()

	var respJson ResponseAPI

	err = json.NewDecoder(resp.Body).Decode(&respJson)
	if err != nil {
		log.Fatalf("Error decoder json %v", err)
	}

	urlFile := respJson.Strain[0].Url
	respFile, err := http.Get(urlFile)
	if err != nil {
		log.Fatalf("an error get file%v", err)
	}
	defer respFile.Body.Close()

	info, err := minioClient.PutObject(
		ctx,
		"bucket",
		"gwosc_GW150914.hdf5",
		respFile.Body,
		respFile.ContentLength,
		minio.PutObjectOptions{
			ContentType: resp.Header.Get("Content-Type"),
		},
	)

	if err != nil {
		log.Fatalf("An error put object %v", err)
	}
	log.Printf("File upload successfully, size: %dMB", (info.Size / 1024 / 1024))
}

func main() {
	minioClient, err := connectionMinio(MinioCredentials{
		accessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		secretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		endpoint:  "minio.platform:9000",
	})

	if err != nil {
		log.Fatalf("Error connection minio %v", err)
	}

	streamToMinio(minioClient)
}

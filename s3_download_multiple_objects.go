package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Downloads multiple objects in the specified S3 Bucket
//
// Usage:
//    go run s3_download_multiple_objects.go Bucket Prefix LocalDirectory

var Bucket string
var Prefix string
var LocalDirectory string


func main() {
	if len(os.Args) != 4 {
        exitErrorf("Bucket, prefix and destination names required\nUsage: %s bucket_name item_name",
            os.Args[0])
    }

    Bucket := os.Args[1]
    Prefix := os.Args[2]
    LocalDirectory := os.Args[3]
    
    
    cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatalln("error:", err)
	}

	client := s3.NewFromConfig(cfg)
	manager := manager.NewDownloader(client)

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &Bucket,
		Prefix: &Prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Fatalln("error:", err)
		}
		for _, obj := range page.Contents {
			if err := downloadToFile(manager, LocalDirectory, Bucket, aws.ToString(obj.Key)); err != nil {
				log.Fatalln("error:", err)
			}
		}
	}
}

func downloadToFile(downloader *manager.Downloader, targetDirectory, bucket, key string) error {
	// Create the directories in the path
	file := filepath.Join(targetDirectory, key)
	if err := os.MkdirAll(filepath.Dir(file), 0775); err != nil {
		return err
	}

	// Set up the local file
	fd, err := os.Create(file)
	if err != nil {
		return err
	}
	defer fd.Close()

	// Download the file using the AWS SDK for Go
	fmt.Printf("Downloading s3://%s/%s to %s...\n", bucket, key, file)
	_, err = downloader.Download(context.TODO(), fd, &s3.GetObjectInput{Bucket: &bucket, Key: &key})

	return err
}

func exitErrorf(msg string, args ...interface{}) {
    fmt.Fprintf(os.Stderr, msg+"\n", args...)
    os.Exit(1)
}
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	bucket  = "plato-alexander"
	key     = "robots.txt"
	timeout = time.Minute
)

var (
	client     *s3.Client
	downloader *manager.Downloader
	err        error
	testFile   *os.File
	url        = fmt.Sprintf("s3://%s/%s", bucket, key)
)

func main() {
	testFile, err = os.CreateTemp("", "s3timer")
	if err != nil {
		panic("unable to create temp file")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-west-2"))
	if err != nil {
		panic(fmt.Sprintf("unable to load SDK config, %v", err))
	}

	client = s3.NewFromConfig(cfg)
	downloader = manager.NewDownloader(client)

	start := time.Now()
	fetchS3objectMetadata(ctx)
	fmt.Printf("time elapsed fetching %s metadata: %s\n", url, time.Since(start))

	start = time.Now()
	numBytes := downloadS3object(ctx)
	fmt.Printf("time elapsed fetching %s content (%d bytes): %s\n", url, numBytes, time.Since(start))
}

func downloadS3object(ctx context.Context) int64 {
	numBytes, err := downloader.Download(ctx, testFile, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		panic(err)
	}

	return numBytes
}

func fetchS3objectMetadata(ctx context.Context) {
	meta, err := client.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesChecksum,
			types.ObjectAttributesEtag,
		},
	})

	if err != nil {
		panic(err)
	}

	if meta.Checksum != nil {
		fmt.Printf("%s checksum: %s\n", url, *meta.Checksum.ChecksumSHA1)
	}
	if meta.ETag != nil {
		fmt.Printf("%s ETag: %s\n", url, *meta.ETag)
	}
	if meta.LastModified != nil {
		fmt.Printf("%s last modified: %s\n", url, *meta.LastModified)
	}
}

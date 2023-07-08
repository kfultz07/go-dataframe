package dataframe

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func CreateDataFrameFromAwsS3(path, item, bucket, region, awsAccessKey, awsSecretKey string) DataFrame {
	// Prechecks
	if strings.Contains(item, ".csv") != true {
		panic("AWS S3: only CSV files are currently supported.")
	}
	if len(path) == 0 {
		panic("AWS S3: you must provide a path")
	}
	if len(item) == 0 {
		panic("AWS S3: you must provide a file name")
	}
	if len(bucket) == 0 {
		panic("AWS S3: you must provide a bucket name")
	}
	if len(region) == 0 {
		panic("AWS S3: you must provide a region")
	}
	if len(awsAccessKey) == 0 {
		panic("AWS S3: you must provide an access key")
	}
	if len(awsSecretKey) == 0 {
		panic("AWS S3: you must provide a secret key")
	}

	// Set environment variables.
	os.Setenv("AWS_ACCESS_KEY", awsAccessKey)
	os.Setenv("AWS_SECRET_KEY", awsSecretKey)

	// Create path.
	filePath, _ := filepath.Abs(path + item)

	// Create file.
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("AWS S3: Error creating the file --> %v", err)
	}

	defer file.Close()

	// Initialize an AWS session.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		log.Fatalf("AWS S3: Error initializing session --> %v", err)
	}

	// Download file from AWS
	downloader := s3manager.NewDownloader(sess)

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})
	if err != nil {
		log.Fatalf("AWS S3: Error downloading the file --> %v", err)
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")

	df := CreateDataFrame(path, item)

	return df
}

func UploadFileToAwsS3(path, filename, bucket, region string) error {
	// Check user entries
	if path[len(path)-1:] != "/" {
		path = path + "/"
	}

	// Initialize an AWS session.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		log.Fatalf("AWS S3: Error initializing session --> %v", err)
	}

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

	f, err := os.Open(path + filename)
	if err != nil {
		return errors.New("Failed to open file.")
	}

	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
		Body:   f,
	})
	if err != nil {
		return errors.New("Failed to upload file to AWS S3")
	}
	fmt.Printf("File successfully uploaded to, %s\n", aws.StringValue(&result.Location))
	return nil
}

package dataframe

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func CreateDataFrameFromAwsS3(path, item, bucket, region, awsAccessKey, awsSecretKey string) (DataFrame, error) {
	switch {
	case !strings.Contains(item, ".csv"):
		return DataFrame{}, errors.New("create dataframe from aws s3: only csv files are currently supported")
	case len(path) == 0:
		return DataFrame{}, errors.New("create dataframe from aws s3: must provide a path")
	case len(item) == 0:
		return DataFrame{}, errors.New("create dataframe from aws s3: must provide a file name")
	case len(bucket) == 0:
		return DataFrame{}, errors.New("create dataframe from aws s3: must provide a bucket name")
	case len(region) == 0:
		return DataFrame{}, errors.New("create dataframe from aws s3: must provide a region")
	case len(awsAccessKey) == 0:
		return DataFrame{}, errors.New("create dataframe from aws s3: must provide an access key")
	case len(awsSecretKey) == 0:
		return DataFrame{}, errors.New("create dataframe from aws s3: must provide a secret key")
	}

	// Set environment variables.
	os.Setenv("AWS_ACCESS_KEY", awsAccessKey)
	os.Setenv("AWS_SECRET_KEY", awsSecretKey)

	// Create path.
	filePath, err := filepath.Abs(path + item)
	if err != nil {
		return DataFrame{}, err
	}

	// Create file.
	file, err := os.Create(filePath)
	if err != nil {
		return DataFrame{}, fmt.Errorf("create dataframe from aws s3: error creating the file '%s'", err)
	}
	defer file.Close()

	// Initialize an AWS session.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		return DataFrame{}, errors.New("create dataframe from aws s3: error initializing session")
	}

	// Download file from AWS
	downloader := s3manager.NewDownloader(sess)

	numBytes, err := downloader.Download(file, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(item)})
	if err != nil {
		return DataFrame{}, fmt.Errorf("create dataframe from aws s3: error downloading file '%s'", err)
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")

	df := CreateDataFrame(path, item)

	return df, nil
}

func UploadFileToAwsS3(path, filename, bucket, region string) error {
	// Check user entries
	if path[len(path)-1:] != "/" {
		path = path + "/"
	}

	// Initialize an AWS session.
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return fmt.Errorf("upload file to s3: error initializing session '%s'", err)
	}

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

	f, err := os.Open(path + filename)
	if err != nil {
		return errors.New("upload file to s3: failed to open file")
	}

	// Upload the file to S3.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
		Body:   f,
	})
	if err != nil {
		return errors.New("upload file to s3: failed to upload file to aws s3")
	}
	return nil
}

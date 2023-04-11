package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/s3", new(RootModule))
}

// RootModule is the global module object type. It is instantiated once per test
// run and will be used to create `k6/x/s3` module instances for each VU.
type RootModule struct{}

// S3 represents an instance of the S3 module for every VU.
type S3 struct {
	vu modules.VU
}

// Ensure the interfaces are implemented correctly.
var (
	_ modules.Module   = &RootModule{}
	_ modules.Instance = &S3{}
)

// NewModuleInstance implements the modules.Module interface to return
// a new instance for each VU.
func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &S3{vu: vu}
}

// Exports implements the modules.Instance interface and returns the exports
// of the JS module.
func (s3 *S3) Exports() modules.Exports {
	return modules.Exports{Default: s3}
}

// Generates a random byte buffer of the given size.
func (*S3) RandomData(size int64) []byte {
	buf := make([]byte, size)
	_, err := rand.Read(buf)
	if err != nil {
		log.Printf("Unable to generate random byte buffer: %v\n", err)
		return nil
	}
	return buf
}

// Creates a new S3 client from the given configuration.
func (*S3) Create(accessKey, secretKey, endpoint, region string) (*s3.Client, error) {
	creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: endpoint,
		}, nil
	})
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(creds),
		config.WithEndpointResolverWithOptions(customResolver))
	if err != nil {
		log.Printf("Unable to load config: %v\n", err)
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	}), nil
}

// Uploads the given file to the S3 bucket with the given key.
func (*S3) UploadFile(client *s3.Client, bucketName, objectKey, fileName string) error {
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("Unable to open file %v to upload: %v\n", fileName, err)
	} else {
		defer file.Close()
		_, err := client.PutObject(context.Background(),
			&s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
				Body:   file,
			}, s3.WithAPIOptions(
				v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
			))
		if err != nil {
			log.Printf("Unable to upload file %v to %v/%v: %v\n", fileName, bucketName, objectKey, err)
		}
	}
	return err
}

// Multipart upload of the given large file to the S3 bucket with the given key.
func (*S3) UploadLargeFile(client *s3.Client, bucketName, objectKey, fileName string, partSize int64, concurrency int) error {
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("Unable to open large file %v to upload: %v\n", fileName, err)
	} else {
		defer file.Close()
		uploader := manager.NewUploader(client, func(u *manager.Uploader) {
			u.PartSize = partSize
			u.Concurrency = concurrency
			u.ClientOptions = append(u.ClientOptions, s3.WithAPIOptions(
				v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
			))
		})
		_, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   file,
		})
		if err != nil {
			log.Printf("Unable to upload large file %v to %v/%v: %v\n", fileName, bucketName, objectKey, err)
		}
	}
	return err
}

// Uploads the given byte data to the S3 bucket with the given key.
func (*S3) UploadData(client *s3.Client, bucketName, objectKey string, data []byte) error {
	_, err := client.PutObject(context.Background(),
		&s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(data),
		}, s3.WithAPIOptions(
			v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
		))
	if err != nil {
		log.Printf("Unable to upload bytes to %v/%v: %v\n", bucketName, objectKey, err)
	}
	return err
}

// Downloads the given byte data from the S3 bucket with the given key.
func (*S3) DownloadDataRange(client *s3.Client, bucketName, objectKey string, begin, end int) ([]byte, error) {
	result, err := client.GetObject(context.Background(),
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", begin, end)),
		})
	if err != nil {
		log.Printf("Unable to download bytes from %v/%v: %v\n", bucketName, objectKey, err)
		return nil, err
	}
	bytes, err := io.ReadAll(result.Body)
	if err != nil {
		log.Printf("Unable to read bytes from %v/%v: %v\n", bucketName, objectKey, err)
		return nil, err
	}
	return bytes, err
}

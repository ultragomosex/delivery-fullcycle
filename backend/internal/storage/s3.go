package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Client struct {
	rawClient *minio.Client
	bucket    string
}

func New(endpoint, accessKey, secretKey, defaultBucket string, useSSL bool) (*Client, error) {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("init s3 client: %w", err)
	}

	return &Client{
		rawClient: minioClient,
		bucket:    defaultBucket,
	}, nil
}

func (c *Client) EnsureBucket(ctx context.Context, bucket string) error {
	if bucket == "" {
		bucket = c.bucket
	}

	exists, err := c.rawClient.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("check bucket exists: %w", err)
	}

	if !exists {
		err = c.rawClient.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}
	}

	return nil
}

func (c *Client) Upload(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) error {
	if bucket == "" {
		bucket = c.bucket
	}

	_, err := c.rawClient.PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("put s3 object: %w", err)
	}

	return nil
}

func (c *Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, string, int64, error) {
	if bucket == "" {
		bucket = c.bucket
	}

	obj, err := c.rawClient.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", 0, fmt.Errorf("get s3 object: %w", err)
	}

	stat, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		return nil, "", 0, fmt.Errorf("stat s3 object: %w", err)
	}

	contentType := stat.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return obj, contentType, stat.Size, nil
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.rawClient.BucketExists(ctx, c.bucket)
	if err != nil {
		return fmt.Errorf("s3 ping failed: %w", err)
	}
	return nil
}

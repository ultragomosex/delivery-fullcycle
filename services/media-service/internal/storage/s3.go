package storage

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 32*1024)
		return &b
	},
}

type S3Client struct {
	raw    *minio.Client
	bucket string
}

func New(endpoint, accessKey, secretKey, defaultBucket string, useSSL bool) (*S3Client, error) {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	raw, err := minio.New(endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:    useSSL,
		Transport: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("init s3 client: %w", err)
	}

	return &S3Client{
		raw:    raw,
		bucket: defaultBucket,
	}, nil
}

func (s *S3Client) EnsureBucket(ctx context.Context, bucket string) error {
	if bucket == "" {
		bucket = s.bucket
	}

	exists, err := s.raw.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("check bucket exists: %w", err)
	}

	if !exists {
		err = s.raw.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}
	}

	return nil
}

func (s *S3Client) Upload(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) error {
	if bucket == "" {
		bucket = s.bucket
	}

	_, err := s.raw.PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("put s3 object: %w", err)
	}

	return nil
}

func (s *S3Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, string, int64, time.Time, string, error) {
	if bucket == "" {
		bucket = s.bucket
	}

	obj, err := s.raw.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", 0, time.Time{}, "", fmt.Errorf("get s3 object: %w", err)
	}

	stat, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		return nil, "", 0, time.Time{}, "", fmt.Errorf("stat s3 object: %w", err)
	}

	contentType := stat.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return obj, contentType, stat.Size, stat.LastModified, stat.ETag, nil
}

func (s *S3Client) StreamCopy(dst io.Writer, src io.Reader) (int64, error) {
	bufPtr := bufferPool.Get().(*[]byte)
	defer bufferPool.Put(bufPtr)

	return io.CopyBuffer(dst, src, *bufPtr)
}

func (s *S3Client) Ping(ctx context.Context) error {
	_, err := s.raw.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("s3 ping failed: %w", err)
	}
	return nil
}

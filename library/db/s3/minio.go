/**
 *@Author: pangj
 *@Description:
 *@File: minio
 *@Version:
 *@Date: 2022/05/07/17:25
 */

package s3

import (
	"context"
	"errors"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"log"
	"time"
)

type MinioClient struct {
	Client *minio.Client
	Bucket string
	Config *MinioConfig
}

// Config Minio Config .
type MinioConfig struct {
	Endpoint        string `check:"required"`
	AccessKey       string `check:"required"`
	SecretKey       string `check:"required"`
	Bucket          string `check:"required"`
	UseSSL          bool
	ConnMaxLifetime time.Duration
}

func (mc *MinioClient) Close() error {
	return nil
}

// NewMinioClient 初始化minio client
func NewMinioClient(conf *MinioConfig) (*MinioClient, error) {
	minioClient, err := minio.New(conf.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(conf.AccessKey, conf.SecretKey, ""),
		Secure: conf.UseSSL,
	})
	if err != nil {
		return nil, err
	}
	return &MinioClient{Client: minioClient, Bucket: conf.Bucket}, nil
}

func (mc *MinioClient) NewBucket(ctx context.Context, bucketName, region string) error {
	if region == "" {
		region = "cn-north-1"
	}
	err := mc.Client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{
		Region:        region,
		ObjectLocking: true,
	})
	if err != nil {
		// 检查存储桶是否已经存在。
		exists, err := mc.Client.BucketExists(ctx, bucketName)
		if err != nil {
			return err
		}
		if exists {
			log.Printf("We already own %s\n", bucketName)
			return errors.New(fmt.Sprintf("%s bucket existed", bucketName))
		}
	}
	return nil
}

func (mc *MinioClient) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	// 检查存储桶是否已经存在。
	exists, err := mc.Client.BucketExists(ctx, bucketName)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	return false, nil
}

func (mc *MinioClient) RemoveBucket(ctx context.Context, bucketName string) error {
	err := mc.Client.RemoveBucket(ctx, bucketName)
	if err != nil {
		return err
	}
	return nil
}

func (mc *MinioClient) PutObject(ctx context.Context, bucketName, objectName string, file io.Reader, size int64) error {
	_, err := mc.Client.PutObject(ctx, bucketName, objectName, file, size, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (mc *MinioClient) FPutObject(ctx context.Context, bucketName, objectName string, filePath string) error {
	_, err := mc.Client.FPutObject(ctx, bucketName, objectName, filePath, minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (mc *MinioClient) GetObject(ctx context.Context, bucketName, objectName string) (reader io.Reader, err error) {
	object, err := mc.Client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (mc *MinioClient) FGetObject(ctx context.Context, bucketName, objectName string, filePath string) error {

	return mc.Client.FGetObject(ctx, bucketName, objectName, filePath, minio.GetObjectOptions{})
}

func (mc *MinioClient) RemoveObject(ctx context.Context, bucketName, objectName string) error {
	return mc.Client.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})

}

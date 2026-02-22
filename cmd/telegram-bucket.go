package cmd

import (
	"context"
	"database/sql"
	"os"
	"time"

	"github.com/minio/minio/internal/bucket/versioning"
)

// MakeBucket creates a new bucket in the Telegram backend
func (t *TelegramObjectLayer) MakeBucket(ctx context.Context, bucket string, opts MakeBucketOptions) error {
	// Intercept system buckets to local disk
	if t.isSystemBucket(bucket) {
		return os.MkdirAll(t.getLocalPath(bucket, ""), 0755)
	}

	_, err := t.db.ExecContext(ctx, "INSERT INTO buckets (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", bucket)
	if err != nil {
		return err
	}
	return nil
}

// GetBucketInfo returns bucket metadata
func (t *TelegramObjectLayer) GetBucketInfo(ctx context.Context, bucket string, opts BucketOptions) (BucketInfo, error) {
	// Intercept system buckets to local disk
	if t.isSystemBucket(bucket) {
		fi, err := os.Stat(t.getLocalPath(bucket, ""))
		if err != nil {
			return BucketInfo{}, BucketNotFound{Bucket: bucket}
		}
		return BucketInfo{
			Name:    bucket,
			Created: fi.ModTime(),
		}, nil
	}

	var created time.Time
	err := t.db.QueryRowContext(ctx, "SELECT created_at FROM buckets WHERE name = $1", bucket).Scan(&created)
	if err == sql.ErrNoRows {
		return BucketInfo{}, BucketNotFound{Bucket: bucket}
	} else if err != nil {
		return BucketInfo{}, err
	}

	return BucketInfo{
		Name:    bucket,
		Created: created,
	}, nil
}

// ListBuckets lists all buckets
func (t *TelegramObjectLayer) ListBuckets(ctx context.Context, opts BucketOptions) ([]BucketInfo, error) {
	// We only want to list real user buckets from PostgreSQL
	// MinIO system buckets are meant to be hidden from the S3 list API anyway
	rows, err := t.db.QueryContext(ctx, "SELECT name, created_at FROM buckets")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var bucketInfos []BucketInfo
	for rows.Next() {
		var name string
		var created time.Time
		if err := rows.Scan(&name, &created); err != nil {
			continue
		}
		bucketInfos = append(bucketInfos, BucketInfo{
			Name:    name,
			Created: created,
		})
	}
	return bucketInfos, nil
}

// DeleteBucket deletes a bucket if empty
func (t *TelegramObjectLayer) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	// Intercept system buckets to local disk
	if t.isSystemBucket(bucket) {
		return os.RemoveAll(t.getLocalPath(bucket, ""))
	}

	// Check if bucket is empty
	var count int
	err := t.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM objects WHERE bucket = $1", bucket).Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 && !opts.Force {
		return BucketNotEmpty{Bucket: bucket}
	}

	// Delete bucket
	res, err := t.db.ExecContext(ctx, "DELETE FROM buckets WHERE name = $1", bucket)
	if err != nil {
		return err
	}

	rowsAffected, _ := res.RowsAffected()
	if rowsAffected == 0 {
		return BucketNotFound{Bucket: bucket}
	}

	// If force delete, cleanup objects
	if opts.Force {
		_, _ = t.db.ExecContext(ctx, "DELETE FROM objects WHERE bucket = $1", bucket)
	}

	return nil
}

// Stubs/No-ops for additional bucket operations

// GetBucketVersioning returns the bucket versioning configuration
func (t *TelegramObjectLayer) GetBucketVersioning(ctx context.Context, bucket string) (*versioning.Versioning, error) {
	return nil, NotImplemented{}
}

// GetBucketObjectLockConfig returns the object lock configuration for a bucket
func (t *TelegramObjectLayer) GetBucketObjectLockConfig(ctx context.Context, bucket string) (interface{}, error) {
	return nil, NotImplemented{}
}

// GetBucketQuota returns the bucket quota configuration
func (t *TelegramObjectLayer) GetBucketQuota(ctx context.Context, bucket string) (interface{}, error) {
	return nil, NotImplemented{}
}

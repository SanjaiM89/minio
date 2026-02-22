package cmd

import "context"

func (t *TelegramObjectLayer) MakeBucket(ctx context.Context, bucket string, opts MakeBucketOptions) error {
	if t.isSystemBucket(bucket) {
		return t.base.MakeBucket(ctx, bucket, opts)
	}

	// Create bucket natively so WebUI recognizes it
	if err := t.base.MakeBucket(ctx, bucket, opts); err != nil {
		return err
	}
	// Track it in Postgres
	_, err := t.db.ExecContext(ctx, "INSERT INTO buckets (name) VALUES ($1) ON CONFLICT DO NOTHING", bucket)
	return err
}

func (t *TelegramObjectLayer) DeleteBucket(ctx context.Context, bucket string, opts DeleteBucketOptions) error {
	if t.isSystemBucket(bucket) {
		return t.base.DeleteBucket(ctx, bucket, opts)
	}

	if err := t.base.DeleteBucket(ctx, bucket, opts); err != nil {
		return err
	}
	t.db.ExecContext(ctx, "DELETE FROM buckets WHERE name = $1", bucket)
	return nil
}

// Notice how we removed ListBuckets and GetBucketInfo?
// The embedded native layer will perfectly handle them for us!

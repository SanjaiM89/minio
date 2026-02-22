package cmd

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7/pkg/tags"
)

// TelegramObjectLayer implements ObjectLayer interface using Telegram as backend
type TelegramObjectLayer struct {
	tgClient          *telegram.Client
	db                *sql.DB
	config            *TelegramConfig
	channelAccessHash int64 // resolved once at startup
}

// NewTelegramObjectLayer initializes the Telegram object layer
func NewTelegramObjectLayer(ctx context.Context) (ObjectLayer, error) {
	cfg := LoadTelegramConfig()
	if !cfg.TelegramEnabled {
		return nil, fmt.Errorf("telegram backend is not enabled")
	}

	db, err := sql.Open("postgres", cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	// Initialize Schema
	schema := `
	CREATE TABLE IF NOT EXISTS buckets (
		name TEXT PRIMARY KEY,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE IF NOT EXISTS objects (
		bucket TEXT NOT NULL,
		key TEXT NOT NULL,
		metadata JSONB,
		PRIMARY KEY (bucket, key)
	);
	`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	opts := telegram.Options{}

	// Setup Proxy if configured
	if cfg.ProxyURL != "" {
		u, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proxy URL: %w", err)
		}

		q := u.Query()
		server := q.Get("server")
		port := q.Get("port")
		secretHex := q.Get("secret")

		if server != "" && port != "" && secretHex != "" {
			secret, err := hex.DecodeString(secretHex)
			if err != nil {
				return nil, fmt.Errorf("failed to decode proxy secret: %w", err)
			}

			addr := net.JoinHostPort(server, port)
			resolver, err := dcs.MTProxy(addr, secret, dcs.MTProxyOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to create MTProxy resolver: %w", err)
			}
			opts.Resolver = resolver
			fmt.Printf("Using MTProto proxy: %s\n", addr)
		}
	}

	// Initialize Telegram client
	client := telegram.NewClient(cfg.AppID, cfg.AppHash, opts)

	// Start client in background (simplified for this layer)
	// MinIO needs synchronous client availability, but Telegram login is async.
	// We'll start a goroutine to keep the client running, and wait for auth.

	// Note: Ideally, we should manage this lifecycle better.
	// For this PoC, we will try to auth immediately.

	// Creating a detached context for the background client loop
	bgCtx := context.Background()

	// ready carries: (accessHash int64, error)
	type readyMsg struct {
		accessHash int64
		err        error
	}
	ready := make(chan readyMsg, 1)

	go func() {
		err := client.Run(bgCtx, func(ctx context.Context) error {
			// Authenticate as Bot
			if _, err := client.Auth().Bot(ctx, cfg.BotToken); err != nil {
				return fmt.Errorf("auth failed: %w", err)
			}

			// Resolve channel access hash once
			api := client.API()
			chResult, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
				&tg.InputChannel{ChannelID: cfg.BareChannelID, AccessHash: 0},
			})
			var accessHash int64
			if err == nil {
				if chats, ok := chResult.(*tg.MessagesChats); ok && len(chats.Chats) > 0 {
					if ch, ok := chats.Chats[0].(*tg.Channel); ok {
						accessHash = ch.AccessHash
					}
				}
			}
			// Even if we can't resolve the hash, we still signal ready.
			// Uploads will fail gracefully later if hash is 0.
			ready <- readyMsg{accessHash: accessHash}

			// Block until context canceled
			<-ctx.Done()
			return ctx.Err()
		})
		if err != nil {
			ready <- readyMsg{err: err}
			fmt.Printf("Telegram client error: %v\n", err)
		}
	}()

	// Wait for readiness (timeout after 30s)
	var channelAccessHash int64
	select {
	case msg := <-ready:
		if msg.err != nil {
			return nil, msg.err
		}
		channelAccessHash = msg.accessHash
	case <-time.After(30 * time.Second):
		fmt.Println("Warning: Telegram client not ready in 30s")
	}

	// Initialize the global leader lock so subsystems like the data-scanner
	// don't panic with a nil pointer dereference when calling GetLock.
	layer := &TelegramObjectLayer{
		db:                db,
		config:            cfg,
		tgClient:          client,
		channelAccessHash: channelAccessHash,
	}
	if globalLeaderLock == nil {
		globalLeaderLock = newSharedLock(GlobalContext, layer, "leader.lock")
	}

	// Register the object layer globally so health checks pass
	setObjectLayer(layer)

	return layer, nil
}

// Shutdown saves any in-progress state and stops the backend
func (t *TelegramObjectLayer) Shutdown(ctx context.Context) error {
	return t.db.Close()
}

// StorageInfo returns underlying storage statistics
func (t *TelegramObjectLayer) StorageInfo(ctx context.Context, metrics bool) StorageInfo {
	return StorageInfo{
		Disks: []madmin.Disk{
			{
				State:     madmin.DriveStateOk,
				PoolIndex: 0,
				SetIndex:  0,
				DiskIndex: 0,
			},
		},
		Backend: madmin.BackendInfo{
			Type: madmin.Erasure,
		},
	}
}

// BackendInfo returns the backend type
func (t *TelegramObjectLayer) BackendInfo() madmin.BackendInfo {
	return madmin.BackendInfo{
		Type: madmin.Erasure,
	}
}

// LocalStorageInfo returns local storage info (same as StorageInfo for now)
func (t *TelegramObjectLayer) LocalStorageInfo(ctx context.Context, metrics bool) StorageInfo {
	return t.StorageInfo(ctx, metrics)
}

// Legacy returns false as this is a new backend
func (t *TelegramObjectLayer) Legacy() bool {
	return false
}

// Operations to be implemented in other files:

// NSScanner is a no-op for now
func (t *TelegramObjectLayer) NSScanner(ctx context.Context, updates chan<- DataUsageInfo, wantCycle uint32, scanMode madmin.HealScanMode) error {
	return nil // No-op for now
}

// Stubs for required interface methods to make it compile
// (We will move these to respective files as we implement them)

// Re-adding dsync import and proper implementation

// NewNSLock returns a new namespace lock
func (t *TelegramObjectLayer) NewNSLock(bucket string, objects ...string) RWLocker {
	return &noOpLocker{}
}

type noOpLocker struct{}

func (n *noOpLocker) GetLock(ctx context.Context, timeout *dynamicTimeout) (LockContext, error) {
	return LockContext{ctx: ctx, cancel: func() {}}, nil
}

func (n *noOpLocker) Unlock(lkCtx LockContext) {
	lkCtx.Cancel()
}

func (n *noOpLocker) GetRLock(ctx context.Context, timeout *dynamicTimeout) (LockContext, error) {
	return LockContext{ctx: ctx, cancel: func() {}}, nil
}

func (n *noOpLocker) RUnlock(lkCtx LockContext) {
	lkCtx.Cancel()
}

func (n *noOpLocker) String() string { return "noop" }
func (n *noOpLocker) IsClosed() bool { return false }
func (n *noOpLocker) Close() error   { return nil }
func (n *noOpLocker) IncLockRef()    {}
func (n *noOpLocker) DecLockRef()    {}

// SetDriveCounts returns the drive counts for the backend
func (t *TelegramObjectLayer) SetDriveCounts() []int {
	return []int{1}
}

// GetDisks returns the disks for the backend
func (t *TelegramObjectLayer) GetDisks(poolIdx, setIdx int) ([]StorageAPI, error) {
	return nil, nil
}

// HealFormat heals the format of the backend
func (t *TelegramObjectLayer) HealFormat(ctx context.Context, dryRun bool) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, nil
}

// HealBucket heals a bucket
func (t *TelegramObjectLayer) HealBucket(ctx context.Context, bucket string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, nil
}

// HealObject heals an object
func (t *TelegramObjectLayer) HealObject(ctx context.Context, bucket, object, versionID string, opts madmin.HealOpts) (madmin.HealResultItem, error) {
	return madmin.HealResultItem{}, nil
}

// HealObjects heals objects in a bucket
func (t *TelegramObjectLayer) HealObjects(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn HealObjectFn) error {
	return nil
}

// CheckAbandonedParts checks for abandoned parts
func (t *TelegramObjectLayer) CheckAbandonedParts(ctx context.Context, bucket, object string, opts madmin.HealOpts) error {
	return nil
}

// Health returns the health of the backend
func (t *TelegramObjectLayer) Health(ctx context.Context, opts HealthOptions) HealthResult {
	return HealthResult{HealthyRead: true}
}

// PutObjectMetadata updates object metadata
func (t *TelegramObjectLayer) PutObjectMetadata(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, NotImplemented{}
}

// DecomTieredObject decomposes a tiered object
func (t *TelegramObjectLayer) DecomTieredObject(ctx context.Context, bucket, object string, fi FileInfo, opts ObjectOptions) error {
	return NotImplemented{}
}

// PutObjectTags updates object tags
func (t *TelegramObjectLayer) PutObjectTags(ctx context.Context, bucket, object string, tags string, opts ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, NotImplemented{}
}

// GetObjectTags returns object tags
func (t *TelegramObjectLayer) GetObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (*tags.Tags, error) {
	return nil, NotImplemented{}
}

// DeleteObjectTags deletes object tags
func (t *TelegramObjectLayer) DeleteObjectTags(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, NotImplemented{}
}

// TransitionObject transitions an object to a different tier
func (t *TelegramObjectLayer) TransitionObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return NotImplemented{}
}

// RestoreTransitionedObject restores a transitioned object
func (t *TelegramObjectLayer) RestoreTransitionedObject(ctx context.Context, bucket, object string, opts ObjectOptions) error {
	return NotImplemented{}
}

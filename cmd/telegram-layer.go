package cmd

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	_ "github.com/lib/pq"
)

const (
	numUploadWorkers    = 4   // Number of concurrent upload workers
	uploadQueueCapacity = 100 // Capacity of the upload job queue
)

// uploadJob represents a single file chunk to be uploaded to Telegram.
type uploadJob struct {
	ctx        context.Context
	partName   string
	reader     io.Reader // Changed from io.ReaderAt
	size       int64
	resultChan chan<- uploadResult
}

// uploadResult contains the result of an upload job.
type uploadResult struct {
	msgID int
	err   error
}

// TelegramObjectLayer implements ObjectLayer interface using Telegram as backend
type TelegramObjectLayer struct {
	ObjectLayer       // Embeds ALL native MinIO methods automatically!
	base              ObjectLayer // Reference to native layer for delegation
	tgClient          *telegram.Client
	db                *sql.DB
	config            *TelegramConfig
	channelAccessHash int64
	hashMu            sync.RWMutex
	readyCh           chan struct{}
	uploadQueue       chan uploadJob
}

// uploadWorker is a background worker that processes upload jobs from the queue.
func (t *TelegramObjectLayer) uploadWorker() {
	// Each worker has its own uploader and sender instance.
	u := uploader.NewUploader(t.tgClient.API()).WithThreads(4) // 4 threads per worker
	sender := message.NewSender(t.tgClient.API()).WithUploader(u)

	// The target channel for uploads is stable, so we can get it once.
	t.hashMu.RLock()
	target := sender.To(&tg.InputPeerChannel{
		ChannelID:  t.config.BareChannelID,
		AccessHash: t.channelAccessHash,
	})
	t.hashMu.RUnlock()

	for job := range t.uploadQueue {
		upload, err := u.Upload(job.ctx, uploader.NewUpload(job.partName, job.reader, job.size))
		if err != nil {
			job.resultChan <- uploadResult{err: fmt.Errorf("worker upload failed: %w", err)}
			continue
		}

		var msgUpdates tg.UpdatesClass
		var lastErr error

		// Retry loop for sending the file message. The worker sleeps on FloodWait,
		// not the main API request goroutine.
		for retries := 0; retries < 15; retries++ {
			msgUpdates, lastErr = target.File(job.ctx, upload)
			if lastErr == nil {
				break
			}
			if d, ok := tgerr.AsFloodWait(lastErr); ok {
				time.Sleep(d + time.Second)
				continue
			}
			// Optional: shorter sleep for other transient errors
			if retries < 3 {
				time.Sleep(time.Duration(retries+1) * 2 * time.Second)
				continue
			}
			break
		}

		if lastErr != nil {
			job.resultChan <- uploadResult{err: fmt.Errorf("worker send failed after retries: %w", lastErr)}
			continue
		}

		// Extract Message ID from the response
		msgID := 0
		switch upds := msgUpdates.(type) {
		case *tg.Updates:
			for _, upd := range upds.Updates {
				switch update := upd.(type) {
				case *tg.UpdateNewMessage:
					if m, ok := update.Message.(*tg.Message); ok {
						msgID = m.ID
					}
				case *tg.UpdateNewChannelMessage:
					if m, ok := update.Message.(*tg.Message); ok {
						msgID = m.ID
					}
				}
				if msgID != 0 {
					break
				}
			}
		case *tg.UpdateShortSentMessage:
			msgID = upds.ID
		}

		if msgID == 0 {
			job.resultChan <- uploadResult{err: fmt.Errorf("worker failed to extract message ID")}
			continue
		}

		job.resultChan <- uploadResult{msgID: msgID}
	}
}

func (t *TelegramObjectLayer) isSystemBucket(bucket string) bool {
	return bucket == minioMetaBucket || bucket == minioMetaMultipartBucket || bucket == ".minio.sys"
}

func (t *TelegramObjectLayer) tgReady(ctx context.Context) error {
	select {
	case <-t.readyCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("telegram client not ready: %w", ctx.Err())
	}
}

// NewTelegramObjectLayer initializes the Telegram object wrapper
func NewTelegramObjectLayer(ctx context.Context, base ObjectLayer) (ObjectLayer, error) {
	cfg := LoadTelegramConfig()

	db, err := sql.Open("postgres", cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS buckets (name TEXT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
	CREATE TABLE IF NOT EXISTS objects (bucket TEXT NOT NULL, key TEXT NOT NULL, metadata JSONB, data BYTEA, PRIMARY KEY (bucket, key));
	`
	db.ExecContext(ctx, schema)
	db.ExecContext(ctx, "ALTER TABLE objects ADD COLUMN IF NOT EXISTS data BYTEA")

	opts := telegram.Options{}
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
				return nil, fmt.Errorf("invalid proxy secret, must be a valid hex string (got '%s'): %w", secretHex, err)
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

	client := telegram.NewClient(cfg.AppID, cfg.AppHash, opts)
	bgCtx := context.Background()

	layer := &TelegramObjectLayer{
		ObjectLayer: base, // Expose all native MinIO functions to the WebUI!
		base:        base,
		db:          db,
		config:      cfg,
		tgClient:    client,
		readyCh:     make(chan struct{}),
		uploadQueue: make(chan uploadJob, uploadQueueCapacity),
	}

	go func() {
		client.Run(bgCtx, func(ctx context.Context) error {
			client.Auth().Bot(ctx, cfg.BotToken)

			api := client.API()
			chResult, _ := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
				&tg.InputChannel{ChannelID: cfg.BareChannelID, AccessHash: 0},
			})
			if chats, ok := chResult.(*tg.MessagesChats); ok && len(chats.Chats) > 0 {
				if ch, ok := chats.Chats[0].(*tg.Channel); ok {
					layer.hashMu.Lock()
					layer.channelAccessHash = ch.AccessHash
					layer.hashMu.Unlock()
				}
			}

			close(layer.readyCh)
			fmt.Printf("Telegram client ready (channelAccessHash=%d)\n", layer.channelAccessHash)

			// Wait until the client is ready to start the upload workers
			// because they need the client and access hash.
			for i := 0; i < numUploadWorkers; i++ {
				go layer.uploadWorker()
			}

			<-ctx.Done()
			return ctx.Err()
		})
	}()

	setObjectLayer(layer)
	return layer, nil
}

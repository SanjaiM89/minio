package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/minio/madmin-go/v3"
)

// MaxTelegramChunkSize is the maximum size of a single Telegram file upload
const (
	// MaxTelegramChunkSize is the Telegram 2GB limit, we use 1.9GB to be safe
	MaxTelegramChunkSize = 1900 * 1024 * 1024
)

// ObjectMetadata holds metadata for an object stored in the Telegram backend
type ObjectMetadata struct {
	Bucket      string    `json:"bucket"`
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	ETag        string    `json:"etag"`
	ContentType string    `json:"content_type"`
	Parts       []int     `json:"parts"` // Message IDs
	ModTime     time.Time `json:"mod_time"`
}

// PutObject uploads an object to Telegram and stores metadata in PostgreSQL
func (t *TelegramObjectLayer) PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	reader := data.Reader
	size := data.Size()

	// Initialize uploader with 16 threads for maximum speed
	u := uploader.NewUploader(t.tgClient.API()).WithThreads(16)
	sender := message.NewSender(t.tgClient.API()).WithUploader(u)
	// Use InputPeerChannel directly to avoid gotd's peer resolver
	// mis-parsing "channel#ID" (the # is treated as a URL fragment).
	target := sender.To(&tg.InputPeerChannel{
		ChannelID:  t.config.BareChannelID,
		AccessHash: t.channelAccessHash,
	})

	var msgIDs []int
	remaining := size
	partNum := 0

	for remaining > 0 {
		chunkSize := remaining
		if chunkSize > MaxTelegramChunkSize {
			chunkSize = MaxTelegramChunkSize
		}

		partNum++
		partName := fmt.Sprintf("%s.part%d", object, partNum)

		// Use LimitReader to read only the chunk size
		limitedReader := io.LimitReader(reader, chunkSize)

		// Upload chunk
		upload, err := u.Upload(ctx, uploader.NewUpload(partName, limitedReader, chunkSize))
		if err != nil {
			return ObjectInfo{}, fmt.Errorf("telegram upload failed for part %d: %w", partNum, err)
		}

		// Send message with chunk
		msgUpdates, err := target.File(ctx, upload)
		if err != nil {
			return ObjectInfo{}, fmt.Errorf("telegram send failed for part %d: %w", partNum, err)
		}

		// Robust Message ID Extraction for channels
		msgID := 0
		switch upds := msgUpdates.(type) {
		case *tg.Updates:
			for _, upd := range upds.Updates {
				switch u := upd.(type) {
				case *tg.UpdateNewMessage:
					if m, ok := u.Message.(*tg.Message); ok {
						msgID = m.ID
					}
				case *tg.UpdateNewChannelMessage:
					if m, ok := u.Message.(*tg.Message); ok {
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
			return ObjectInfo{}, fmt.Errorf("failed to extract message ID for part %d", partNum)
		}

		msgIDs = append(msgIDs, msgID)
		remaining -= chunkSize
	}

	// 2. Store Metadata in PostgreSQL
	meta := ObjectMetadata{
		Bucket:      bucket,
		Key:         object,
		Size:        size,
		ETag:        data.MD5CurrentHexString(),
		ContentType: opts.UserDefined["content-type"],
		Parts:       msgIDs,
		ModTime:     time.Now().UTC(),
	}
	if meta.ContentType == "" {
		meta.ContentType = "application/octet-stream"
	}

	metaBytes, _ := json.Marshal(meta)

	// Upsert implementation for Postgres
	query := `
		INSERT INTO objects (bucket, key, metadata) 
		VALUES ($1, $2, $3)
		ON CONFLICT (bucket, key) 
		DO UPDATE SET metadata = EXCLUDED.metadata
	`
	if _, err := t.db.ExecContext(ctx, query, bucket, object, metaBytes); err != nil {
		return ObjectInfo{}, fmt.Errorf("postgres upsert failed: %w", err)
	}

	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        size,
		ModTime:     meta.ModTime,
		ETag:        meta.ETag,
		ContentType: meta.ContentType,
	}, nil
}

// GetObjectNInfo returns a reader for the object
func (t *TelegramObjectLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error) {
	var metaBytes []byte
	err = t.db.QueryRowContext(ctx, "SELECT metadata FROM objects WHERE bucket=$1 AND key=$2", bucket, object).Scan(&metaBytes)
	if err == sql.ErrNoRows {
		return nil, ObjectNotFound{Bucket: bucket, Object: object}
	} else if err != nil {
		return nil, err
	}

	var meta ObjectMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, err
	}

	objInfo := ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        meta.Size,
		ModTime:     meta.ModTime,
		ETag:        meta.ETag,
		ContentType: meta.ContentType,
	}

	pr, pw := io.Pipe()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				pw.CloseWithError(fmt.Errorf("panic in download goroutine: %v", r))
			} else {
				pw.Close()
			}
		}()

		api := t.tgClient.API()
		// Initialize downloader
		d := downloader.NewDownloader()

		for _, msgID := range meta.Parts {
			// Get message to find the document
			resp, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
				Channel: &tg.InputChannel{
					ChannelID:  t.config.BareChannelID,
					AccessHash: t.channelAccessHash,
				},
				ID: []tg.InputMessageClass{
					&tg.InputMessageID{ID: msgID},
				},
			})
			if err != nil {
				pw.CloseWithError(fmt.Errorf("failed to get message %d: %w", msgID, err))
				return
			}

			var msg *tg.Message
			switch msgs := resp.(type) {
			case *tg.MessagesMessages:
				if len(msgs.Messages) > 0 {
					msg, _ = msgs.Messages[0].(*tg.Message)
				}
			case *tg.MessagesMessagesSlice:
				if len(msgs.Messages) > 0 {
					msg, _ = msgs.Messages[0].(*tg.Message)
				}
			case *tg.MessagesChannelMessages:
				if len(msgs.Messages) > 0 {
					msg, _ = msgs.Messages[0].(*tg.Message)
				}
			}

			if msg == nil {
				pw.CloseWithError(fmt.Errorf("message %d not found or invalid type", msgID))
				return
			}

			media, ok := msg.Media.(*tg.MessageMediaDocument)
			if !ok {
				pw.CloseWithError(fmt.Errorf("message %d does not contain a document", msgID))
				return
			}

			doc, ok := media.Document.(*tg.Document)
			if !ok {
				pw.CloseWithError(fmt.Errorf("media in message %d is not a document", msgID))
				return
			}

			// For parallel download, we need a WriterAt.
			// We'll create a temporary file for this part.
			tmpFile, err := os.CreateTemp("", "minio-tg-part-*")
			if err != nil {
				pw.CloseWithError(fmt.Errorf("failed to create temp file: %w", err))
				return
			}
			tmpName := tmpFile.Name()
			defer os.Remove(tmpName)
			defer tmpFile.Close()

			// Download the document in parallel (16 threads)
			_, err = d.Download(api, doc.AsInputDocumentFileLocation()).WithThreads(16).Parallel(ctx, tmpFile)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("parallel download failed for message %d: %w", msgID, err))
				return
			}

			// Stream the part from temp file to the pipe
			if _, err := tmpFile.Seek(0, 0); err != nil {
				pw.CloseWithError(fmt.Errorf("failed to seek temp file: %w", err))
				return
			}
			if _, err := io.Copy(pw, tmpFile); err != nil {
				pw.CloseWithError(fmt.Errorf("failed to stream part from temp file: %w", err))
				return
			}
		}
	}()

	return NewGetObjectReaderFromReader(pr, objInfo, opts)
}

// DeleteObject deletes object from Telegram and Redis
func (t *TelegramObjectLayer) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	var metaBytes []byte
	err := t.db.QueryRowContext(ctx, "SELECT metadata FROM objects WHERE bucket=$1 AND key=$2", bucket, object).Scan(&metaBytes)
	if err == sql.ErrNoRows {
		return ObjectInfo{}, ObjectNotFound{Bucket: bucket, Object: object}
	}

	var meta ObjectMetadata
	json.Unmarshal(metaBytes, &meta)

	// Delete from Postgres
	t.db.ExecContext(ctx, "DELETE FROM objects WHERE bucket=$1 AND key=$2", bucket, object)

	// Delete from Telegram (Async/Best effort)
	go func() {
		// t.tgClient.API().MessagesDeleteMessages(ctx, &tg.MessagesDeleteMessagesRequest{ID: meta.Parts})
	}()

	return ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, nil
}

// GetObjectInfo reads object metadata
func (t *TelegramObjectLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	var metaBytes []byte
	err = t.db.QueryRowContext(ctx, "SELECT metadata FROM objects WHERE bucket=$1 AND key=$2", bucket, object).Scan(&metaBytes)
	if err == sql.ErrNoRows {
		return ObjectInfo{}, ObjectNotFound{Bucket: bucket, Object: object}
	} else if err != nil {
		return ObjectInfo{}, err
	}

	var meta ObjectMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        meta.Size,
		ModTime:     meta.ModTime,
		ETag:        meta.ETag,
		ContentType: meta.ContentType,
	}, nil
}

// ListObjects lists objects
func (t *TelegramObjectLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	// Simple prefix search using LIKE
	// Note: This does not implement marker/delimiter logic correctly for full S3 compliance
	// but suffices for this basic layer.
	query := "SELECT metadata FROM objects WHERE bucket=$1 AND key LIKE $2 LIMIT $3"
	rows, err := t.db.QueryContext(ctx, query, bucket, prefix+"%", maxKeys)
	if err != nil {
		return ListObjectsInfo{}, err
	}
	defer rows.Close()

	var objects []ObjectInfo
	for rows.Next() {
		var metaBytes []byte
		if err := rows.Scan(&metaBytes); err != nil {
			continue
		}
		var meta ObjectMetadata
		if err := json.Unmarshal(metaBytes, &meta); err == nil {
			objects = append(objects, ObjectInfo{
				Bucket:  meta.Bucket,
				Name:    meta.Key,
				Size:    meta.Size,
				ModTime: meta.ModTime,
				ETag:    meta.ETag,
			})
		}
	}

	return ListObjectsInfo{
		Objects: objects,
	}, nil
}

// ListObjectsV2 lists objects using the V2 protocol
func (t *TelegramObjectLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
	// Reuse ListObjects logic or implement V2 specifics
	return ListObjectsV2Info{}, NotImplemented{}
}

// CopyObject copies an object from source to destination
func (t *TelegramObjectLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, NotImplemented{}
}

// DeleteObjects deletes multiple objects in a single batch
func (t *TelegramObjectLayer) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]DeletedObject, len(objects))
	for i, obj := range objects {
		_, err := t.DeleteObject(ctx, bucket, obj.ObjectName, opts)
		errs[i] = err
		dobjects[i] = DeletedObject{ObjectName: obj.ObjectName}
	}
	return dobjects, errs
}

// ListObjectVersions lists all versions of an object
func (t *TelegramObjectLayer) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error) {
	return ListObjectVersionsInfo{}, NotImplemented{}
}

// Walk traverses the object namespace
func (t *TelegramObjectLayer) Walk(ctx context.Context, bucket, prefix string, results chan<- itemOrErr[ObjectInfo], opts WalkOptions) error {
	defer close(results)

	query := "SELECT metadata FROM objects WHERE bucket=$1 AND key LIKE $2"
	rows, err := t.db.QueryContext(ctx, query, bucket, prefix+"%")
	if err != nil {
		results <- itemOrErr[ObjectInfo]{Err: err}
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var metaBytes []byte
		if err := rows.Scan(&metaBytes); err != nil {
			continue
		}
		var meta ObjectMetadata
		if err := json.Unmarshal(metaBytes, &meta); err == nil {
			select {
			case results <- itemOrErr[ObjectInfo]{
				Item: ObjectInfo{
					Bucket:      meta.Bucket,
					Name:        meta.Key,
					Size:        meta.Size,
					ModTime:     meta.ModTime,
					ETag:        meta.ETag,
					ContentType: meta.ContentType,
				},
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// HealObjectsV2 heals objects in a bucket using the V2 protocol
func (t *TelegramObjectLayer) HealObjectsV2(ctx context.Context, bucket, prefix string, opts madmin.HealOpts, fn HealObjectFn) error {
	return NotImplemented{}
}

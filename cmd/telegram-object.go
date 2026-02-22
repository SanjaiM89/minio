package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
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

func (t *TelegramObjectLayer) PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	// Delegate system files to native MinIO!
	if t.isSystemBucket(bucket) {
		return t.base.PutObject(ctx, bucket, object, data, opts)
	}

	// Wait for Telegram client to be ready
	if err := t.tgReady(ctx); err != nil {
		return ObjectInfo{}, err
	}

	tmpFile, err := os.CreateTemp("", "minio-tg-upload-*") // Use default temp dir
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("failed to create temp file: %w", err)
	}
	tempFilePath := tmpFile.Name()
	defer os.Remove(tempFilePath)

	size, err := io.Copy(tmpFile, data.Reader)
	if err != nil {
		tmpFile.Close()
		return ObjectInfo{}, fmt.Errorf("failed to copy data to temp file: %w", err)
	}
	tmpFile.Close()

	var msgIDs []int
	var internalData []byte

	if size < 500000 {
		internalData, err = os.ReadFile(tempFilePath)
		if err != nil {
			return ObjectInfo{}, fmt.Errorf("failed to read internal/small data: %w", err)
		}
	} else {
		reopenedFile, err := os.Open(tempFilePath)
		if err != nil {
			return ObjectInfo{}, fmt.Errorf("failed to open temp file for telegram upload: %w", err)
		}
		defer reopenedFile.Close()

		var jobs []uploadJob
		var resultChans []chan uploadResult
		remaining := size
		partNum := 0

		// First, create all the jobs for the chunks
		for remaining > 0 {
			chunkSize := remaining
			if chunkSize > MaxTelegramChunkSize {
				chunkSize = MaxTelegramChunkSize
			}
			partNum++

			offset := size - remaining
			resultChan := make(chan uploadResult, 1)
			job := uploadJob{
				ctx:        ctx,
				partName:   fmt.Sprintf("%s.part%d", object, partNum),
				reader:     io.NewSectionReader(reopenedFile, offset, chunkSize),
				size:       chunkSize,
				resultChan: resultChan,
			}

			jobs = append(jobs, job)
			resultChans = append(resultChans, resultChan)
			remaining -= chunkSize
		}

		// Send all jobs to the queue
		for _, job := range jobs {
			t.uploadQueue <- job
		}

		// Wait for all jobs to complete and collect the results
		for _, resChan := range resultChans {
			select {
			case result := <-resChan:
				if result.err != nil {
					// The request context will be canceled on return, which should signal
					// other workers to stop, but there might be a race.
					return ObjectInfo{}, fmt.Errorf("upload worker failed for part: %w", result.err)
				}
				msgIDs = append(msgIDs, result.msgID)
			case <-ctx.Done():
				// The client cancelled the request
				return ObjectInfo{}, ctx.Err()
			}
		}
	}

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

	query := `
		INSERT INTO objects (bucket, key, metadata, data)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (bucket, key)
		DO UPDATE SET metadata = EXCLUDED.metadata, data = EXCLUDED.data
	`
	if _, err := t.db.ExecContext(ctx, query, bucket, object, metaBytes, internalData); err != nil {
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

func (t *TelegramObjectLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error) {
	// Delegate system files to native MinIO!
	if t.isSystemBucket(bucket) {
		return t.base.GetObjectNInfo(ctx, bucket, object, rs, h, opts)
	}

	// Wait for Telegram client to be ready
	if err := t.tgReady(ctx); err != nil {
		return nil, err
	}

	var metaBytes []byte
	var internalData []byte
	err = t.db.QueryRowContext(ctx, "SELECT metadata, data FROM objects WHERE bucket=$1 AND key=$2", bucket, object).Scan(&metaBytes, &internalData)
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

	if len(internalData) > 0 {
		return NewGetObjectReaderFromReader(bytes.NewReader(internalData), objInfo, opts)
	}

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		api := t.tgClient.API()
		d := downloader.NewDownloader()

		for _, msgID := range meta.Parts {
			var resp tg.MessagesMessagesClass
			var getErr error
			for retries := 0; retries < 10; retries++ {
				t.hashMu.RLock()
				resp, getErr = api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
					Channel: &tg.InputChannel{
						ChannelID:  t.config.BareChannelID,
						AccessHash: t.channelAccessHash,
					},
					ID: []tg.InputMessageClass{
						&tg.InputMessageID{ID: msgID},
					},
				})
				t.hashMu.RUnlock()
				if getErr == nil {
					break
				}
				if d, ok := tgerr.AsFloodWait(getErr); ok {
					time.Sleep(d + time.Second)
					continue
				}
				break
			}
			if getErr != nil {
				pw.CloseWithError(fmt.Errorf("failed to get message %d: %w", msgID, getErr))
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

			// Download the document and stream it directly to the pipe writer.
			// This avoids saving the chunk to a temporary file on disk.
			_, err = d.Download(api, doc.AsInputDocumentFileLocation()).Stream(ctx, pw)
			if err != nil {
				// The pipe might be closed by the reader, which is not a server error.
				if !errors.Is(err, io.ErrClosedPipe) {
					pw.CloseWithError(fmt.Errorf("streaming download failed for message %d: %w", msgID, err))
				}
				return
			}
		}
	}()

	return NewGetObjectReaderFromReader(pr, objInfo, opts)
}

func (t *TelegramObjectLayer) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	if t.isSystemBucket(bucket) {
		return t.base.DeleteObject(ctx, bucket, object, opts)
	}

	var metaBytes []byte
	err := t.db.QueryRowContext(ctx, "SELECT metadata FROM objects WHERE bucket=$1 AND key=$2", bucket, object).Scan(&metaBytes)
	if err == sql.ErrNoRows {
		// MinIO expects an empty ObjectInfo on successful delete of a non-existent object
		return ObjectInfo{}, nil
	}

	var meta ObjectMetadata
	json.Unmarshal(metaBytes, &meta)

	t.db.ExecContext(ctx, "DELETE FROM objects WHERE bucket=$1 AND key=$2", bucket, object)

	if len(meta.Parts) > 0 {
		go func() {
			t.hashMu.RLock()
			// Best effort deletion
			t.tgClient.API().ChannelsDeleteMessages(context.Background(), &tg.ChannelsDeleteMessagesRequest{
				Channel: &tg.InputChannel{ChannelID: t.config.BareChannelID, AccessHash: t.channelAccessHash},
				ID:      meta.Parts,
			})
			t.hashMu.RUnlock()
		}()
	}

	return ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, nil
}

func (t *TelegramObjectLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if t.isSystemBucket(bucket) {
		return t.base.GetObjectInfo(ctx, bucket, object, opts)
	}

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

func (t *TelegramObjectLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	query := "SELECT metadata FROM objects WHERE bucket=$1 AND key LIKE $2 AND key > $3 ORDER BY key ASC LIMIT $4"
	rows, err := t.db.QueryContext(ctx, query, bucket, prefix+"%", marker, maxKeys)
	if err != nil {
		return ListObjectsInfo{}, err
	}
	defer rows.Close()

	var objects []ObjectInfo
	var lastKey string
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
			lastKey = meta.Key
		}
	}

	isTruncated := len(objects) == maxKeys
	var nextMarker string
	if isTruncated {
		nextMarker = lastKey
	}

	return ListObjectsInfo{
		Objects:     objects,
		IsTruncated: isTruncated,
		NextMarker:  nextMarker,
	}, nil
}

func (t *TelegramObjectLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
	if t.isSystemBucket(bucket) {
		return t.base.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	}

	// Use the V1 list implementation
	info, err := t.ListObjects(ctx, bucket, prefix, continuationToken, delimiter, maxKeys)
	if err != nil {
		return ListObjectsV2Info{}, err
	}

	return ListObjectsV2Info{
		Objects:               info.Objects,
		IsTruncated:           info.IsTruncated,
		NextContinuationToken: info.NextMarker,
	}, nil
}

// These functions are not needed for the telegram backend, but are required by the ObjectLayer interface.
// The embedded base layer will handle them for system buckets.
// For user buckets, we can return NotImplemented or a sensible default.

func (t *TelegramObjectLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (ObjectInfo, error) {
	if t.isSystemBucket(srcBucket) && t.isSystemBucket(destBucket) {
		return t.base.CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, dstOpts)
	}
	return ObjectInfo{}, NotImplemented{}
}

func (t *TelegramObjectLayer) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	if t.isSystemBucket(bucket) {
		return t.base.DeleteObjects(ctx, bucket, objects, opts)
	}
	errs := make([]error, len(objects))
	dobjects := make([]DeletedObject, len(objects))
	for i, obj := range objects {
		_, errs[i] = t.DeleteObject(ctx, bucket, obj.ObjectName, opts)
		if errs[i] == nil {
			dobjects[i] = DeletedObject{ObjectName: obj.ObjectName}
		}
	}
	return dobjects, errs
}

func (t *TelegramObjectLayer) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error) {
	if t.isSystemBucket(bucket) {
		return t.base.ListObjectVersions(ctx, bucket, prefix, marker, versionMarker, delimiter, maxKeys)
	}
	return ListObjectVersionsInfo{}, nil // No versioning for telegram backend
}

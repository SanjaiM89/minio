package cmd

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"
)

// putLocalObject writes system files directly to the local disk
func (t *TelegramObjectLayer) putLocalObject(ctx context.Context, bucket, object string, data *PutObjReader) (ObjectInfo, error) {
	targetPath := t.getLocalPath(bucket, object)

	// Ensure the parent directories exist
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return ObjectInfo{}, err
	}

	f, err := os.Create(targetPath)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer f.Close()

	bytesWritten, err := io.Copy(f, data)
	if err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		Size:    bytesWritten,
		ModTime: time.Now(),
		IsDir:   false,
	}, nil
}

// getLocalObject reads system files from the local disk
func (t *TelegramObjectLayer) getLocalObject(ctx context.Context, bucket, object string, startOffset int64, length int64, writer io.Writer) error {
	f, err := os.Open(t.getLocalPath(bucket, object))
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectNotFound{Bucket: bucket, Object: object}
		}
		return err
	}
	defer f.Close()

	if startOffset > 0 {
		if _, err = f.Seek(startOffset, io.SeekStart); err != nil {
			return err
		}
	}

	if length > 0 {
		_, err = io.CopyN(writer, f, length)
	} else {
		_, err = io.Copy(writer, f)
	}
	return err
}

// statLocalObject gets file info for local system files
func (t *TelegramObjectLayer) statLocalObject(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	fi, err := os.Stat(t.getLocalPath(bucket, object))
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectInfo{}, ObjectNotFound{Bucket: bucket, Object: object}
		}
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		Size:    fi.Size(),
		ModTime: fi.ModTime(),
		IsDir:   fi.IsDir(),
	}, nil
}

// deleteLocalObject deletes a system file from the local disk
func (t *TelegramObjectLayer) deleteLocalObject(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	info, err := t.statLocalObject(ctx, bucket, object)
	if err != nil {
		return ObjectInfo{}, err
	}

	err = os.Remove(t.getLocalPath(bucket, object))
	if err != nil && os.IsNotExist(err) {
		return ObjectInfo{}, ObjectNotFound{Bucket: bucket, Object: object}
	}

	return info, err
}

// listLocalObjects is required for when MinIO lists the `.minio.sys` directory
func (t *TelegramObjectLayer) listLocalObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	var objects []ObjectInfo

	searchDir := filepath.Join(t.LocalDiskPath, bucket, prefix)
	entries, err := os.ReadDir(searchDir)
	if err != nil {
		if os.IsNotExist(err) {
			return ListObjectsInfo{IsTruncated: false}, nil
		}
		return ListObjectsInfo{}, err
	}

	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		objects = append(objects, ObjectInfo{
			Bucket:  bucket,
			Name:    filepath.Join(prefix, entry.Name()),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   entry.IsDir(),
		})
	}

	return ListObjectsInfo{
		Objects:     objects,
		IsTruncated: false,
	}, nil
}

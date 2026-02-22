package cmd

import (
	"context"
)

// Multipart operations stubs

// ListMultipartUploads lists all in-progress multipart uploads
func (t *TelegramObjectLayer) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (ListMultipartsInfo, error) {
	return ListMultipartsInfo{}, NotImplemented{}
}

// NewMultipartUpload starts a new multipart upload
func (t *TelegramObjectLayer) NewMultipartUpload(ctx context.Context, bucket, object string, opts ObjectOptions) (*NewMultipartUploadResult, error) {
	return nil, NotImplemented{}
}

// CopyObjectPart copies a part of an object to a multipart upload
func (t *TelegramObjectLayer) CopyObjectPart(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset int64, length int64, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (PartInfo, error) {
	return PartInfo{}, NotImplemented{}
}

// PutObjectPart uploads a part of an object in a multipart upload
func (t *TelegramObjectLayer) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, data *PutObjReader, opts ObjectOptions) (PartInfo, error) {
	return PartInfo{}, NotImplemented{}
}

// GetMultipartInfo returns information about a multipart upload
func (t *TelegramObjectLayer) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) (MultipartInfo, error) {
	return MultipartInfo{}, NotImplemented{}
}

// ListObjectParts lists all parts of a multipart upload
func (t *TelegramObjectLayer) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts ObjectOptions) (ListPartsInfo, error) {
	return ListPartsInfo{}, NotImplemented{}
}

// AbortMultipartUpload aborts a multipart upload
func (t *TelegramObjectLayer) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts ObjectOptions) error {
	return NotImplemented{}
}

// CompleteMultipartUpload completes a multipart upload
func (t *TelegramObjectLayer) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, uploadedParts []CompletePart, opts ObjectOptions) (ObjectInfo, error) {
	return ObjectInfo{}, NotImplemented{}
}

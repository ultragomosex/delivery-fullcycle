package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"

	"delivery-fullcycle/services/media-service/internal/storage"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MediaHandler struct {
	dbPool   *pgxpool.Pool
	s3Client *storage.S3Client
	defaultB string
}

func NewMediaHandler(dbPool *pgxpool.Pool, s3Client *storage.S3Client, defaultBucket string) *MediaHandler {
	return &MediaHandler{
		dbPool:   dbPool,
		s3Client: s3Client,
		defaultB: defaultBucket,
	}
}

func (h *MediaHandler) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	dbErr := h.dbPool.Ping(ctx)
	s3Err := h.s3Client.Ping(ctx)

	status := "ok"
	code := http.StatusOK
	if dbErr != nil || s3Err != nil {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":   status,
		"database": dbErr == nil,
		"s3":       s3Err == nil,
		"time":     time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *MediaHandler) ServeMedia(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	objectKey := chi.URLParam(r, "*")
	if objectKey == "" {
		http.Error(w, "object key required", http.StatusBadRequest)
		return
	}

	obj, contentType, size, lastModified, etag, err := h.s3Client.GetObject(r.Context(), bucket, objectKey)
	if err != nil {
		http.Error(w, "media not found", http.StatusNotFound)
		return
	}
	defer obj.Close()

	if match := r.Header.Get("If-None-Match"); match != "" && etag != "" && match == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("Content-Type", contentType)
	if size > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	}
	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	if !lastModified.IsZero() {
		w.Header().Set("Last-Modified", lastModified.UTC().Format(http.TimeFormat))
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")

	if r.Method == http.MethodGet {
		_, _ = h.s3Client.StreamCopy(w, obj)
	}
}

func (h *MediaHandler) Upload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "invalid multipart form: "+err.Error(), http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing file field: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	targetBucket := r.FormValue("bucket")
	if targetBucket == "" {
		targetBucket = h.defaultB
	}

	folder := r.FormValue("folder")
	if folder == "" {
		folder = "uploads"
	}

	filename := header.Filename
	ext := path.Ext(filename)
	key := fmt.Sprintf("%s/%d%s", folder, time.Now().UnixNano(), ext)

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if err := h.s3Client.Upload(r.Context(), targetBucket, key, file, header.Size, contentType); err != nil {
		http.Error(w, "failed to upload to s3: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"bucket": targetBucket,
		"key":    key,
		"url":    fmt.Sprintf("/media/%s/%s", targetBucket, key),
		"size":   header.Size,
	})
}

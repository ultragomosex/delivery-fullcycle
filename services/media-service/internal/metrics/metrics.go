package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HttpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "media_http_requests_total",
			Help: "Total number of HTTP requests processed by media-service",
		},
		[]string{"method", "path", "status"},
	)

	HttpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "media_http_request_duration_seconds",
			Help:    "HTTP request latency in seconds for media-service",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	MediaUploadsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "media_uploads_total",
			Help: "Total number of media uploads processed",
		},
		[]string{"status", "mime_type"},
	)

	MediaBytesTransferredTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "media_bytes_transferred_total",
			Help: "Total bytes of media transferred",
		},
		[]string{"direction"},
	)
)

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

		next.ServeHTTP(ww, r)

		pattern := chi.RouteContext(r.Context()).RoutePattern()
		if pattern == "" {
			pattern = r.URL.Path
		}

		duration := time.Since(start).Seconds()
		statusStr := strconv.Itoa(ww.Status())

		HttpRequestsTotal.WithLabelValues(r.Method, pattern, statusStr).Inc()
		HttpRequestDuration.WithLabelValues(r.Method, pattern).Observe(duration)
	})
}

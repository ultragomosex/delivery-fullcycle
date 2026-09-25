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
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed",
		},
		[]string{"method", "path", "status"},
	)

	HttpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	OrdersCreatedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "orders_created_total",
			Help: "Total number of created orders",
		},
		[]string{"status"},
	)

	OrdersAmountTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "orders_amount_rubles_total",
			Help: "Total monetary amount of all created orders",
		},
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

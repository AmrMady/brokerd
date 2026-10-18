// Package metrics exports brokerd's numbers to Prometheus.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/AmrMady/brokerd/internal/engine"
	"github.com/AmrMady/brokerd/internal/marketdata"
	"github.com/AmrMady/brokerd/internal/stream"
)

// Registry holds brokerd's metrics. Counts the pipeline already keeps, like
// the sequencer's gaps and the hub's drops, are read when scraped rather
// than counted twice.
type Registry struct {
	reg      *prometheus.Registry
	fill     prometheus.Histogram
	post     prometheus.Histogram
	requests *prometheus.HistogramVec
}

// New returns a registry with the Go runtime and process metrics.
func New() *Registry {
	r := &Registry{
		reg: prometheus.NewRegistry(),
		fill: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "brokerd_order_fill_seconds",
			Help:    "Time from placing an order to its fill.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 10), // 1ms to 4m
		}),
		post: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "brokerd_ledger_post_seconds",
			Help:    "Time to post a fill or a transfer to the ledger.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12), // 0.5ms to 1s
		}),
		requests: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "brokerd_http_request_seconds",
			Help:    "HTTP requests by route and status.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "code"}),
	}
	r.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		r.fill, r.post, r.requests)
	return r
}

// Handler serves the metrics for scraping.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}

// ObserveFill records how long an order took to fill.
func (r *Registry) ObserveFill(d time.Duration) { r.fill.Observe(d.Seconds()) }

// ObservePost records how long a ledger posting took.
func (r *Registry) ObservePost(d time.Duration) { r.post.Observe(d.Seconds()) }

// ObserveRequest records an HTTP request by its route pattern, which keeps
// the label count bounded, unlike the path.
func (r *Registry) ObserveRequest(route string, status int, d time.Duration) {
	r.requests.WithLabelValues(route, strconv.Itoa(status)).Observe(d.Seconds())
}

// MarketData exports the sequencer's and the aggregators' counts.
func (r *Registry) MarketData(seq *marketdata.Sequencer, bars map[string]*marketdata.Aggregator) {
	counter := func(name, help string, f func(marketdata.SequenceStats) uint64) {
		r.reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help},
			func() float64 { return float64(f(seq.Stats())) }))
	}
	counter("brokerd_marketdata_events_total", "Trades and quotes passed on by the sequencer.",
		func(s marketdata.SequenceStats) uint64 { return s.Events })
	counter("brokerd_marketdata_sequence_gaps_total", "Events that skipped ahead in their symbol's sequence.",
		func(s marketdata.SequenceStats) uint64 { return s.Gaps })
	counter("brokerd_marketdata_missed_events_total", "Sequence numbers skipped by gaps.",
		func(s marketdata.SequenceStats) uint64 { return s.Missed })
	counter("brokerd_marketdata_stale_events_total", "Duplicate or out-of-order events dropped.",
		func(s marketdata.SequenceStats) uint64 { return s.Stale })

	for timeframe, agg := range bars {
		labels := prometheus.Labels{"timeframe": timeframe}
		r.reg.MustRegister(
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "brokerd_marketdata_bars_total", Help: "Bars built.", ConstLabels: labels,
			}, func() float64 { return float64(agg.Stats().Bars) }),
			prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "brokerd_marketdata_late_trades_total", Help: "Trades dropped because their bar had closed.", ConstLabels: labels,
			}, func() float64 { return float64(agg.Stats().Late) }),
		)
	}
}

// Engine exports the engine's counts.
func (r *Registry) Engine(e *engine.Engine) {
	r.reg.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "brokerd_engine_expiry_errors_total", Help: "Times an order failed to expire."},
			func() float64 { return float64(e.Stats().ExpiryErrors) }),
	)
}

// Stream exports the hub's counts.
func (r *Registry) Stream(hub *stream.Hub) {
	r.reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "brokerd_stream_clients", Help: "Connected stream clients."},
			func() float64 { return float64(hub.Stats().Clients) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "brokerd_stream_subscriptions", Help: "Channel and symbol subscriptions."},
			func() float64 { return float64(hub.Stats().Subscriptions) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "brokerd_stream_dropped_messages_total", Help: "Messages dropped from full client queues."},
			func() float64 { return float64(hub.Stats().Dropped) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "brokerd_stream_disconnects_total", Help: "Clients disconnected for being too slow."},
			func() float64 { return float64(hub.Stats().Disconnected) }),
	)
}

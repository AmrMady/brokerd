package marketdata_test

import (
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

// The designs QuoteCache was chosen over, to compare against.

type quoteStore interface {
	HandleQuote(marketdata.Quote)
	Quote(symbol string) (marketdata.Quote, bool)
}

type mutexCache struct {
	mu     sync.RWMutex
	quotes map[string]marketdata.Quote
}

func (c *mutexCache) HandleQuote(q marketdata.Quote) {
	c.mu.Lock()
	c.quotes[q.Symbol] = q
	c.mu.Unlock()
}

func (c *mutexCache) Quote(symbol string) (marketdata.Quote, bool) {
	c.mu.RLock()
	q, ok := c.quotes[symbol]
	c.mu.RUnlock()
	return q, ok
}

type shardedCache struct {
	seed   maphash.Seed
	shards [64]struct {
		mu     sync.RWMutex
		quotes map[string]marketdata.Quote
		_      [32]byte // keep shards on separate cache lines
	}
}

func newShardedCache() *shardedCache {
	c := &shardedCache{seed: maphash.MakeSeed()}
	for i := range c.shards {
		c.shards[i].quotes = map[string]marketdata.Quote{}
	}
	return c
}

func (c *shardedCache) shard(symbol string) int {
	return int(maphash.String(c.seed, symbol) % uint64(len(c.shards)))
}

func (c *shardedCache) HandleQuote(q marketdata.Quote) {
	s := &c.shards[c.shard(q.Symbol)]
	s.mu.Lock()
	s.quotes[q.Symbol] = q
	s.mu.Unlock()
}

func (c *shardedCache) Quote(symbol string) (marketdata.Quote, bool) {
	s := &c.shards[c.shard(symbol)]
	s.mu.RLock()
	q, ok := s.quotes[symbol]
	s.mu.RUnlock()
	return q, ok
}

// snapshotCache copies its whole index when a symbol is added.
type snapshotCache struct {
	mu    sync.Mutex
	index atomic.Pointer[map[string]*atomic.Pointer[marketdata.Quote]]
}

func newSnapshotCache() *snapshotCache {
	c := &snapshotCache{}
	c.index.Store(&map[string]*atomic.Pointer[marketdata.Quote]{})
	return c
}

func (c *snapshotCache) HandleQuote(q marketdata.Quote) {
	p := (*c.index.Load())[q.Symbol]
	if p == nil {
		c.mu.Lock()
		old := *c.index.Load()
		if p = old[q.Symbol]; p == nil {
			next := make(map[string]*atomic.Pointer[marketdata.Quote], len(old)+1)
			for k, v := range old {
				next[k] = v
			}
			p = new(atomic.Pointer[marketdata.Quote])
			next[q.Symbol] = p
			c.index.Store(&next)
		}
		c.mu.Unlock()
	}
	p.Store(&q)
}

func (c *snapshotCache) Quote(symbol string) (marketdata.Quote, bool) {
	p := (*c.index.Load())[symbol]
	if p == nil {
		return marketdata.Quote{}, false
	}
	q := p.Load()
	if q == nil {
		return marketdata.Quote{}, false
	}
	return *q, true
}

var caches = []struct {
	name string
	new  func() quoteStore
}{
	{"rwmutex", func() quoteStore { return &mutexCache{quotes: map[string]marketdata.Quote{}} }},
	{"sharded", func() quoteStore { return newShardedCache() }},
	{"snapshot", func() quoteStore { return newSnapshotCache() }},
	{"syncmap", func() quoteStore { return &marketdata.QuoteCache{} }},
}

func benchQuotes(n int) []marketdata.Quote {
	quotes := make([]marketdata.Quote, n)
	for i := range quotes {
		quotes[i] = marketdata.Quote{Symbol: fmt.Sprintf("S%05d", i), Seq: uint64(i)}
	}
	return quotes
}

func BenchmarkQuoteCache(b *testing.B) {
	quotes := benchQuotes(1000)
	for _, c := range caches {
		read := func(b *testing.B, s quoteStore) {
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if _, ok := s.Quote(quotes[i%len(quotes)].Symbol); !ok {
						b.Error("missing quote")
						return
					}
					i += 7
				}
			})
		}
		b.Run(c.name+"/read", func(b *testing.B) {
			s := c.new()
			for _, q := range quotes {
				s.HandleQuote(q)
			}
			read(b, s)
		})
		b.Run(c.name+"/read-while-writing", func(b *testing.B) {
			s := c.new()
			for _, q := range quotes {
				s.HandleQuote(q)
			}
			var (
				stop   atomic.Bool
				wg     sync.WaitGroup
				writes int
			)
			wg.Go(func() {
				for ; !stop.Load(); writes++ {
					s.HandleQuote(quotes[writes%len(quotes)])
				}
			})
			b.ResetTimer()
			read(b, s)
			stop.Store(true)
			wg.Wait()
			b.ReportMetric(float64(writes)/b.Elapsed().Seconds(), "writes/s")
		})
		b.Run(c.name+"/add-10k-symbols", func(b *testing.B) {
			added := benchQuotes(10000)
			for range b.N {
				s := c.new()
				for _, q := range added {
					s.HandleQuote(q)
				}
			}
		})
	}
}

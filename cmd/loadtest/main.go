// Command loadtest drives a running brokerd with traders placing and
// canceling orders, and reports throughput and latency per operation.
//
// Each worker is one trader with its own account. It places market buys,
// market sells of shares it holds, and limit buys far below the market,
// which rest and which it then cancels.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"
)

func main() {
	url := flag.String("url", "http://localhost:8080", "brokerd to test")
	workers := flag.Int("workers", 50, "traders placing orders at once")
	duration := flag.Duration("duration", 30*time.Second, "how long to run")
	symbol := flag.String("symbol", "AAPL", "symbol to trade")
	flag.Parse()
	if err := run(*url, *workers, *duration, *symbol); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type client struct {
	http *http.Client
	url  string
}

// call sends a request and decodes the response into out, if it's set.
func (c *client) call(ctx context.Context, method, path, key, body string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.url+path, bytes.NewBufferString(body))
	if err != nil {
		return 0, err
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // nothing to do about it
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if out != nil && resp.StatusCode < 300 {
		err = json.Unmarshal(data, out)
	}
	return resp.StatusCode, err
}

type trader struct {
	id, key string
	shares  int
}

func newTrader(ctx context.Context, c *client) (*trader, error) {
	var created struct {
		Account struct{ ID string } `json:"account"`
		APIKey  string              `json:"api_key"`
	}
	if status, err := c.call(ctx, "POST", "/v1/accounts", "", "", &created); err != nil || status != http.StatusCreated {
		return nil, fmt.Errorf("create account: %d %w", status, err)
	}
	t := &trader{id: created.Account.ID, key: created.APIKey}
	if status, err := c.call(ctx, "POST", "/v1/accounts/"+t.id+"/deposits", t.key, `{"amount":"10000000"}`, nil); err != nil || status != http.StatusCreated {
		return nil, fmt.Errorf("deposit: %d %w", status, err)
	}
	return t, nil
}

// sample is one request: what it was, its status, and how long it took.
type sample struct {
	op     string
	status int
	took   time.Duration
}

func (t *trader) step(ctx context.Context, c *client, symbol string, rng *rand.Rand, record func(sample)) {
	timed := func(op, method, path, body string, out any) int {
		began := time.Now()
		status, err := c.call(ctx, method, path, t.key, body, out)
		if err != nil && ctx.Err() == nil {
			status = 0
		}
		if ctx.Err() == nil {
			record(sample{op, status, time.Since(began)})
		}
		return status
	}
	orders := "/v1/accounts/" + t.id + "/orders"
	switch n := rng.IntN(10); {
	case n < 5 || t.shares == 0:
		var o struct{ Status string }
		body := fmt.Sprintf(`{"symbol":%q,"side":"buy","type":"market","time_in_force":"day","qty":"1"}`, symbol)
		if timed("market buy", "POST", orders, body, &o) == http.StatusCreated && o.Status == "filled" {
			t.shares++
		}
	case n < 7:
		var o struct{ Status string }
		body := fmt.Sprintf(`{"symbol":%q,"side":"sell","type":"market","time_in_force":"day","qty":"1"}`, symbol)
		if timed("market sell", "POST", orders, body, &o) == http.StatusCreated && o.Status == "filled" {
			t.shares--
		}
	default:
		var o struct{ ID string }
		body := fmt.Sprintf(`{"symbol":%q,"side":"buy","type":"limit","time_in_force":"gtc","qty":"1","limit_price":"1"}`, symbol)
		if timed("limit buy", "POST", orders, body, &o) == http.StatusCreated {
			timed("cancel", "DELETE", orders+"/"+o.ID, "", nil)
		}
	}
}

func run(url string, workers int, duration time.Duration, symbol string) error {
	c := &client{http: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: workers}}, url: url}
	setup, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	traders := make([]*trader, workers)
	for i := range traders {
		t, err := newTrader(setup, c)
		if err != nil {
			return err
		}
		traders[i] = t
	}

	var (
		mu      sync.Mutex
		samples []sample
		wg      sync.WaitGroup
	)
	record := func(s sample) {
		mu.Lock()
		samples = append(samples, s)
		mu.Unlock()
	}
	ctx, stop := context.WithTimeout(context.Background(), duration)
	defer stop()
	began := time.Now()
	for i, t := range traders {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(i), 1)) //nolint:gosec // picking operations, not secrets
			for ctx.Err() == nil {
				t.step(ctx, c, symbol, rng, record)
			}
		})
	}
	wg.Wait()
	report(samples, time.Since(began), workers)
	return nil
}

func report(samples []sample, elapsed time.Duration, workers int) {
	byOp := map[string][]time.Duration{}
	statuses := map[string]map[int]int{}
	for _, s := range samples {
		byOp[s.op] = append(byOp[s.op], s.took)
		if statuses[s.op] == nil {
			statuses[s.op] = map[int]int{}
		}
		statuses[s.op][s.status]++
	}
	fmt.Printf("%d requests in %s with %d traders: %.0f requests/s\n\n", len(samples), elapsed.Round(time.Millisecond),
		workers, float64(len(samples))/elapsed.Seconds())
	fmt.Printf("%-12s %8s %8s %8s %8s %8s  %s\n", "operation", "count", "p50", "p95", "p99", "max", "statuses")
	for _, op := range []string{"market buy", "market sell", "limit buy", "cancel"} {
		d := byOp[op]
		if len(d) == 0 {
			continue
		}
		slices.Sort(d)
		p := func(q float64) string { return d[int(q*float64(len(d)-1))].Round(100 * time.Microsecond).String() }
		fmt.Printf("%-12s %8d %8s %8s %8s %8s  %v\n", op, len(d), p(0.5), p(0.95), p(0.99), p(1), statuses[op])
	}
}

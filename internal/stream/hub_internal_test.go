package stream

import (
	"testing"
	"time"

	"github.com/AmrMady/brokerd/internal/marketdata"
)

// Publishing, on the feed's goroutine, takes no lock that subscription
// changes hold, so a client changing its subscriptions can't stall the
// feed.
func TestPublishTakesNoHubLock(t *testing.T) {
	t.Parallel()
	h, err := NewHub(Config{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := h.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.Subscribe(Subscriptions{Quotes: []string{"AAPL"}}); err != nil {
		t.Fatal(err)
	}

	h.mu.Lock()
	published := make(chan struct{})
	go func() {
		h.HandleQuote(marketdata.Quote{Symbol: "AAPL", Seq: 1})
		close(published)
	}()
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Error("publishing waited for the hub's lock")
	}
	h.mu.Unlock()
	<-published
}

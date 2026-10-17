package stream

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/shopspring/decimal"
)

// Subscriptions lists symbols by channel. It's both what a client asks for
// and what the hub reports back.
type Subscriptions struct {
	Trades []string `json:"trades"`
	Quotes []string `json:"quotes"`
	Bars   []string `json:"bars"`
}

var symbolPattern = regexp.MustCompile(`^[A-Z][A-Z0-9./-]{0,15}$`)

func (s Subscriptions) topics() ([]topic, error) {
	var topics []topic
	for _, ch := range []struct {
		channel Channel
		symbols []string
	}{{Trades, s.Trades}, {Quotes, s.Quotes}, {Bars, s.Bars}} {
		for _, symbol := range ch.symbols {
			if !symbolPattern.MatchString(symbol) {
				return nil, fmt.Errorf("invalid symbol %q", symbol)
			}
			topics = append(topics, topic{ch.channel, symbol})
		}
	}
	return topics, nil
}

// request is what a client sends.
type request struct {
	Action string `json:"action"` // subscribe or unsubscribe
	Subscriptions
}

type subscriptionMessage struct {
	Type string `json:"type"`
	Subscriptions
}

type errorMessage struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type tradeMessage struct {
	Type      string          `json:"type"`
	Symbol    string          `json:"symbol"`
	Price     decimal.Decimal `json:"price"`
	Size      decimal.Decimal `json:"size"`
	Timestamp time.Time       `json:"timestamp"`
	Seq       uint64          `json:"seq"`
}

type quoteMessage struct {
	Type      string          `json:"type"`
	Symbol    string          `json:"symbol"`
	Bid       decimal.Decimal `json:"bid"`
	BidSize   decimal.Decimal `json:"bid_size"`
	Ask       decimal.Decimal `json:"ask"`
	AskSize   decimal.Decimal `json:"ask_size"`
	Timestamp time.Time       `json:"timestamp"`
	Seq       uint64          `json:"seq"`
}

type barMessage struct {
	Type      string          `json:"type"`
	Symbol    string          `json:"symbol"`
	Timeframe string          `json:"timeframe"`
	Start     time.Time       `json:"start"`
	Open      decimal.Decimal `json:"open"`
	High      decimal.Decimal `json:"high"`
	Low       decimal.Decimal `json:"low"`
	Close     decimal.Decimal `json:"close"`
	Volume    decimal.Decimal `json:"volume"`
	Trades    int             `json:"trades"`
}

// encode marshals m. Only a time outside the years 0 to 9999 can fail to
// encode, and no feed sends one; such a message is dropped.
func encode(m any) ([]byte, bool) {
	b, err := json.Marshal(m)
	return b, err == nil
}

// timeframe writes 1s and 1m as such rather than as 1s and 1m0s.
func timeframe(d time.Duration) string {
	switch {
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	default:
		return d.String()
	}
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

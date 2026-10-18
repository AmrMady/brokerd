package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/orders"
	"github.com/AmrMady/brokerd/internal/portfolio"
)

type orderRequest struct {
	Symbol        string              `json:"symbol"`
	Side          string              `json:"side"`
	Type          string              `json:"type"`
	TimeInForce   string              `json:"time_in_force"`
	Quantity      decimal.Decimal     `json:"qty"`
	LimitPrice    decimal.NullDecimal `json:"limit_price"`
	ClientOrderID string              `json:"client_order_id"`
}

type orderJSON struct {
	ID            string              `json:"id"`
	ClientOrderID string              `json:"client_order_id,omitempty"`
	Symbol        string              `json:"symbol"`
	Side          string              `json:"side"`
	Type          string              `json:"type"`
	TimeInForce   string              `json:"time_in_force"`
	Quantity      decimal.Decimal     `json:"qty"`
	LimitPrice    decimal.NullDecimal `json:"limit_price"`
	Status        string              `json:"status"`
	RejectReason  string              `json:"reject_reason,omitempty"`
	CancelReason  string              `json:"cancel_reason,omitempty"`
	Commission    decimal.Decimal     `json:"commission"`
	FillPrice     decimal.NullDecimal `json:"fill_price"`
	CreatedAt     time.Time           `json:"created_at"`
	UpdatedAt     time.Time           `json:"updated_at"`
	ExpiresAt     *time.Time          `json:"expires_at"`
	ClosedAt      *time.Time          `json:"closed_at"`
}

func orderOf(o orders.Order) orderJSON {
	out := orderJSON{
		ID: o.ID, ClientOrderID: o.ClientOrderID, Symbol: string(o.Symbol), Side: string(o.Side), Type: string(o.Type),
		TimeInForce: string(o.TimeInForce), Quantity: o.Quantity, LimitPrice: o.LimitPrice, Status: string(o.Status),
		RejectReason: o.RejectReason, CancelReason: o.CancelReason, Commission: o.Commission, FillPrice: o.FillPrice,
		CreatedAt: o.CreatedAt, UpdatedAt: o.UpdatedAt,
	}
	if !o.ExpiresAt.IsZero() {
		out.ExpiresAt = &o.ExpiresAt
	}
	if !o.ClosedAt.IsZero() {
		out.ClosedAt = &o.ClosedAt
	}
	return out
}

// placeOrder places an order. An order the account can't cover is still
// created, as rejected, with the reason. Without a client_order_id, the
// request's Idempotency-Key becomes one, so a retry can't place a second
// order.
func (s *Server) placeOrder(w http.ResponseWriter, r *http.Request) error {
	var body orderRequest
	if err := decode(r, &body); err != nil {
		return err
	}
	if body.ClientOrderID == "" {
		body.ClientOrderID = r.Header.Get("Idempotency-Key")
	}
	o, err := s.cfg.Engine.Place(r.Context(), orders.Request{
		AccountID:     r.PathValue("account"),
		ClientOrderID: body.ClientOrderID,
		Symbol:        ledger.Asset(body.Symbol),
		Side:          ledger.Side(body.Side),
		Type:          orders.Type(body.Type),
		TimeInForce:   orders.TimeInForce(body.TimeInForce),
		Quantity:      body.Quantity,
		LimitPrice:    body.LimitPrice,
	})
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, orderOf(o))
	return nil
}

func (s *Server) listOrders(w http.ResponseWriter, r *http.Request) error {
	limit, err := pageLimit(r)
	if err != nil {
		return err
	}
	var open bool
	switch status := r.URL.Query().Get("status"); status {
	case "", "all":
	case "open":
		open = true
	default:
		return newError(http.StatusUnprocessableEntity, "invalid_request", "status must be open or all, got %q", status)
	}
	list, err := orders.List(r.Context(), s.cfg.Pool, r.PathValue("account"), open, r.URL.Query().Get("before"), limit)
	if err != nil {
		return err
	}
	out := make([]orderJSON, len(list))
	for i, o := range list {
		out[i] = orderOf(o)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getOrder(w http.ResponseWriter, r *http.Request) error {
	o, err := s.cfg.Engine.Order(r.Context(), r.PathValue("account"), r.PathValue("order"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, orderOf(o))
	return nil
}

func (s *Server) cancelOrder(w http.ResponseWriter, r *http.Request) error {
	o, err := s.cfg.Engine.Cancel(r.Context(), r.PathValue("account"), r.PathValue("order"))
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, orderOf(o))
	return nil
}

type positionJSON struct {
	Symbol      string              `json:"symbol"`
	Quantity    decimal.Decimal     `json:"qty"`
	Available   decimal.Decimal     `json:"qty_available"`
	CostBasis   decimal.NullDecimal `json:"cost_basis"`
	AverageCost decimal.NullDecimal `json:"avg_entry_price"`
	Price       decimal.NullDecimal `json:"current_price"`
	MarketValue decimal.NullDecimal `json:"market_value"`
	Unrealized  decimal.NullDecimal `json:"unrealized_pl"`
}

func (s *Server) positions(w http.ResponseWriter, r *http.Request) error {
	list, err := portfolio.Positions(r.Context(), s.cfg.Pool, s.cfg.Quotes, r.PathValue("account"))
	if err != nil {
		return err
	}
	out := make([]positionJSON, len(list))
	for i, p := range list {
		out[i] = positionJSON{string(p.Symbol), p.Quantity, p.Available, p.CostBasis, p.AverageCost, p.Price, p.MarketValue, p.Unrealized}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

type activityJSON struct {
	ID        int64             `json:"id"`
	Seq       int64             `json:"seq"`
	Type      string            `json:"type"`
	CreatedAt time.Time         `json:"created_at"`
	Details   map[string]string `json:"details"`
	Changes   []changeJSON      `json:"changes"`
}

type changeJSON struct {
	Asset  string          `json:"asset"`
	Amount decimal.Decimal `json:"amount"`
}

func (s *Server) activities(w http.ResponseWriter, r *http.Request) error {
	limit, err := pageLimit(r)
	if err != nil {
		return err
	}
	var before int64
	if v := r.URL.Query().Get("before"); v != "" {
		if before, err = strconv.ParseInt(v, 10, 64); err != nil || before < 1 {
			return newError(http.StatusUnprocessableEntity, "invalid_request", "before must be an activity's seq, got %q", v)
		}
	}
	list, err := ledger.Activities(r.Context(), s.cfg.Pool, r.PathValue("account"), before, limit)
	if err != nil {
		return err
	}
	out := make([]activityJSON, len(list))
	for i, a := range list {
		out[i] = activityJSON{ID: a.EntryID, Seq: a.Seq, Type: string(a.Kind), CreatedAt: a.CreatedAt, Details: a.Metadata}
		for _, c := range a.Changes {
			out[i].Changes = append(out[i].Changes, changeJSON{string(c.Asset), c.Amount})
		}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// pageLimit reads the limit parameter: 50 by default, at most 500.
func pageLimit(r *http.Request) (int, error) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return 50, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > orders.MaxPageSize {
		return 0, newError(http.StatusUnprocessableEntity, "invalid_request", "limit must be between 1 and %d, got %q", orders.MaxPageSize, v)
	}
	return n, nil
}

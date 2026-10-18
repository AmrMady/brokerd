// Package orders stores orders and moves them through their states:
//
//	new -> accepted -> filled | canceled | expired
//	new -> rejected
//
// Each move is a compare-and-set on the status, and the only way to change
// an order, so a fill, a cancel and an expiry can't all happen to the same
// order. A trigger in Postgres enforces the same rules.
package orders

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
)

// Errors returned by this package.
var (
	ErrInvalid               = errors.New("invalid order")
	ErrNotFound              = errors.New("order not found")
	ErrNotOpen               = errors.New("order is not open")
	ErrClientOrderIDConflict = errors.New("client order id already used for a different order")
	errNotNew                = errors.New("order is no longer new")
)

// Type is how an order is priced.
type Type string

// Order types.
const (
	Market Type = "market" // fills at the current quote
	Limit  Type = "limit"  // fills at the limit price or better
)

// TimeInForce is how long an order stays open.
type TimeInForce string

// Times in force.
const (
	Day TimeInForce = "day" // expires at the session's close
	GTC TimeInForce = "gtc" // good till canceled
)

// Status is where an order is in its life.
type Status string

// Statuses.
const (
	New      Status = "new"
	Accepted Status = "accepted"
	Filled   Status = "filled"
	Canceled Status = "canceled"
	Rejected Status = "rejected"
	Expired  Status = "expired"
)

// Request is an order as a client places it.
type Request struct {
	AccountID     string
	ClientOrderID string // optional; a retry with the same one returns the first order
	Symbol        ledger.Asset
	Side          ledger.Side
	Type          Type
	TimeInForce   TimeInForce
	Quantity      decimal.Decimal
	LimitPrice    decimal.NullDecimal // limit orders only
}

const maxClientOrderID = 64

// Validate checks what can be checked without the database.
func (r Request) Validate() error {
	if r.AccountID == "" {
		return fmt.Errorf("%w: account is required", ErrInvalid)
	}
	if len(r.ClientOrderID) > maxClientOrderID {
		return fmt.Errorf("%w: client order id is longer than %d characters", ErrInvalid, maxClientOrderID)
	}
	for _, c := range r.ClientOrderID {
		if !unicode.IsPrint(c) {
			return fmt.Errorf("%w: client order id has a character that isn't printable", ErrInvalid)
		}
	}
	if r.Symbol == "" || r.Symbol == ledger.USD {
		return fmt.Errorf("%w: symbol is required", ErrInvalid)
	}
	if r.Side != ledger.Buy && r.Side != ledger.Sell {
		return fmt.Errorf("%w: side must be buy or sell, got %q", ErrInvalid, r.Side)
	}
	if r.TimeInForce != Day && r.TimeInForce != GTC {
		return fmt.Errorf("%w: time in force must be day or gtc, got %q", ErrInvalid, r.TimeInForce)
	}
	if !r.Quantity.IsPositive() {
		return fmt.Errorf("%w: quantity must be positive, got %s", ErrInvalid, r.Quantity)
	}
	switch r.Type {
	case Market:
		if r.LimitPrice.Valid {
			return fmt.Errorf("%w: a market order has no limit price", ErrInvalid)
		}
	case Limit:
		if !r.LimitPrice.Valid {
			return fmt.Errorf("%w: a limit order needs a limit price", ErrInvalid)
		}
		return checkTick(r.LimitPrice.Decimal)
	default:
		return fmt.Errorf("%w: type must be market or limit, got %q", ErrInvalid, r.Type)
	}
	return nil
}

// checkTick applies the minimum price increment for US stocks: a cent at
// $1 and up, a hundredth of a cent below.
func checkTick(price decimal.Decimal) error {
	if !price.IsPositive() {
		return fmt.Errorf("%w: limit price must be positive, got %s", ErrInvalid, price)
	}
	places := int32(2)
	if price.LessThan(decimal.NewFromInt(1)) {
		places = 4
	}
	if !price.Equal(price.Truncate(places)) {
		return fmt.Errorf("%w: limit price %s has more than %d decimal places", ErrInvalid, price, places)
	}
	return nil
}

// sameAs reports whether o was placed by the same request as r.
func (o Order) sameAs(r Request) bool {
	return o.AccountID == r.AccountID && o.Symbol == r.Symbol && o.Side == r.Side && o.Type == r.Type &&
		o.TimeInForce == r.TimeInForce && o.Quantity.Equal(r.Quantity) &&
		o.LimitPrice.Valid == r.LimitPrice.Valid && o.LimitPrice.Decimal.Equal(r.LimitPrice.Decimal)
}

// Order is a stored order.
type Order struct {
	ID            string
	AccountID     string
	ClientOrderID string
	Symbol        ledger.Asset
	Side          ledger.Side
	Type          Type
	TimeInForce   TimeInForce
	Quantity      decimal.Decimal
	LimitPrice    decimal.NullDecimal
	Status        Status
	RejectReason  string
	HoldID        int64
	ExpiresAt     time.Time // day orders: the close of their session
	FillPrice     decimal.NullDecimal
	FillEntryID   int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	ClosedAt      time.Time // zero while new or accepted
}

// Open reports whether o can still fill.
func (o Order) Open() bool {
	return o.Status == Accepted
}

// PastExpiry reports whether o is a day order whose session closed by now.
func (o Order) PastExpiry(now time.Time) bool {
	return !o.ExpiresAt.IsZero() && !now.Before(o.ExpiresAt)
}

const columns = `id::text, account_id::text, COALESCE(client_order_id, ''), symbol, side, type, time_in_force,
	quantity, limit_price, status, COALESCE(reject_reason, ''), COALESCE(hold_id, 0),
	expires_at, fill_price, COALESCE(fill_entry_id, 0), created_at, updated_at, closed_at`

func scan(row pgx.Row) (Order, error) {
	var (
		o               Order
		expires, closed *time.Time
	)
	err := row.Scan(&o.ID, &o.AccountID, &o.ClientOrderID, &o.Symbol, &o.Side, &o.Type, &o.TimeInForce,
		&o.Quantity, &o.LimitPrice, &o.Status, &o.RejectReason, &o.HoldID,
		&expires, &o.FillPrice, &o.FillEntryID, &o.CreatedAt, &o.UpdatedAt, &closed)
	if expires != nil {
		o.ExpiresAt = *expires
	}
	if closed != nil {
		o.ClosedAt = *closed
	}
	return o, err
}

func scanRow(row pgx.CollectableRow) (Order, error) {
	return scan(row)
}

// Create stores r as a new order. If the account already placed an order
// with the same client order id, it returns that order and created is
// false, or ErrClientOrderIDConflict if the request was different.
func Create(ctx context.Context, tx pgx.Tx, r Request) (o Order, created bool, err error) {
	if err = r.Validate(); err != nil {
		return Order{}, false, err
	}
	var clientOrderID *string
	if r.ClientOrderID != "" {
		clientOrderID = &r.ClientOrderID
	}
	o, err = scan(tx.QueryRow(ctx, `
		INSERT INTO orders (account_id, client_order_id, symbol, side, type, time_in_force, quantity, limit_price)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (account_id, client_order_id) WHERE client_order_id IS NOT NULL DO NOTHING
		RETURNING `+columns,
		r.AccountID, clientOrderID, r.Symbol, r.Side, r.Type, r.TimeInForce, r.Quantity, r.LimitPrice))
	if err == nil {
		return o, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Order{}, false, fmt.Errorf("create order: %w", err)
	}
	// A conflict waits for the other insert to commit, so the order is there.
	o, err = scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM orders WHERE account_id = $1 AND client_order_id = $2`,
		r.AccountID, r.ClientOrderID))
	if err != nil {
		return Order{}, false, fmt.Errorf("load order %q: %w", r.ClientOrderID, err)
	}
	if !o.sameAs(r) {
		return Order{}, false, fmt.Errorf("%w: %q", ErrClientOrderIDConflict, r.ClientOrderID)
	}
	return o, false, nil
}

// Accept moves a new order to accepted once its hold is placed. Day orders
// expire at expiresAt.
func Accept(ctx context.Context, tx pgx.Tx, id string, holdID int64, expiresAt time.Time) (Order, error) {
	var expires *time.Time
	if !expiresAt.IsZero() {
		expires = &expiresAt
	}
	return transition(ctx, tx, id, New, Accepted, `hold_id = $4, expires_at = $5`, holdID, expires)
}

// Reject moves a new order to rejected.
func Reject(ctx context.Context, tx pgx.Tx, id, reason string) (Order, error) {
	if reason == "" {
		return Order{}, fmt.Errorf("%w: a rejection needs a reason", ErrInvalid)
	}
	return transition(ctx, tx, id, New, Rejected, `reject_reason = $4, closed_at = now()`, reason)
}

// Fill moves an accepted order to filled.
func Fill(ctx context.Context, tx pgx.Tx, id string, price decimal.Decimal, entryID int64) (Order, error) {
	return transition(ctx, tx, id, Accepted, Filled, `fill_price = $4, fill_entry_id = $5, closed_at = now()`, price, entryID)
}

// Cancel moves an accepted order to canceled.
func Cancel(ctx context.Context, tx pgx.Tx, id string) (Order, error) {
	return transition(ctx, tx, id, Accepted, Canceled, `closed_at = now()`)
}

// Expire moves an accepted order to expired.
func Expire(ctx context.Context, tx pgx.Tx, id string) (Order, error) {
	return transition(ctx, tx, id, Accepted, Expired, `closed_at = now()`)
}

// transition is the compare-and-set behind every status change. set
// assigns the other columns the change sets, from $4 on.
func transition(ctx context.Context, tx pgx.Tx, id string, from, to Status, set string, args ...any) (Order, error) {
	o, err := scan(tx.QueryRow(ctx, `
		UPDATE orders SET status = $3, updated_at = now(), `+set+`
		WHERE id = $1 AND status = $2
		RETURNING `+columns,
		append([]any{id, from, to}, args...)...))
	if err == nil {
		return o, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Order{}, fmt.Errorf("%s order %s: %w", to, id, err)
	}
	if o, err = Get(ctx, tx, id); err != nil {
		return Order{}, err
	}
	if from == Accepted {
		return o, fmt.Errorf("%w: %s is %s", ErrNotOpen, id, o.Status)
	}
	return o, fmt.Errorf("%w: %s is %s", errNotNew, id, o.Status)
}

var idPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// checkID turns an ID that can't be an order's into ErrNotFound, rather than
// a Postgres syntax error.
func checkID(id string) error {
	if !idPattern.MatchString(id) {
		return fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return nil
}

// Lock locks an order's row for the rest of tx and returns it. Callers lock
// an order before touching its hold in the ledger, so a fill and a cancel of
// the same order never take their locks in different orders.
func Lock(ctx context.Context, tx pgx.Tx, id string) (Order, error) {
	if err := checkID(id); err != nil {
		return Order{}, err
	}
	o, err := scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM orders WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Order{}, fmt.Errorf("lock order %s: %w", id, err)
	}
	return o, nil
}

// Get returns an order.
func Get(ctx context.Context, q ledger.Querier, id string) (Order, error) {
	if err := checkID(id); err != nil {
		return Order{}, err
	}
	o, err := scan(q.QueryRow(ctx, `SELECT `+columns+` FROM orders WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Order{}, fmt.Errorf("load order %s: %w", id, err)
	}
	return o, nil
}

// MaxPageSize is the most orders List returns at once.
const MaxPageSize = 500

// List returns an account's orders, newest first: open ones only if open
// is set. before is the ID of the last order of the previous page, or empty
// for the first page.
func List(ctx context.Context, q ledger.Querier, accountID string, open bool, before string, limit int) ([]Order, error) {
	if limit < 1 || limit > MaxPageSize {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d, got %d", ErrInvalid, MaxPageSize, limit)
	}
	var cursor *string
	if before != "" {
		if err := checkID(before); err != nil {
			return nil, fmt.Errorf("%w: page cursor %q", ErrInvalid, before)
		}
		cursor = &before
	}
	rows, err := q.Query(ctx, `
		SELECT `+columns+` FROM orders
		WHERE account_id = $1
		  AND ($2 = false OR status = 'accepted')
		  AND ($3::uuid IS NULL OR (created_at, id) < (SELECT created_at, id FROM orders WHERE id = $3::uuid))
		ORDER BY created_at DESC, id DESC
		LIMIT $4`, accountID, open, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	list, err := pgx.CollectRows(rows, scanRow)
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	return list, nil
}

// OpenOrders returns every accepted order, oldest first.
func OpenOrders(ctx context.Context, q ledger.Querier) ([]Order, error) {
	rows, err := q.Query(ctx, `SELECT `+columns+` FROM orders WHERE status = 'accepted' ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("load open orders: %w", err)
	}
	list, err := pgx.CollectRows(rows, scanRow)
	if err != nil {
		return nil, fmt.Errorf("load open orders: %w", err)
	}
	return list, nil
}

// Expiring returns up to limit accepted orders that expire at or before now.
func Expiring(ctx context.Context, q ledger.Querier, now time.Time, limit int) ([]Order, error) {
	rows, err := q.Query(ctx, `
		SELECT `+columns+` FROM orders
		WHERE status = 'accepted' AND expires_at <= $1
		ORDER BY expires_at, id
		LIMIT $2`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("load expiring orders: %w", err)
	}
	list, err := pgx.CollectRows(rows, scanRow)
	if err != nil {
		return nil, fmt.Errorf("load expiring orders: %w", err)
	}
	return list, nil
}

package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/AmrMady/brokerd/internal/ledger"
	"github.com/AmrMady/brokerd/internal/orders"
)

// apiError is an error with the status and code a client gets for it.
type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.message }

func newError(status int, code, format string, args ...any) *apiError {
	return &apiError{status: status, code: code, message: fmt.Sprintf(format, args...)}
}

var (
	errUnauthorized = newError(http.StatusUnauthorized, "unauthorized", "missing or invalid API key")
	errNotFound     = newError(http.StatusNotFound, "not_found", "not found")
)

// errorFor maps an error from the domain packages to what the client sees.
// Anything it doesn't know is an internal error, and its message is kept
// from the client.
func errorFor(err error) *apiError {
	var e *apiError
	switch {
	case errors.As(err, &e):
		return e
	case errors.Is(err, ledger.ErrAccountNotFound), errors.Is(err, orders.ErrNotFound),
		errors.Is(err, ledger.ErrTransferNotFound):
		return newError(http.StatusNotFound, "not_found", "%s", err)
	case errors.Is(err, ledger.ErrInsufficientFunds):
		return newError(http.StatusUnprocessableEntity, "insufficient_funds", "%s", err)
	case errors.Is(err, ledger.ErrUnsettledFunds):
		return newError(http.StatusUnprocessableEntity, "unsettled_funds", "%s", err)
	case errors.Is(err, ledger.ErrAccountRestricted), errors.Is(err, ledger.ErrAccountClosed):
		return newError(http.StatusForbidden, "account_not_active", "%s", err)
	case errors.Is(err, ledger.ErrIdempotencyConflict), errors.Is(err, orders.ErrClientOrderIDConflict):
		return newError(http.StatusConflict, "idempotency_conflict", "%s", err)
	case errors.Is(err, orders.ErrNotOpen):
		return newError(http.StatusConflict, "order_not_open", "%s", err)
	case errors.Is(err, ledger.ErrInvalid), errors.Is(err, orders.ErrInvalid):
		return newError(http.StatusUnprocessableEntity, "invalid_request", "%s", err)
	default:
		return newError(http.StatusInternalServerError, "internal", "internal error")
	}
}

type errorBody struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

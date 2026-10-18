package api

import (
	"context"
	"crypto/rand"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
)

type transferJSON struct {
	ID        int64           `json:"id"`
	Direction string          `json:"direction"`
	Amount    decimal.Decimal `json:"amount"`
	Status    string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func transferOf(t ledger.Transfer) transferJSON {
	return transferJSON{t.ID, string(t.Direction), t.Amount, string(t.Status), t.CreatedAt, t.UpdatedAt}
}

type transferRequest struct {
	Amount decimal.Decimal `json:"amount"`
}

func (s *Server) deposit(w http.ResponseWriter, r *http.Request) error {
	return s.transfer(w, r, ledger.RequestDeposit)
}

func (s *Server) withdraw(w http.ResponseWriter, r *http.Request) error {
	return s.transfer(w, r, ledger.RequestWithdrawal)
}

// transfer moves cash in or out. This is paper money, so the bank confirms
// at once: the transfer is requested and completed in one transaction. The
// ledger still records it as pending first, as it would with a real bank.
// The request's Idempotency-Key, if it has one, is also the ledger's key, so
// a retry can't move the money twice.
func (s *Server) transfer(w http.ResponseWriter, r *http.Request,
	request func(context.Context, pgx.Tx, ledger.CashTransfer) (ledger.Transfer, error),
) error {
	var body transferRequest
	if err := decode(r, &body); err != nil {
		return err
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		key = rand.Text()
	}
	var t ledger.Transfer
	began := time.Now()
	err := pgx.BeginFunc(r.Context(), s.cfg.Pool, func(tx pgx.Tx) error {
		var err error
		t, err = request(r.Context(), tx, ledger.CashTransfer{
			IdempotencyKey: key, AccountID: r.PathValue("account"), Amount: body.Amount,
		})
		if err != nil || t.Status != ledger.TransferPending {
			return err
		}
		t, err = ledger.CompleteTransfer(r.Context(), tx, t.ID)
		return err
	})
	if err != nil {
		return err
	}
	s.cfg.ObservePost(time.Since(began))
	writeJSON(w, http.StatusCreated, transferOf(t))
	return nil
}

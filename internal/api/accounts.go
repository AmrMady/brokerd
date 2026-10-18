package api

import (
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"github.com/AmrMady/brokerd/internal/ledger"
)

type accountJSON struct {
	ID        string    `json:"id"`
	Number    string    `json:"number"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Cash      *cashJSON `json:"cash,omitempty"`
}

type cashJSON struct {
	Balance      decimal.Decimal `json:"balance"`
	Held         decimal.Decimal `json:"held"`
	Unsettled    decimal.Decimal `json:"unsettled"`
	Available    decimal.Decimal `json:"available"`
	Withdrawable decimal.Decimal `json:"withdrawable"`
}

func accountOf(a ledger.Account) accountJSON {
	return accountJSON{ID: a.ID, Number: a.Number, Status: string(a.Status), CreatedAt: a.CreatedAt}
}

// createAccount opens an account and returns it with its API key. The key
// is only ever shown here.
func (s *Server) createAccount(w http.ResponseWriter, r *http.Request) error {
	var (
		a   ledger.Account
		key string
	)
	err := pgx.BeginFunc(r.Context(), s.cfg.Pool, func(tx pgx.Tx) error {
		var err error
		if a, err = ledger.OpenAccount(r.Context(), tx); err != nil {
			return err
		}
		key, err = newKey(r.Context(), tx, a.ID)
		return err
	})
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusCreated, struct {
		Account accountJSON `json:"account"`
		APIKey  string      `json:"api_key"`
	}{accountOf(a), key})
	return nil
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("account")
	a, err := ledger.AccountByID(r.Context(), s.cfg.Pool, id)
	if err != nil {
		return err
	}
	balances, err := ledger.Balances(r.Context(), s.cfg.Pool, id)
	if err != nil {
		return err
	}
	out := accountOf(a)
	out.Cash = &cashJSON{}
	for _, b := range balances {
		if b.Asset == ledger.USD {
			out.Cash = &cashJSON{b.Balance, b.Held, b.Unsettled, b.Available, b.Withdrawable}
		}
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

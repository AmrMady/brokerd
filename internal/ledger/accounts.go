package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Account is a customer's brokerage account.
type Account struct {
	ID        string
	Number    string
	CreatedAt time.Time
}

const accountColumns = `id::text, number, created_at`

func scanAccount(row pgx.Row) (Account, error) {
	var a Account
	err := row.Scan(&a.ID, &a.Number, &a.CreatedAt)
	return a, err
}

// OpenAccount creates a customer account.
func OpenAccount(ctx context.Context, tx pgx.Tx) (Account, error) {
	a, err := scanAccount(tx.QueryRow(ctx, `
		INSERT INTO accounts (kind, number)
		VALUES ('customer', lpad(nextval('account_numbers')::text, 10, '0'))
		RETURNING `+accountColumns))
	if err != nil {
		return Account{}, fmt.Errorf("open account: %w", err)
	}
	return a, nil
}

// AccountByID returns the customer account with the given id.
func AccountByID(ctx context.Context, q Querier, id string) (Account, error) {
	if err := validateCustomer(id); err != nil {
		return Account{}, err
	}
	a, err := scanAccount(q.QueryRow(ctx, `
		SELECT `+accountColumns+` FROM accounts WHERE id = $1 AND kind = 'customer'`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, fmt.Errorf("%w: %s", ErrAccountNotFound, id)
	}
	if err != nil {
		return Account{}, fmt.Errorf("load account %s: %w", id, err)
	}
	return a, nil
}

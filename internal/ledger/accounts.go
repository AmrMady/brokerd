package ledger

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

// AccountStatus is the state of a customer account.
type AccountStatus string

// Account statuses. A restricted account can still receive deposits and
// settle orders it already has, but can't place holds or withdraw. Closed is
// final.
const (
	AccountActive     AccountStatus = "active"
	AccountRestricted AccountStatus = "restricted"
	AccountClosed     AccountStatus = "closed"
)

// Account is a customer's brokerage account.
type Account struct {
	ID        string
	Number    string
	Status    AccountStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

// StatusChange is a recorded change to an account's status.
type StatusChange struct {
	From      AccountStatus
	To        AccountStatus
	Reason    string
	CreatedAt time.Time
}

const accountColumns = `id::text, number, status, created_at, updated_at`

func scanAccount(row pgx.Row) (Account, error) {
	var a Account
	err := row.Scan(&a.ID, &a.Number, &a.Status, &a.CreatedAt, &a.UpdatedAt)
	return a, err
}

func getAccount(row pgx.Row, id string) (Account, error) {
	a, err := scanAccount(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, fmt.Errorf("%w: %s", ErrAccountNotFound, id)
	}
	if err != nil {
		return Account{}, fmt.Errorf("load account %s: %w", id, err)
	}
	return a, nil
}

// OpenAccount creates an active customer account.
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
	return getAccount(q.QueryRow(ctx, `
		SELECT `+accountColumns+` FROM accounts WHERE id = $1 AND kind = 'customer'`, id), id)
}

// SetAccountStatus moves an open account between active and restricted and
// records why.
func SetAccountStatus(ctx context.Context, tx pgx.Tx, id string, status AccountStatus, reason string) (Account, error) {
	if status != AccountActive && status != AccountRestricted {
		return Account{}, fmt.Errorf("%w: status must be active or restricted, got %q", ErrInvalid, status)
	}
	a, err := lockAccountForUpdate(ctx, tx, id, reason)
	if err != nil {
		return Account{}, err
	}
	switch a.Status {
	case AccountClosed:
		return Account{}, fmt.Errorf("%w: %s", ErrAccountClosed, id)
	case status:
		return a, nil
	default:
		return changeStatus(ctx, tx, a, status, reason)
	}
}

// CloseAccount closes an account whose balances and holds are all zero and
// records why. Closing a closed account is a no-op.
func CloseAccount(ctx context.Context, tx pgx.Tx, id, reason string) (Account, error) {
	a, err := lockAccountForUpdate(ctx, tx, id, reason)
	if err != nil || a.Status == AccountClosed {
		return a, err
	}

	var notEmpty bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM ledger_accounts
			WHERE account_id = $1 AND (balance <> 0 OR held <> 0)
		)`, id).Scan(&notEmpty)
	if err != nil {
		return Account{}, fmt.Errorf("check balances of %s: %w", id, err)
	}
	if notEmpty {
		return Account{}, fmt.Errorf("%w: %s", ErrAccountNotEmpty, id)
	}
	return changeStatus(ctx, tx, a, AccountClosed, reason)
}

// AccountStatusChanges returns an account's status history, oldest first.
func AccountStatusChanges(ctx context.Context, q Querier, id string) ([]StatusChange, error) {
	if err := validateCustomer(id); err != nil {
		return nil, err
	}
	rows, err := q.Query(ctx, `
		SELECT from_status, to_status, reason, created_at
		FROM account_status_changes
		WHERE account_id = $1
		ORDER BY id`, id)
	if err != nil {
		return nil, fmt.Errorf("load status history of %s: %w", id, err)
	}
	changes, err := pgx.CollectRows(rows, scanStatusChange)
	if err != nil {
		return nil, fmt.Errorf("load status history of %s: %w", id, err)
	}
	return changes, nil
}

func scanStatusChange(row pgx.CollectableRow) (StatusChange, error) {
	var c StatusChange
	err := row.Scan(&c.From, &c.To, &c.Reason, &c.CreatedAt)
	return c, err
}

// lockAccountForUpdate takes the exclusive lock status changes need. It waits
// for writes holding the shared lock (see lockAccount), so balances read
// afterwards can't change until the transaction ends.
func lockAccountForUpdate(ctx context.Context, tx pgx.Tx, id, reason string) (Account, error) {
	if err := validateCustomer(id); err != nil {
		return Account{}, err
	}
	if reason == "" {
		return Account{}, fmt.Errorf("%w: a reason is required", ErrInvalid)
	}
	return getAccount(tx.QueryRow(ctx, `
		SELECT `+accountColumns+` FROM accounts WHERE id = $1 AND kind = 'customer' FOR UPDATE`, id), id)
}

func changeStatus(ctx context.Context, tx pgx.Tx, a Account, to AccountStatus, reason string) (Account, error) {
	return getAccount(tx.QueryRow(ctx, `
		WITH change AS (
			INSERT INTO account_status_changes (account_id, from_status, to_status, reason)
			VALUES ($1, $2, $3, $4)
		)
		UPDATE accounts SET status = $3, updated_at = now()
		WHERE id = $1
		RETURNING `+accountColumns,
		a.ID, a.Status, to, reason), a.ID)
}

// lockAccount takes a shared lock on a customer account and returns its
// status. Writes take it before any other lock and hold it until commit, so
// status changes wait for writes in flight and later writes see the new
// status.
func lockAccount(ctx context.Context, tx pgx.Tx, id string) (AccountStatus, error) {
	var status AccountStatus
	err := tx.QueryRow(ctx, `
		SELECT status FROM accounts WHERE id = $1 AND kind = 'customer' FOR SHARE`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: %s", ErrAccountNotFound, id)
	}
	if err != nil {
		return "", fmt.Errorf("lock account %s: %w", id, err)
	}
	return status, nil
}

func requireStatus(id string, status AccountStatus, allowed ...AccountStatus) error {
	if slices.Contains(allowed, status) {
		return nil
	}
	if status == AccountClosed {
		return fmt.Errorf("%w: %s", ErrAccountClosed, id)
	}
	return fmt.Errorf("%w: %s", ErrAccountRestricted, id)
}

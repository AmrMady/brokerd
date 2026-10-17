package ledger

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// Balance is the current state of one asset in an account.
type Balance struct {
	AccountID string
	Asset     Asset
	Balance   decimal.Decimal
	Held      decimal.Decimal
	Available decimal.Decimal
	Version   int64
}

// Balances returns the balances of an account, ordered by asset.
func Balances(ctx context.Context, q Querier, accountID string) ([]Balance, error) {
	if err := validateAccountID(accountID); err != nil {
		return nil, err
	}
	rows, err := q.Query(ctx, `
		SELECT account_id::text, asset, balance, held, version
		FROM ledger_accounts
		WHERE account_id = $1
		ORDER BY asset`, accountID)
	if err != nil {
		return nil, fmt.Errorf("load balances of %s: %w", accountID, err)
	}
	balances, err := pgx.CollectRows(rows, scanBalance)
	if err != nil {
		return nil, fmt.Errorf("load balances of %s: %w", accountID, err)
	}
	return balances, nil
}

func scanBalance(row pgx.CollectableRow) (Balance, error) {
	var b Balance
	err := row.Scan(&b.AccountID, &b.Asset, &b.Balance, &b.Held, &b.Version)
	b.Available = b.Balance.Sub(b.Held)
	return b, err
}

// accountRef names a ledger account: one asset in one account.
type accountRef struct {
	account string
	asset   Asset
}

func (r accountRef) String() string {
	return r.account + "/" + string(r.asset)
}

func compareRefs(a, b accountRef) int {
	return cmp.Or(cmp.Compare(a.account, b.account), cmp.Compare(a.asset, b.asset))
}

// resolveAccounts returns ledger account ids for refs, creating any that
// don't exist yet.
func resolveAccounts(ctx context.Context, tx pgx.Tx, refs []accountRef) (map[accountRef]int64, error) {
	refs = slices.Clone(refs)
	slices.SortFunc(refs, compareRefs)
	refs = slices.Compact(refs)

	ids, err := lookupAccounts(ctx, tx, refs)
	if err != nil || len(ids) == len(refs) {
		return ids, err
	}
	var missing []accountRef
	for _, r := range refs {
		if _, ok := ids[r]; !ok {
			missing = append(missing, r)
		}
	}
	if err = createAccounts(ctx, tx, missing); err != nil {
		return nil, err
	}
	if ids, err = lookupAccounts(ctx, tx, refs); err != nil {
		return nil, err
	}
	for _, r := range refs {
		if _, ok := ids[r]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, r.account)
		}
	}
	return ids, nil
}

func lookupAccounts(ctx context.Context, q Querier, refs []accountRef) (map[accountRef]int64, error) {
	accounts := make([]string, len(refs))
	assets := make([]string, len(refs))
	for i, r := range refs {
		accounts[i], assets[i] = r.account, string(r.asset)
	}
	rows, err := q.Query(ctx, `
		SELECT la.id, la.account_id::text, la.asset
		FROM ledger_accounts la
		JOIN unnest($1::text[], $2::text[]) AS r(account_id, asset)
		  ON la.account_id = r.account_id::uuid AND la.asset = r.asset`,
		accounts, assets)
	if err != nil {
		return nil, fmt.Errorf("look up ledger accounts: %w", err)
	}

	ids := make(map[accountRef]int64, len(refs))
	var (
		id  int64
		ref accountRef
	)
	_, err = pgx.ForEachRow(rows, []any{&id, &ref.account, &ref.asset}, func() error {
		ids[ref] = id
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("look up ledger accounts: %w", err)
	}
	return ids, nil
}

// createAccounts inserts refs in their sorted order, so that concurrent
// transactions creating the same ledger accounts don't deadlock. Refs whose
// account doesn't exist are skipped.
func createAccounts(ctx context.Context, tx pgx.Tx, refs []accountRef) error {
	accounts := make([]string, len(refs))
	assets := make([]string, len(refs))
	for i, r := range refs {
		accounts[i], assets[i] = r.account, string(r.asset)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO ledger_accounts (account_id, kind, asset)
		SELECT a.id, a.kind, r.asset
		FROM unnest($1::text[], $2::text[]) WITH ORDINALITY AS r(account_id, asset, n)
		JOIN accounts a ON a.id = r.account_id::uuid
		ORDER BY r.n
		ON CONFLICT (account_id, asset) DO NOTHING`,
		accounts, assets)
	if err != nil {
		return fmt.Errorf("create ledger accounts: %w", err)
	}
	return nil
}

type change struct {
	amount decimal.Decimal
	held   decimal.Decimal
}

type lockedAccount struct {
	id      int64
	ref     accountRef
	kind    string
	balance decimal.Decimal
	held    decimal.Decimal
	version int64
}

func scanLockedAccount(row pgx.CollectableRow) (lockedAccount, error) {
	var a lockedAccount
	err := row.Scan(&a.id, &a.ref.account, &a.ref.asset, &a.kind, &a.balance, &a.held, &a.version)
	return a, err
}

// applyChanges locks the ledger accounts, checks that no customer account
// ends up with a negative available balance, and writes the new values. It
// returns the new version of each ledger account.
func applyChanges(ctx context.Context, tx pgx.Tx, changes map[int64]change) (map[int64]int64, error) {
	// Rows are locked in this order by every write, so writers can't
	// deadlock. System accounts go last because every customer shares them.
	// NO KEY UPDATE, because holds and journal lines reference these rows and
	// their foreign key checks take KEY SHARE locks, which FOR UPDATE blocks.
	rows, err := tx.Query(ctx, `
		SELECT id, account_id::text, asset, kind, balance, held, version
		FROM ledger_accounts
		WHERE id = ANY($1)
		ORDER BY kind, id
		FOR NO KEY UPDATE`,
		slices.Collect(maps.Keys(changes)))
	if err != nil {
		return nil, fmt.Errorf("lock ledger accounts: %w", err)
	}
	locked, err := pgx.CollectRows(rows, scanLockedAccount)
	if err != nil {
		return nil, fmt.Errorf("lock ledger accounts: %w", err)
	}
	if len(locked) != len(changes) {
		return nil, fmt.Errorf("lock ledger accounts: found %d of %d", len(locked), len(changes))
	}

	var (
		ids      = make([]int64, len(locked))
		balances = make([]decimal.Decimal, len(locked))
		helds    = make([]decimal.Decimal, len(locked))
		versions = make([]int64, len(locked))
		next     = make(map[int64]int64, len(locked))
	)
	for i, a := range locked {
		c := changes[a.id]
		balance := a.balance.Add(c.amount)
		held := a.held.Add(c.held)
		if held.IsNegative() {
			return nil, fmt.Errorf("%s: held amount would drop to %s", a.ref, held)
		}
		if a.kind != "system" && balance.LessThan(held) {
			return nil, fmt.Errorf("%w: %s would have %s available", ErrInsufficientFunds, a.ref, balance.Sub(held))
		}
		ids[i], balances[i], helds[i], versions[i] = a.id, balance, held, a.version+1
		next[a.id] = a.version + 1
	}

	_, err = tx.Exec(ctx, `
		UPDATE ledger_accounts AS la
		SET balance = u.balance, held = u.held, version = u.version, updated_at = now()
		FROM unnest($1::bigint[], $2::numeric[], $3::numeric[], $4::bigint[])
		     AS u(id, balance, held, version)
		WHERE la.id = u.id`,
		ids, balances, helds, versions)
	if err != nil {
		return nil, fmt.Errorf("update ledger accounts: %w", err)
	}
	return next, nil
}

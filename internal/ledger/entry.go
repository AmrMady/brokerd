package ledger

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

type posting struct {
	account    string // the customer account the idempotency key belongs to
	key        string // empty for postings made idempotent some other way
	kind       Kind
	holdID     int64
	transferID int64
	reverses   int64
	metadata   map[string]string
	lines      []Line
	request    []byte // what the idempotency key is checked against; hash() if nil
}

func validateLines(lines []Line) error {
	if len(lines) < 2 {
		return fmt.Errorf("%w: an entry needs at least two lines, got %d", ErrUnbalanced, len(lines))
	}
	seen := make(map[accountRef]bool, len(lines))
	sums := make(map[Asset]decimal.Decimal)
	for _, l := range lines {
		ref := accountRef{account: l.AccountID, asset: l.Asset}
		if l.AccountID == "" {
			return fmt.Errorf("%w: line without an account", ErrInvalid)
		}
		if err := l.Asset.validate(); err != nil {
			return err
		}
		if l.Amount.IsZero() {
			return fmt.Errorf("%w: zero amount for %s", ErrInvalid, ref)
		}
		if err := checkDecimal(ref.String()+" amount", l.Amount, maxScale); err != nil {
			return err
		}
		if seen[ref] {
			return fmt.Errorf("%w: %s appears twice in one entry", ErrInvalid, ref)
		}
		seen[ref] = true
		sums[l.Asset] = sums[l.Asset].Add(l.Amount)
	}
	for _, asset := range slices.Sorted(maps.Keys(sums)) {
		if !sums[asset].IsZero() {
			return fmt.Errorf("%w: %s lines sum to %s", ErrUnbalanced, asset, sums[asset])
		}
	}
	return nil
}

// hash has to be the same for retries of the same request, so lines are
// sorted and amounts normalized ("100" and "100.00" hash the same).
func (p posting) hash() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "kind %q\nhold %d\n", p.kind, p.holdID)
	for _, k := range slices.Sorted(maps.Keys(p.metadata)) {
		fmt.Fprintf(&b, "meta %q %q\n", k, p.metadata[k])
	}
	lines := slices.Clone(p.lines)
	sortLines(lines)
	for _, l := range lines {
		fmt.Fprintf(&b, "line %q %q %s\n", l.AccountID, l.Asset, l.Amount)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return sum[:]
}

func sortLines(lines []Line) {
	slices.SortFunc(lines, func(a, b Line) int {
		return cmp.Or(cmp.Compare(a.Asset, b.Asset), cmp.Compare(a.AccountID, b.AccountID))
	})
}

type pendingEntry struct {
	posting
	id        int64
	createdAt time.Time
	accounts  map[accountRef]int64
}

// prepareEntry validates p and creates any missing ledger accounts.
func prepareEntry(ctx context.Context, tx pgx.Tx, p posting) (*pendingEntry, error) {
	if err := validateLines(p.lines); err != nil {
		return nil, err
	}
	if err := checkScales(ctx, tx, p.lines); err != nil {
		return nil, err
	}
	if p.metadata == nil {
		p.metadata = map[string]string{}
	}
	refs := make([]accountRef, len(p.lines))
	for i, l := range p.lines {
		refs[i] = accountRef{account: l.AccountID, asset: l.Asset}
	}
	accounts, err := resolveAccounts(ctx, tx, refs)
	if err != nil {
		return nil, err
	}
	return &pendingEntry{posting: p, accounts: accounts}, nil
}

// insert writes the journal entry row and points its idempotency key, if it
// has one, at it.
func (p *pendingEntry) insert(ctx context.Context, tx pgx.Tx) error {
	err := tx.QueryRow(ctx, `
		INSERT INTO journal_entries (kind, hold_id, transfer_id, metadata)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at`,
		p.kind, nullable(p.holdID), nullable(p.transferID), p.metadata,
	).Scan(&p.id, &p.createdAt)
	if err != nil {
		return fmt.Errorf("insert journal entry: %w", err)
	}
	if p.key == "" {
		return nil
	}
	return recordKey(ctx, tx, p.account, p.key, keyResult{entryID: p.id})
}

// startEntry prepares p, claims its idempotency key and inserts the journal
// entry. If the same request was already posted under the key, it returns
// the existing entry and a nil pendingEntry.
func startEntry(ctx context.Context, tx pgx.Tx, p posting) (*pendingEntry, Entry, error) {
	pending, err := prepareEntry(ctx, tx, p)
	if err != nil {
		return nil, Entry{}, err
	}
	hash := p.request
	if hash == nil {
		hash = p.hash()
	}
	claimed, err := claimKey(ctx, tx, p.account, p.key, hash)
	if err != nil {
		return nil, Entry{}, err
	}
	if !claimed {
		var existing Entry
		existing, err = replayEntry(ctx, tx, p.account, p.key, hash)
		return nil, existing, err
	}
	if err = pending.insert(ctx, tx); err != nil {
		return nil, Entry{}, err
	}
	return pending, Entry{}, nil
}

func replayEntry(ctx context.Context, tx pgx.Tx, account, key string, hash []byte) (Entry, error) {
	used, err := usedKey(ctx, tx, account, key, hash)
	if err != nil {
		return Entry{}, err
	}
	if used.entryID == 0 {
		return Entry{}, fmt.Errorf("%w: %q", ErrIdempotencyConflict, key)
	}
	return loadEntry(ctx, tx, used.entryID)
}

func (p *pendingEntry) accountID(l Line) int64 {
	return p.accounts[accountRef{account: l.AccountID, asset: l.Asset}]
}

// finish updates the balances with the entry's lines plus extra held and
// unsettled changes (by ledger account id), and writes the journal lines.
func (p *pendingEntry) finish(ctx context.Context, tx pgx.Tx, extra map[int64]change) (Entry, error) {
	changes := make(map[int64]change, len(p.lines))
	for _, l := range p.lines {
		changes[p.accountID(l)] = change{amount: l.Amount}
	}
	for id, e := range extra {
		c := changes[id]
		c.held, c.unsettled = e.held, e.unsettled
		changes[id] = c
	}
	versions, err := applyChanges(ctx, tx, changes)
	if err != nil {
		return Entry{}, err
	}

	var (
		accountIDs   = make([]int64, len(p.lines))
		assets       = make([]string, len(p.lines))
		amounts      = make([]decimal.Decimal, len(p.lines))
		lineVersions = make([]int64, len(p.lines))
	)
	for i, l := range p.lines {
		accountIDs[i] = p.accountID(l)
		assets[i] = string(l.Asset)
		amounts[i] = l.Amount
		lineVersions[i] = versions[accountIDs[i]]
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO journal_lines (entry_id, ledger_account_id, asset, amount, account_version)
		SELECT $1, l.ledger_account_id, l.asset, l.amount, l.version
		FROM unnest($2::bigint[], $3::text[], $4::numeric[], $5::bigint[])
		     AS l(ledger_account_id, asset, amount, version)`,
		p.id, accountIDs, assets, amounts, lineVersions)
	if err != nil {
		return Entry{}, fmt.Errorf("insert journal lines: %w", err)
	}

	lines := slices.Clone(p.lines)
	sortLines(lines)
	return Entry{
		ID:              p.id,
		Kind:            p.kind,
		HoldID:          p.holdID,
		TransferID:      p.transferID,
		ReversesEntryID: p.reverses,
		Metadata:        p.metadata,
		Lines:           lines,
		CreatedAt:       p.createdAt,
	}, nil
}

func loadEntry(ctx context.Context, q Querier, id int64) (Entry, error) {
	e := Entry{ID: id}
	var holdID, transferID, reverses *int64
	err := q.QueryRow(ctx, `
		SELECT kind, hold_id, transfer_id, reverses_entry_id, metadata, created_at
		FROM journal_entries
		WHERE id = $1`, id,
	).Scan(&e.Kind, &holdID, &transferID, &reverses, &e.Metadata, &e.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Entry{}, fmt.Errorf("%w: %d", ErrEntryNotFound, id)
	}
	if err != nil {
		return Entry{}, fmt.Errorf("load journal entry %d: %w", id, err)
	}
	e.HoldID, e.TransferID, e.ReversesEntryID = deref(holdID), deref(transferID), deref(reverses)

	rows, err := q.Query(ctx, `
		SELECT la.account_id::text, l.asset, l.amount
		FROM journal_lines l
		JOIN ledger_accounts la ON la.id = l.ledger_account_id
		WHERE l.entry_id = $1`, id)
	if err != nil {
		return Entry{}, fmt.Errorf("load lines of journal entry %d: %w", id, err)
	}
	e.Lines, err = pgx.CollectRows(rows, scanLine)
	if err != nil {
		return Entry{}, fmt.Errorf("load lines of journal entry %d: %w", id, err)
	}
	sortLines(e.Lines)
	return e, nil
}

func scanLine(row pgx.CollectableRow) (Line, error) {
	var l Line
	err := row.Scan(&l.AccountID, &l.Asset, &l.Amount)
	return l, err
}

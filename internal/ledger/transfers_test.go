package ledger_test

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/AmrMady/brokerd/internal/ledger"
)

func (l testLedger) request(request transferRequest, key, account, amount string) (ledger.Transfer, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Transfer, error) {
		return request(l.ctx, tx, ledger.CashTransfer{IdempotencyKey: key, AccountID: account, Amount: dec(amount)})
	})
}

func (l testLedger) mustRequest(t *testing.T, request transferRequest, key, account, amount string) ledger.Transfer {
	t.Helper()
	tr, err := l.request(request, key, account, amount)
	if err != nil {
		t.Fatalf("request transfer %s: %v", key, err)
	}
	return tr
}

func (l testLedger) complete(id int64) (ledger.Transfer, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Transfer, error) { return ledger.CompleteTransfer(l.ctx, tx, id) })
}

func (l testLedger) cancel(id int64) (ledger.Transfer, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Transfer, error) { return ledger.CancelTransfer(l.ctx, tx, id) })
}

func (l testLedger) bankReturn(id int64, reason string) (ledger.Transfer, error) {
	return inTx(l, func(tx pgx.Tx) (ledger.Transfer, error) { return ledger.ReturnTransfer(l.ctx, tx, id, reason) })
}

func wantStatus(t *testing.T, tr ledger.Transfer, want ledger.TransferStatus) {
	t.Helper()
	if tr.Status != want {
		t.Errorf("transfer %d is %s, want %s", tr.ID, tr.Status, want)
	}
}

func TestDepositLifecycle(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	d, err := l.request(ledger.RequestDeposit, "d1", alice, "100")
	noErr(t, err)
	wantStatus(t, d, ledger.TransferPending)
	l.wantBalance(t, alice, ledger.USD, "0", "0")

	d, err = l.complete(d.ID)
	noErr(t, err)
	wantStatus(t, d, ledger.TransferCompleted)
	// The bank telling us twice changes nothing.
	_, err = l.complete(d.ID)
	noErr(t, err)
	l.wantBalance(t, alice, ledger.USD, "100", "0")

	_, err = l.cancel(d.ID)
	wantErr(t, err, ledger.ErrTransferStatus)
	_, err = l.bankReturn(d.ID, "")
	wantErr(t, err, ledger.ErrInvalid)

	d, err = l.bankReturn(d.ID, "R01")
	noErr(t, err)
	wantStatus(t, d, ledger.TransferReturned)
	if d.ReturnReason != "R01" {
		t.Errorf("return reason = %q, want R01", d.ReturnReason)
	}
	_, err = l.bankReturn(d.ID, "R01")
	noErr(t, err)
	l.wantBalance(t, alice, ledger.USD, "0", "0")
	l.wantBalance(t, ledger.ExternalBank, ledger.USD, "0", "0")

	_, err = l.complete(123456)
	wantErr(t, err, ledger.ErrTransferNotFound)
	l.requireReconciled(t)
}

func TestWithdrawalLifecycle(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "100")

	// A pending withdrawal holds the cash, and canceling it gives it back.
	w1, err := l.request(ledger.RequestWithdrawal, "w1", alice, "30")
	noErr(t, err)
	l.wantBalance(t, alice, ledger.USD, "100", "30")
	w1, err = l.cancel(w1.ID)
	noErr(t, err)
	wantStatus(t, w1, ledger.TransferCanceled)
	l.wantBalance(t, alice, ledger.USD, "100", "0")
	_, err = l.complete(w1.ID)
	wantErr(t, err, ledger.ErrTransferStatus)

	_, err = l.request(ledger.RequestWithdrawal, "w2", alice, "100.01")
	wantErr(t, err, ledger.ErrInsufficientFunds)

	w3, err := l.request(ledger.RequestWithdrawal, "w3", alice, "40")
	noErr(t, err)
	_, err = l.complete(w3.ID)
	noErr(t, err)
	l.wantBalance(t, alice, ledger.USD, "60", "0")

	// The bank couldn't deliver it, so the money comes back.
	w3, err = l.bankReturn(w3.ID, "R03")
	noErr(t, err)
	wantStatus(t, w3, ledger.TransferReturned)
	l.wantBalance(t, alice, ledger.USD, "100", "0")
	l.requireReconciled(t)
}

// A deposit comes back after the customer has spent most of it. The ledger
// takes what's left, the firm's deficits account covers the rest, and the
// account is restricted.
func TestReturnedDepositAlreadySpent(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	d, err := l.deposit("d1", alice, "100")
	noErr(t, err)
	h := l.mustHold(t, "h1", alice, ledger.USD, "80")
	_, err = l.settle(ledger.Fill{
		ID: "f1", HoldID: h.ID, AccountID: alice, Symbol: "AAPL", Side: ledger.Buy,
		Quantity: dec("1"), Price: dec("80"), Fee: dec("0"),
	})
	noErr(t, err)

	_, err = l.bankReturn(d.ID, "R01")
	noErr(t, err)
	l.wantBalance(t, alice, ledger.USD, "0", "0")
	l.wantBalance(t, alice, "AAPL", "1", "0")
	l.wantBalance(t, ledger.Deficits, ledger.USD, "-80", "0")
	l.wantBalance(t, ledger.ExternalBank, ledger.USD, "0", "0")

	a, err := ledger.AccountByID(l.ctx, l.pool, alice)
	noErr(t, err)
	if a.Status != ledger.AccountRestricted {
		t.Errorf("account is %s, want restricted", a.Status)
	}
	changes, err := ledger.AccountStatusChanges(l.ctx, l.pool, alice)
	noErr(t, err)
	if len(changes) != 1 || changes[0].Reason != "deposit returned: R01" {
		t.Errorf("status history = %+v, want one change for the returned deposit", changes)
	}
	l.requireReconciled(t)
}

func TestCloseAccountWithPendingTransfer(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)

	d, err := l.request(ledger.RequestDeposit, "d1", alice, "100")
	noErr(t, err)
	_, err = l.closeAccount(alice)
	wantErr(t, err, ledger.ErrAccountNotEmpty)
	_, err = l.cancel(d.ID)
	noErr(t, err)
	_, err = l.closeAccount(alice)
	noErr(t, err)
}

// The bank's "sent" and the customer's cancel race for one withdrawal, with
// some events repeated. Exactly one of them wins.
func TestCompleteRacesWithCancel(t *testing.T) {
	t.Parallel()
	l := newTestLedger(t)
	alice := l.openAccount(t)
	l.mustDeposit(t, "d1", alice, "1000")
	w := l.mustRequest(t, ledger.RequestWithdrawal, "w1", alice, "100")

	runConcurrently(20, func(i int) {
		var err error
		if i%2 == 0 {
			_, err = l.complete(w.ID)
		} else {
			_, err = l.cancel(w.ID)
		}
		if err != nil && !errors.Is(err, ledger.ErrTransferStatus) {
			t.Errorf("event %d: %v", i, err)
		}
	})

	w, err := ledger.TransferByID(l.ctx, l.pool, w.ID)
	noErr(t, err)
	switch w.Status {
	case ledger.TransferCompleted:
		l.wantBalance(t, alice, ledger.USD, "900", "0")
	case ledger.TransferCanceled:
		l.wantBalance(t, alice, ledger.USD, "1000", "0")
	default:
		t.Errorf("withdrawal is %s, want completed or canceled", w.Status)
	}
	l.requireReconciled(t)
}

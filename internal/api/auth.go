package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
)

// keyPrefix marks brokerd API keys, so a leaked one is easy to recognize.
const keyPrefix = "bk_"

// newKey stores a new API key for an account and returns it. Only its hash
// is kept.
func newKey(ctx context.Context, tx pgx.Tx, accountID string) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	key := keyPrefix + base64.RawURLEncoding.EncodeToString(b)
	hash := sha256.Sum256([]byte(key))
	if _, err := tx.Exec(ctx, `INSERT INTO api_keys (key_hash, account_id) VALUES ($1, $2)`, hash[:], accountID); err != nil {
		return "", fmt.Errorf("store API key: %w", err)
	}
	return key, nil
}

// authorized wraps a handler for /v1/accounts/{account}/...: the request
// must carry that account's API key. A key for another account gets the
// same answer as no account at all, so keys can't be used to find accounts.
func (s *Server) authorized(h handlerFunc) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request) error {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || !strings.HasPrefix(key, keyPrefix) {
			return errUnauthorized
		}
		hash := sha256.Sum256([]byte(key))
		var account string
		err := s.cfg.Pool.QueryRow(r.Context(), `SELECT account_id::text FROM api_keys WHERE key_hash = $1`, hash[:]).
			Scan(&account)
		if errors.Is(err, pgx.ErrNoRows) {
			return errUnauthorized
		}
		if err != nil {
			return fmt.Errorf("check API key: %w", err)
		}
		if account != r.PathValue("account") {
			return errNotFound
		}
		return h(w, r)
	}
}

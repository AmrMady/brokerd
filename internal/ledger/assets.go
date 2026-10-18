package ledger

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// AddAsset makes a symbol available to the ledger. scale is how many decimal
// places its quantities may have: 9 for fractional shares, 0 for whole shares
// only. Adding an asset again with the same scale is a no-op.
func AddAsset(ctx context.Context, tx pgx.Tx, symbol Asset, scale int32) error {
	if err := validateSymbol(symbol); err != nil {
		return err
	}
	if scale < 0 || scale > maxScale {
		return fmt.Errorf("%w: scale must be between 0 and %d, got %d", ErrInvalid, maxScale, scale)
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO assets (code, scale) VALUES ($1, $2) ON CONFLICT (code) DO NOTHING`, symbol, scale)
	if err != nil {
		return fmt.Errorf("add asset %s: %w", symbol, err)
	}

	var existing int32
	if err = tx.QueryRow(ctx, `SELECT scale FROM assets WHERE code = $1`, symbol).Scan(&existing); err != nil {
		return fmt.Errorf("add asset %s: %w", symbol, err)
	}
	if existing != scale {
		return fmt.Errorf("%w: %s already exists with scale %d", ErrInvalid, symbol, existing)
	}
	return nil
}

// checkScales rejects lines for unknown assets and amounts with more decimal
// places than their asset allows.
func checkScales(ctx context.Context, q Querier, lines []Line) error {
	codes := make([]string, len(lines))
	for i, l := range lines {
		codes[i] = string(l.Asset)
	}
	scales, err := assetScales(ctx, q, codes)
	if err != nil {
		return err
	}
	for _, l := range lines {
		scale, ok := scales[l.Asset]
		if !ok {
			return fmt.Errorf("%w: unknown asset %s", ErrInvalid, l.Asset)
		}
		if err = checkDecimal(string(l.Asset)+" amount", l.Amount, scale); err != nil {
			return err
		}
	}
	return nil
}

func assetScales(ctx context.Context, q Querier, codes []string) (map[Asset]int32, error) {
	rows, err := q.Query(ctx, `SELECT code, scale FROM assets WHERE code = ANY($1)`, codes)
	if err != nil {
		return nil, fmt.Errorf("load assets: %w", err)
	}
	scales := make(map[Asset]int32, len(codes))
	var (
		code  Asset
		scale int32
	)
	_, err = pgx.ForEachRow(rows, []any{&code, &scale}, func() error {
		scales[code] = scale
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load assets: %w", err)
	}
	return scales, nil
}

// AssetScale returns how many decimal places amounts of asset can have.
func AssetScale(ctx context.Context, q Querier, asset Asset) (int32, error) {
	scales, err := assetScales(ctx, q, []string{string(asset)})
	if err != nil {
		return 0, err
	}
	scale, ok := scales[asset]
	if !ok {
		return 0, fmt.Errorf("%w: unknown asset %s", ErrInvalid, asset)
	}
	return scale, nil
}

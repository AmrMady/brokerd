package ledger

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

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

// Package migrations embeds the SQL migrations.
package migrations

import "embed"

// FS contains the migration files.
//
//go:embed *.sql
var FS embed.FS

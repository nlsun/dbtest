package dbtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

type DBRow map[string]interface{}

type DBTableRows struct {
	Table string
	Rows  []DBRow
}

// For some reason some postgres containers will truncate
// nanoseconds and because of that break time comparisons inside tests.
//
// This happens when a timestamp generated inside golang code (with
// nanoseconds) gets sent to postgres and then the row is queried back out of
// postgres and compared against the original golang time.
func SafeTime(t time.Time) time.Time {
	return t.UTC().Round(time.Microsecond)
}

func InsertDBRows(t *testing.T, conn *pgxpool.Pool, rows DBTableRows) {
	InsertDBTables(t, conn, []DBTableRows{rows})
}

func InsertDBRowsWithCtx(ctx context.Context, t *testing.T, conn *pgxpool.Pool, rows DBTableRows) {
	InsertDBTablesWithCtx(ctx, t, conn, []DBTableRows{rows})
}

// Defaults to a 5 second context timeout
func InsertDBTables(t *testing.T, conn *pgxpool.Pool, tables []DBTableRows) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	InsertDBTablesWithCtx(ctx, t, conn, tables)
}

func InsertDBTablesWithCtx(ctx context.Context, t *testing.T, conn *pgxpool.Pool, tables []DBTableRows) {
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	for _, rows := range tables {
		insertDBRowsTx(ctx, t, tx, rows)
	}

	err = tx.Commit(ctx)
	require.NoError(t, err)
}

func insertDBRowsTx(ctx context.Context, t *testing.T, tx pgx.Tx, rows DBTableRows) {
	for _, row := range rows.Rows {
		errmsg := fmt.Sprintf("table: %s\nrow: %+v", rows.Table, row)
		var columns []string
		var values []interface{}
		for k, v := range row {
			columns = append(columns, k)
			values = append(values, v)
		}
		sql, args, err := squirrel.Insert(rows.Table).
			Columns(columns...).
			Values(values...).
			PlaceholderFormat(squirrel.Dollar).
			ToSql()
		require.NoError(t, err, errmsg)
		_, err = tx.Exec(ctx, sql, args...)
		require.NoError(t, err, errmsg)
	}
}

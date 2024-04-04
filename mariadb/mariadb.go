package mariadb

import (
	"context"
	"database/sql"
	"github.com/channelonerussia/db"
	"github.com/georgysavva/scany/v2/sqlscan"
)

type key string

const (
	TxKey key = "tx"
)

type mariadb struct {
	dbc *sql.DB
}

// NewDB нужна в качестве конструктора
func NewDB(dbc *sql.DB) db.DB {
	return &mariadb{
		dbc: dbc,
	}
}

// ScanOneContext нужна для осуществления и сканирования запроса в параметр dest
func (m *mariadb) ScanOneContext(ctx context.Context, dest interface{}, q db.Query, args ...interface{}) error {

	row, err := m.QueryContext(ctx, q, args...)

	if err != nil {
		return err
	}

	return sqlscan.ScanOne(dest, row)
}

func (m *mariadb) ScanAllContext(ctx context.Context, dest interface{}, q db.Query, args ...interface{}) error {
	row, err := m.QueryContext(ctx, q, args...)

	if err != nil {
		return err
	}

	return sqlscan.ScanAll(dest, row)
}

func (m *mariadb) ExecContext(ctx context.Context, q db.Query, args ...interface{}) (sql.Result, error) {
	tx, ok := ctx.Value(TxKey).(sql.Tx)
	if ok {
		return tx.ExecContext(ctx, q.QueryRaw, args...)
	}

	return m.dbc.ExecContext(ctx, q.QueryRaw, args...)
}

func (m *mariadb) QueryContext(ctx context.Context, q db.Query, args ...interface{}) (*sql.Rows, error) {
	tx, ok := ctx.Value(TxKey).(sql.Tx)
	if ok {
		return tx.QueryContext(ctx, q.QueryRaw, args...)
	}

	return m.dbc.QueryContext(ctx, q.QueryRaw, args...)
}

func (m *mariadb) QueryRowContext(ctx context.Context, q db.Query, args ...interface{}) *sql.Row {
	tx, ok := ctx.Value(TxKey).(sql.Tx)
	if ok {
		return tx.QueryRowContext(ctx, q.QueryRaw, args...)
	}

	return m.dbc.QueryRowContext(ctx, q.QueryRaw, args...)
}

func (m *mariadb) BeginTx(ctx context.Context, txOptions *sql.TxOptions) (*sql.Tx, error) {
	return m.dbc.BeginTx(ctx, txOptions)
}

func (m *mariadb) PingContext(ctx context.Context) error {
	return m.dbc.PingContext(ctx)
}

func (m *mariadb) Close() {
	m.dbc.Close()
}

func MakeContextTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, TxKey, tx)
}

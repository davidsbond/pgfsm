package pgfsm

import (
	"context"
	_ "embed"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func transaction(ctx context.Context, db *pgxpool.Pool, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}

	if err = fn(ctx, tx); err != nil {
		txErr := tx.Rollback(ctx)
		if errors.Is(txErr, pgx.ErrTxClosed) {
			return err
		}

		return errors.Join(err, txErr)
	}

	err = tx.Commit(ctx)
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}

	return err
}

func insert(ctx context.Context, tx pgx.Tx, encoder Encoding, cmd Command) error {
	data, err := encoder.Encode(cmd)
	if err != nil {
		return err
	}

	const q = `INSERT INTO pgfsm.command (kind, data) VALUES ($1, $2)`

	_, err = tx.Exec(ctx, q, cmd.Kind(), data)
	return err
}

type (
	record struct {
		id   uint64
		kind string
		data []byte
	}
)

func next(ctx context.Context, tx pgx.Tx, limit uint) ([]record, error) {
	const q = `
		DELETE FROM pgfsm.command WHERE id IN (
		    SELECT id FROM pgfsm.command 
			ORDER BY id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		) RETURNING id, kind, data
	`

	rows, err := tx.Query(ctx, q, limit)
	if err != nil {
		return nil, err
	}

	records := make([]record, 0, limit)
	defer rows.Close()

	for rows.Next() {
		var r record
		if err = rows.Scan(&r.id, &r.kind, &r.data); err != nil {
			return nil, err
		}

		records = append(records, r)
	}

	return records, rows.Err()
}

//go:embed migrate.sql
var migration string

func migrateUp(ctx context.Context, db *pgxpool.Pool) error {
	statements := strings.Split(migration, ";")

	for _, statement := range statements {
		if strings.TrimSpace(statement) == "" {
			continue
		}

		if _, err := db.Exec(ctx, statement); err != nil {
			return err
		}
	}

	return nil
}

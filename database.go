package pgfsm

import (
	"context"
	_ "embed"
	"errors"
	"log/slog"
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

func next(ctx context.Context, tx pgx.Tx) (int64, string, []byte, error) {
	const q = `
		SELECT id, kind, data FROM pgfsm.command 
		ORDER BY id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`

	var (
		id   int64
		kind string
		data []byte
	)

	if err := tx.QueryRow(ctx, q).Scan(&id, &kind, &data); err != nil {
		return 0, "", []byte{}, err
	}

	return id, kind, data, nil
}

func remove(ctx context.Context, tx pgx.Tx, id int64) error {
	const q = `DELETE FROM pgfsm.command WHERE id = $1`

	_, err := tx.Exec(ctx, q, id)
	return err
}

//go:embed migrate.sql
var migration string

func migrateUp(ctx context.Context, db *pgxpool.Pool, logger *slog.Logger) error {
	logger.DebugContext(ctx, "performing migrations")

	statements := strings.Split(migration, ";")

	for _, statement := range statements {
		if strings.TrimSpace(statement) == "" {
			continue
		}

		logger.
			With(slog.String("statement", statement)).
			DebugContext(ctx, "executing statement")

		if _, err := db.Exec(ctx, statement); err != nil {
			return err
		}
	}

	return nil
}

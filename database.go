package pgfsm

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"log/slog"
	"strings"
)

func transaction(ctx context.Context, db *sql.DB, fn func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}

	if err = fn(ctx, tx); err != nil {
		txErr := tx.Rollback()
		if errors.Is(txErr, sql.ErrTxDone) {
			return err
		}

		return errors.Join(err, txErr)
	}

	err = tx.Commit()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}

	return err
}

func insert(ctx context.Context, tx *sql.Tx, encoder Encoding, cmd Command) error {
	data, err := encoder.Encode(cmd)
	if err != nil {
		return err
	}

	const q = `INSERT INTO pgfsm.command (kind, data) VALUES ($1, $2)`

	_, err = tx.ExecContext(ctx, q, cmd.Kind(), data)
	return err
}

func next(ctx context.Context, tx *sql.Tx) (int64, string, []byte, error) {
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

	if err := tx.QueryRowContext(ctx, q).Scan(&id, &kind, &data); err != nil {
		return 0, "", []byte{}, err
	}

	return id, kind, data, nil
}

func remove(ctx context.Context, tx *sql.Tx, id int64) error {
	const q = `DELETE FROM pgfsm.command WHERE id = $1`

	_, err := tx.ExecContext(ctx, q, id)
	return err
}

//go:embed migrate.sql
var migration string

func migrateUp(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	logger.DebugContext(ctx, "performing migrations")

	statements := strings.Split(migration, ";")

	for _, statement := range statements {
		if strings.TrimSpace(statement) == "" {
			continue
		}

		logger.
			With(slog.String("statement", statement)).
			DebugContext(ctx, "executing statement")

		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}

	return nil
}

package pgfsm

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	mpq "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

var (
	//go:embed migrations/*.sql
	migrations embed.FS
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

	const q = `INSERT INTO pgfsm_command (kind, data) VALUES ($1, $2)`

	_, err = tx.ExecContext(ctx, q, cmd.Kind(), data)
	return err
}

func next(ctx context.Context, tx *sql.Tx) (int64, string, []byte, error) {
	const q = `
		SELECT id, kind, data FROM pgfsm_command 
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
	const q = `DELETE FROM pgfsm_command WHERE id = $1`

	_, err := tx.ExecContext(ctx, q, id)
	return err
}

func migrateUp(db *sql.DB) error {
	source, err := iofs.New(migrations, "migrations")
	if err != nil {
		fmt.Println("source", err)
		return err
	}

	const (
		migrationsTable = "pgfsm_migration"
	)

	destination, err := mpq.WithInstance(db, &mpq.Config{
		MigrationsTable: migrationsTable,
	})
	if err != nil {
		fmt.Println("dest", err)
		return err
	}

	migration, err := migrate.NewWithInstance("iofs", source, "pgx", destination)
	if err != nil {
		fmt.Println("instance", err)
		return err
	}

	err = migration.Up()
	switch {
	case errors.Is(err, migrate.ErrNoChange):
		return nil
	case err != nil:
		fmt.Println("up", err)
		return err
	default:
		return nil
	}
}

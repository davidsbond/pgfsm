// Package pgfsm provides a low-level primitive for implementing finite-state machines using Postgres as an underlying
// store.
//
// Using a single table to read and write consumer-implemented commands, one can process commands in order, optionally
// returning an additional command to be queued up. As postgres is the underlying store, command processing can be
// distributed among multiple instances without the need for leadership among instances. Both reads and writes can
// be performed on any instance.
//
// This allows package consumers to build a reactive system with fault-tolerance when command handling fails,
// providing the ability to retry commands through the use of SQL transactions.
//
// Ideally, a single command should invoke a single action within systems built on top of this package. For example,
// performing an API call to retrieve data. Any follow-up action required to process the data returned should be invoked
// by a subsequent command.
package pgfsm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
)

type (
	// The FSM type represents the finite state machine that stores commands within a database and utilises transactions
	// to handle these commands in order. Because a database is being used as the command store, no leadership is
	// required among the FSM instances and commands will be processed in order using an ordinal identifier.
	//
	// Command implementations should be registered prior to calling FSM.Read using the RegisterCommand function to ensure
	// that commands can be processed.
	FSM struct {
		db               *sql.DB
		commandFactories map[string]func() any
		options          options
	}

	// The Command interface is used to describe types implemented by package consumers that represent the contents
	// of a command stored by the FSM. The implementation should be able to be encoded/decoded using your Encoding
	// implementation.
	Command interface {
		// The Kind method should return a string that indicates the type of the Command. This will inform the FSM
		// which Handler implementation to invoke when the command is read.
		//
		// This method's implementation must be safe to run even if the Command's underlying value is nil. Otherwise,
		// a nil pointer dereference may occur. The FSM will instantiate a new instance of the concrete type in order
		// to call this method. Ideally, use a value rather than a pointer when implementing and referencing your
		// Command implementations.
		Kind() string
	}

	// The UnknownCommandError type is an error implementation used to denote that the FSM has read a command
	// it has no handler for. This error is produced by default. To skip unknown commands, use the SkipUnknownCommands
	// Option when calling New. Note that skipping unknown commands will still cause them to be deleted within the
	// database.
	UnknownCommandError struct {
		// The Command type the FSM does not recognise.
		Kind string
	}
)

func (e UnknownCommandError) Error() string {
	return fmt.Sprintf("unknown command type %q", e.Kind)
}

// New returns a new instance of the FSM type that will read commands from the provided sql.DB instance. This function
// will perform database migrations to ensure that the required tables exist within the database. These database objects
// will reside in their own schema named pgfsm. The user connecting to the database for this function will require
// the necessary permissions to create database objects.
//
// You can also provide zero or more Option functions to modify the behaviour of the FSM. Please see the Option type
// for specifics.
func New(ctx context.Context, db *sql.DB, options ...Option) (*FSM, error) {
	opts := defaultOptions()
	for _, o := range options {
		o(&opts)
	}

	if err := migrateUp(ctx, db, opts.logger); err != nil {
		return nil, err
	}

	return &FSM{
		db:               db,
		options:          opts,
		commandFactories: make(map[string]func() any),
	}, nil
}

// RegisterCommand registers a Command implementation with the FSM. This function must be called for each of your
// Command implementations so that the FSM knows how to decode them. This function's parameterized type must be the
// value of your Command implementation.
//
// For example:
//
// pgfsm.RegisterCommand[MyCommand](fsm)
func RegisterCommand[T Command](fsm *FSM) {
	var cmd T

	fsm.commandFactories[cmd.Kind()] = func() any {
		var out T
		return &out
	}
}

// Write a Command to the FSM. This Command will be encoded using the Encoding implementation and stored within the
// database, where it can then be read and the relevant Handler invoked.
func (fsm *FSM) Write(ctx context.Context, cmd Command) error {
	return transaction(ctx, fsm.db, func(ctx context.Context, tx *sql.Tx) error {
		switch command := cmd.(type) {
		case batchCommand:
			for _, cmd = range command {
				fsm.options.logger.
					With(slog.String("command_kind", cmd.Kind())).
					InfoContext(ctx, "writing command")

				if err := insert(ctx, tx, fsm.options.encoder, cmd); err != nil {
					return err
				}
			}
		default:
			fsm.options.logger.
				With(slog.String("command_kind", cmd.Kind())).
				InfoContext(ctx, "writing command")

			return insert(ctx, tx, fsm.options.encoder, cmd)
		}

		return nil
	})
}

type (
	// The Handler type is a function used with the FSM.Read method and is invoked per-command read by the FSM. A
	// type switch should be used on the cmd parameter for your individual Command implementations as a pointer of
	// the concrete type.
	Handler func(ctx context.Context, cmd any) (Command, error)
)

// Read commands from the FSM. For each command read, the provided Handler implementation will be invoked. This method
// blocks until the provided context is cancelled, or the Handler implementation returns an error. The Handler is
// intended to be used as a single entrypoint for commands. This method should use a type switch on the pointer types
// of your commands and react accordingly.
func (fsm *FSM) Read(ctx context.Context, h Handler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := fsm.next(ctx, h); err != nil {
				return err
			}
		}
	}
}

func (fsm *FSM) next(ctx context.Context, h Handler) error {
	return transaction(ctx, fsm.db, func(ctx context.Context, tx *sql.Tx) error {
		id, kind, data, err := next(ctx, tx)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return err
		}

		log := fsm.options.logger.With(
			slog.String("command_kind", kind),
			slog.Int64("command_id", id),
		)

		factory, ok := fsm.commandFactories[kind]
		switch {
		case !ok && fsm.options.skipUnknownCommands:
			log.WarnContext(ctx, "skipping unknown command")
			return remove(ctx, tx, id)
		case !ok && !fsm.options.skipUnknownCommands:
			return UnknownCommandError{Kind: kind}
		}

		cmd := factory()
		if err = fsm.options.encoder.Decode(data, cmd); err != nil {
			return fmt.Errorf("failed to decode command %d: %w", id, err)
		}

		log.InfoContext(ctx, "handling command")
		returned, err := h(ctx, cmd)
		if err != nil {
			log.ErrorContext(ctx, "error handling command")
			return err
		}

		if returned != nil {
			switch command := returned.(type) {
			case batchCommand:
				for _, batched := range command {
					log.With(slog.String("received_command_kind", batched.Kind())).
						InfoContext(ctx, "received additional command")

					if err = insert(ctx, tx, fsm.options.encoder, batched); err != nil {
						return err
					}
				}

			default:
				log.With(slog.String("received_command_kind", returned.Kind())).
					InfoContext(ctx, "received additional command")

				if err = insert(ctx, tx, fsm.options.encoder, command); err != nil {
					return err
				}
			}
		}

		return remove(ctx, tx, id)
	})
}

type (
	options struct {
		skipUnknownCommands bool
		encoder             Encoding
		logger              *slog.Logger
	}

	// The Option type is a function that can modify the behaviour of the FSM.
	Option func(*options)
)

func defaultOptions() options {
	return options{
		skipUnknownCommands: false,
		encoder:             &JSON{},
		logger:              slog.New(slog.DiscardHandler),
	}
}

// SkipUnknownCommands is an Option implementation that modifies the behaviour of the FSM so that it does not fail
// when reading a Command that it has no associated Handler for. This Option will cause any unknown commands to be
// removed from the database when read. By default, the FSM will return an error when encountering a Command it has
// no Handler for.
func SkipUnknownCommands() Option {
	return func(o *options) { o.skipUnknownCommands = true }
}

// UseEncoding is an Option implementation that modifies the Encoding implementation the FSM will use to encode/decode
// Command implementations. This allows you to modify the binary representation of your Command implementations which
// is stored within the database.
//
// This package provides a JSON and GOB implementation that use encoding/json and encoding/gob respectively.
// By default, the FSM will use the JSON.
func UseEncoding(e Encoding) Option {
	return func(o *options) { o.encoder = e }
}

// UseLogger is an Option implementation that modifies the logger used by the FSM. By default, the FSM uses
// slog.DiscardHandler and will not write any logs.
func UseLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l.WithGroup("pgfsm") }
}

type (
	batchCommand []Command
)

func (cmd batchCommand) Kind() string {
	return ""
}

// Batch returns a single Command implementation that wraps multiple other Command implementations. This can be used
// to return multiple commands at once when returning from a CommandHandler function. Or to send multiple commands to
// the FSM at once using FSM.Write.
func Batch(commands ...Command) Command {
	return batchCommand(commands)
}

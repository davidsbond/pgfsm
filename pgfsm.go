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
	// Handler implementations should be registered prior to calling FSM.Read to ensure commands can be processed.
	FSM struct {
		db       *sql.DB
		handlers map[string]Handler
		options  options
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
// will perform database migrations to ensure that the required tables exist within the database. This table is named
// pgfsm_migration which hopefully will never conflict with an existing table within your chosen database.
//
// You can also provide zero or more Option functions to modify the behaviour of the FSM. Please see the Option type
// for specifics.
func New(db *sql.DB, options ...Option) (*FSM, error) {
	if err := migrateUp(db); err != nil {
		return nil, err
	}

	opts := defaultOptions()
	for _, o := range options {
		o(&opts)
	}

	return &FSM{
		db:       db,
		options:  opts,
		handlers: make(map[string]Handler),
	}, nil
}

// Handle registers a Handler implementation with the FSM which will be invoked when a Command with a matching kind is
// read from the database. This method should be called with all your handlers prior to calling FSM.Read.
func (fsm *FSM) Handle(handler Handler) {
	fsm.handlers[handler.kind()] = handler
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

// Read commands from the FSM. For each command read, the relevant Handler implementation will be invoked. This method
// blocks until the provided context is cancelled, or a Handler implementation returns an error.
func (fsm *FSM) Read(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := fsm.next(ctx); err != nil {
				return err
			}
		}
	}
}

func (fsm *FSM) next(ctx context.Context) error {
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

		h, ok := fsm.handlers[kind]
		switch {
		case !ok && fsm.options.skipUnknownCommands:
			log.WarnContext(ctx, "skipping unknown command")
			return remove(ctx, tx, id)
		case !ok && !fsm.options.skipUnknownCommands:
			return UnknownCommandError{Kind: kind}
		}

		log.InfoContext(ctx, "handling command")
		cmd, err := h.handle(ctx, fsm.options.encoder, data)
		if err != nil {
			log.ErrorContext(ctx, "error handling command")
			return err
		}

		if cmd != nil {

			switch command := cmd.(type) {
			case batchCommand:
				for _, cmd = range command {
					log.With(slog.String("received_command_kind", cmd.Kind())).
						InfoContext(ctx, "received additional command")

					if err = insert(ctx, tx, fsm.options.encoder, cmd); err != nil {
						return err
					}
				}

			default:
				log.With(slog.String("received_command_kind", cmd.Kind())).
					InfoContext(ctx, "received additional command")

				err = insert(ctx, tx, fsm.options.encoder, cmd)
			}

			if err != nil {
				return err
			}
		}

		return remove(ctx, tx, id)
	})
}

type (
	// The Handler interface describes types that can process and decode individual commands read from the
	// database. This interface isn't intended to be implemented by consumers of this package. Rather, they
	// should wrap their handler function using the NewHandler method, which allows you to maintain type
	// safety by setting an Encoding on the FSM.
	Handler interface {
		handle(context.Context, Encoding, []byte) (Command, error)
		kind() string
	}

	// The CommandHandler type is a function that consumers of this package are expected to implement and register
	// with the FSM in order to handle their self-defined Command implementations. This function will be invoked each
	// time the FSM finds a Command with a matching kind and allows an optional command to be returned in response
	// to the one read.
	//
	// Returning a nil Command should be used to finish handling a chain of commands. If this function returns an
	// error, the read command will remain within the FSM and the error will be propagated up to the FSM.Read method,
	// causing the FSM to terminate.
	CommandHandler[T Command] func(context.Context, T) (Command, error)
)

func (ch CommandHandler[T]) handle(ctx context.Context, encoder Encoding, p []byte) (Command, error) {
	var input T

	if err := encoder.Decode(p, &input); err != nil {
		return nil, err
	}

	return ch(ctx, input)
}

func (ch CommandHandler[T]) kind() string {
	var cmd T

	// Note: Your IDE may complain of a possible nil pointer dereference here. This won't be an issue when Command
	// implementations are values. If they are used as pointers, the method will still be called. A nil pointer
	// dereference could occur if the Kind implementation attempts to construct the string using member fields but
	// really these should always be constants.
	return cmd.Kind()
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

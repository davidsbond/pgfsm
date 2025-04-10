# pgfsm

[![Go Reference](https://pkg.go.dev/badge/github.com/davidsbond/pgfsm.svg)](https://pkg.go.dev/github.com/davidsbond/pgfsm) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/davidsbond/pgfsm/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/davidsbond/pgfsm/actions/workflows/go.yml)

A Go package for building finite-state machines using PostgreSQL.

## How it works

This package revolves around the idea of a `Command`. A `Command` typically represents a single operation that needs to
be acted upon in order to modify the state of the FSM. It is a simple interface that only requires a single method (
named `Kind`) that returns a string representing the kind of command. Commands must also be capable of being encoded
into a binary representation, which limits them to basic types and structures.

Each time you write a command to the FSM. It is encoded and stored within the `pgfsm.command` table. This table is
created automatically when calling `pgfsm.New`. This package uses its own schema `pgfsm` to keep the table isolated from
any other tables applications built on top of this package may use.

Commands use an ordinal identifier to ensure that they're processed in order of insertion. This is an implementation
detail not exposed to users of the package. Once processed, their entry in the `pgfsm.command` table is removed,
providing once-only processing of individual commands. When using a concurrency of greater than one, this package
provides at-least-once processing of commands in batches. Batches of commands are dependent on eachother as they are
handled within the same transaction. If a single command within the batch fails, all commands in the batch are returned
to the database.

This package uses a registration system for commands utilising parameterised types that allows commands to be directly
decoded into their concrete types without excessive usage of reflection.

When handling a command, you have the option to return a command as a result. This allows commands to act as a graph of
sorts. Where the successful processing of one command creates zero or more child commands. This can be used to define
complex step-by-step behaviour where retries are required while maintaining ordering. If one command is dependent on the
processing of another, this command can never be processed without the prior one.

Because Postgres is the underlying store, all `FSM` instances can read and write commands, distributing the execution of
state changes that commands represent across all instances. Using `FOR UPDATE SKIP LOCKED` allows instances to process
different commands in parallel. A single instance will lock a command while it is being processed.

## Example

This section tries to outline a simple example of a system that takes in addresses and performs a lookup on the GPS
coordinates of those addresses.

Firstly, lets create a new `FSM` using the `New` function:

```go
fsm, err := pgfsm.New(ctx, db)
if err != nil {
// Handle errors.
}
```

Next, lets define a command that denotes a new address has been added:

```go
type SaveAddressCommand struct {
    ID      string
    Address string
}

func (SaveAddressCommand) Kind() string {
    return "address:save"
}
```

The `SaveAddressCommand` implements the `pgfsm.Command` interface by providing the `Kind` method. There's no rules on
how to format your command kinds. I've used a `noun:verb` style here.

Once we've defined a command, we need to register it with the `FSM` so it can be decoded:

```go
pgfsm.RegisterCommand[SaveAddressCommand](fsm)
```

Now, we can write this command using the `fsm.Write` method:

```go
cmd := SaveAddressCommand{
    ID:      "some-id",
    Address: "123 Fake Street"
}

if err := fsm.Write(ctx, cmd); err != nil {
// Handle errors.
}
```

The above outlines the process of defining and writing commands. But how do we then act upon them? This is achieved
using the `FSM.Read` method which accepts a handler function. Using a type switch, we can determine the command we're
handling and act accordingly:

```go
err = fsm.Read(ctx, func (ctx context.Context, cmd any) (pgfsm.Command, error) {
    switch command := cmd.(type) {
        case *SaveAddressCommand:
            // This is a function you would provide that handles the command and optionally returns one.
            return handleSaveAddress(ctx, cmd)
        default:
            return nil, nil
    }
})
```

Let's take a look at what this `handleSaveAddress` function does. This is a function you would implement yourself to act
upon a command and modify your state accordingly. In this case, we want to save the address details, then return a
command indicating that this address should have its GPS coordinates looked-up.

For this, we'll need to define a new command:

```go
type LookupAddressGPSCommand {
    AddressID string
}

func (LookupAddressCommand) Kind() string {
    return "address:lookup:gps"
}
```

Then we register it:

```go
pgfsm.RegisterCommand[LookupAddressGPSCommand](fsm)
```

Then we can return it from the `handleSaveAddress` function:

```go
func handleSaveAddress(ctx context.Context, command *SaveAddressCommand) (pgfsm.Command, error) {
    // Some code can go here to insert the address details into some table.
    
    return LookupAddressGPSCommand{AddressID: command.ID}, nil
}
```

From here, it is hopefully clear how you draw the rest of the owl. You handle the `LookupAddressGPSCommand` in the same
fashion, this function performing an API call of some sort that gets you the GPS data and return another command which,
when handled, inserts the GPS data into the relevant table.

If you want a handler function to return multiple commands at once, you can use the `pgfsm.Batch` function, which
converts one or more commands into a single `pgfsm.Command` implementation.

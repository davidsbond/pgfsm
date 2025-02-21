package pgfsm_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/davidsbond/pgfsm"
)

type (
	TestCommandA struct {
		Foo int `json:"foo"`
	}

	TestCommandB struct {
		Foo int `json:"foo"`
	}

	TestCommandC struct {
		Foo int `json:"foo"`
	}
)

func (tc TestCommandA) Kind() string {
	return "TestCommandA"
}

func (tc TestCommandB) Kind() string {
	return "TestCommandB"
}

func (tc TestCommandC) Kind() string {
	return "TestCommandC"
}

func TestFSM_ReadWrite(t *testing.T) {
	t.Parallel()

	db := testDB(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	fsm, err := pgfsm.New(db,
		pgfsm.SkipUnknownCommands(),
		pgfsm.UseEncoding(&pgfsm.GOB{}),
		pgfsm.UseLogger(logger),
	)
	require.NoError(t, err)

	var (
		handledA bool
		handledB bool
		handledC bool
	)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*10)
	defer cancel()

	fsm.Handle(pgfsm.CommandHandler[TestCommandA](func(ctx context.Context, a TestCommandA) (pgfsm.Command, error) {
		handledA = true
		return TestCommandB{Foo: a.Foo + 1}, nil
	}))

	fsm.Handle(pgfsm.CommandHandler[TestCommandB](func(ctx context.Context, b TestCommandB) (pgfsm.Command, error) {
		handledB = true
		return TestCommandC{Foo: b.Foo + 1}, nil
	}))

	fsm.Handle(pgfsm.CommandHandler[TestCommandC](func(ctx context.Context, c TestCommandC) (pgfsm.Command, error) {
		handledC = true
		assert.EqualValues(t, 3, c.Foo)
		return nil, nil
	}))

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return fsm.Write(ctx, TestCommandA{Foo: 1})
	})

	group.Go(func() error {
		return fsm.Read(ctx)
	})

	err = group.Wait()
	if !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err)
	}

	assert.True(t, handledA)
	assert.True(t, handledB)
	assert.True(t, handledC)
}

func testDB(t *testing.T) *sql.DB {
	t.Helper()

	u := &url.URL{
		Scheme:   "postgres",
		Host:     "localhost:5432",
		User:     url.UserPassword("postgres", "postgres"),
		Path:     "postgres",
		RawQuery: "sslmode=disable",
	}

	db, err := sql.Open("postgres", u.String())
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	require.NoError(t, db.PingContext(t.Context()))

	return db
}

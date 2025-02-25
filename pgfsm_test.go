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

	pgfsm.RegisterCommand[TestCommandA](fsm)
	pgfsm.RegisterCommand[TestCommandB](fsm)
	pgfsm.RegisterCommand[TestCommandC](fsm)

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return fsm.Write(ctx, pgfsm.Batch(
			TestCommandA{Foo: 1},
			TestCommandA{Foo: 1},
			TestCommandA{Foo: 1},
		))
	})

	group.Go(func() error {
		return fsm.Read(ctx, func(ctx context.Context, cmd any) (pgfsm.Command, error) {
			switch msg := cmd.(type) {
			case *TestCommandA:
				handledA = true
				return TestCommandB{Foo: msg.Foo + 1}, nil
			case *TestCommandB:
				handledB = true
				return pgfsm.Batch(
					TestCommandC{Foo: msg.Foo + 1},
					TestCommandC{Foo: msg.Foo + 1},
				), nil
			case *TestCommandC:
				handledC = true
				return nil, nil
			default:
				assert.Fail(t, "should be skipping unknown commands")
				return nil, nil
			}
		})
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

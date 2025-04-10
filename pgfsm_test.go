package pgfsm_test

import (
	"context"
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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

	fsm, err := pgfsm.New(t.Context(), db,
		pgfsm.SkipUnknownCommands(),
		pgfsm.UseEncoding(&pgfsm.GOB{}),
		pgfsm.PollInterval(time.Millisecond, time.Minute),
		pgfsm.SetConcurrency(10),
	)
	require.NoError(t, err)

	var (
		handledA atomic.Bool
		handledB atomic.Bool
		handledC atomic.Bool
	)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
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
				handledA.Store(true)
				return TestCommandB{Foo: msg.Foo + 1}, nil
			case *TestCommandB:
				handledB.Store(true)
				return pgfsm.Batch(
					TestCommandC{Foo: msg.Foo + 1},
					TestCommandC{Foo: msg.Foo + 1},
				), nil
			case *TestCommandC:
				handledC.Store(true)
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

	assert.True(t, handledA.Load())
	assert.True(t, handledB.Load())
	assert.True(t, handledC.Load())
}

func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	u := &url.URL{
		Scheme:   "postgres",
		Host:     "localhost:5432",
		User:     url.UserPassword("postgres", "postgres"),
		Path:     "postgres",
		RawQuery: "sslmode=disable",
	}

	db, err := pgxpool.New(t.Context(), u.String())
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	require.NoError(t, db.Ping(t.Context()))

	return db
}

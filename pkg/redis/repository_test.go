package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	testpkg "github.com/aqaliarept/go-ddd-kit/pkg/internal/test"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// redisTestAgg wraps TestAggWrapper and adds Namespace method for Redis
type redisTestAgg struct {
	testpkg.TestAggWrapper
}

func (r *redisTestAgg) Namespace() Namespace {
	return "test_agg"
}

// redisTestContainer wraps a Redis testcontainer
type redisTestContainer struct {
	container testcontainers.Container
	client    *redis.Client
}

// Client returns the Redis client
func (r *redisTestContainer) Client() *redis.Client {
	return r.client
}

// Cleanup terminates the container
func (r *redisTestContainer) Cleanup() {
	ctx := context.Background()
	if r.client != nil {
		//nolint:errcheck // Cleanup errors in test teardown are non-fatal
		_ = r.client.Close()
	}
	if r.container != nil {
		//nolint:errcheck // Cleanup errors in test teardown are non-fatal
		_ = r.container.Terminate(ctx)
	}
}

// SetupRedisTestContainer creates and starts a Redis testcontainer
func SetupRedisTestContainer(t *testing.T) *redisTestContainer {
	ctx := context.Background()

	// Start Redis container
	redisContainer, err := rediscontainer.Run(ctx,
		"redis:7-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second).
				WithPollInterval(1*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start Redis container: %v", err)
	}

	// Get connection string
	connStr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		termErr := redisContainer.Terminate(ctx)
		t.Fatalf("failed to get Redis connection string: %v", errors.Join(err, termErr))
	}

	// Parse connection string and create Redis client
	opt, err := redis.ParseURL(connStr)
	if err != nil {
		termErr := redisContainer.Terminate(ctx)
		t.Fatalf("failed to parse Redis connection string: %v", errors.Join(err, termErr))
	}

	client := redis.NewClient(opt)

	// Ping to verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		closeErr := client.Close()
		termErr := redisContainer.Terminate(ctx)
		t.Fatalf("failed to ping Redis: %v", errors.Join(err, closeErr, termErr))
	}

	return &redisTestContainer{
		container: redisContainer,
		client:    client,
	}
}

// redisTestRunner implements the TestRunner interface for Redis tests
type redisTestRunner struct {
	container *redisTestContainer
	repo      core.Repository
}

func (r *redisTestRunner) SetupRepository(t *testing.T) core.Repository {
	return r.repo
}

func (r *redisTestRunner) SetupContext(t *testing.T) context.Context {
	return context.Background()
}

func (r *redisTestRunner) NewAggregate(agg any) testpkg.TestAggregate {
	wrapper := testpkg.NewTestAggWrapper(agg)
	return &redisTestAgg{TestAggWrapper: *wrapper}
}

func TestRedisRepository(t *testing.T) {
	container := SetupRedisTestContainer(t)
	defer container.Cleanup()

	runner := &redisTestRunner{
		container: container,
		repo:      newRepository(container.Client()),
	}

	testpkg.RunBaseTests(t, runner)
}

func TestRedisRepository_Expiration(t *testing.T) {
	container := SetupRedisTestContainer(t)
	defer container.Cleanup()

	repo := newRepository(container.Client())
	ctx := context.Background()

	t.Run("Save new aggregate with expiration", func(t *testing.T) {
		baseAgg := testpkg.NewTestAgg("expire-test-id-1")
		wrapper := testpkg.NewTestAggWrapper(baseAgg)
		agg := &redisTestAgg{TestAggWrapper: *wrapper}
		_, err := agg.SingleEventCommand("expire-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg, WithExpiration(1*time.Second))
		require.NoError(t, err)

		baseLoadedAgg := testpkg.NewTestAgg("")
		loadedWrapper := testpkg.NewTestAggWrapper(baseLoadedAgg)
		loadedAgg := &redisTestAgg{TestAggWrapper: *loadedWrapper}
		err = repo.Load(ctx, "expire-test-id-1", loadedAgg)
		require.NoError(t, err)
		state := testpkg.GetState[testpkg.TestAggState](loadedAgg)
		require.Equal(t, "expire-value", state.String)

		time.Sleep(1100 * time.Millisecond)

		err = repo.Load(ctx, "expire-test-id-1", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run("Save existing aggregate with expiration", func(t *testing.T) {
		baseAgg := testpkg.NewTestAgg("expire-test-id-2")
		wrapper := testpkg.NewTestAggWrapper(baseAgg)
		agg := &redisTestAgg{TestAggWrapper: *wrapper}
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		_, err = agg.SingleEventCommand("updated-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg, WithExpiration(1*time.Second))
		require.NoError(t, err)

		baseLoadedAgg := testpkg.NewTestAgg("")
		loadedWrapper := testpkg.NewTestAggWrapper(baseLoadedAgg)
		loadedAgg := &redisTestAgg{TestAggWrapper: *loadedWrapper}
		err = repo.Load(ctx, "expire-test-id-2", loadedAgg)
		require.NoError(t, err)
		state := testpkg.GetState[testpkg.TestAggState](loadedAgg)
		require.Equal(t, "updated-value", state.String)

		time.Sleep(1100 * time.Millisecond)

		err = repo.Load(ctx, "expire-test-id-2", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run("Save aggregate without expiration persists", func(t *testing.T) {
		baseAgg := testpkg.NewTestAgg("no-expire-test-id")
		wrapper := testpkg.NewTestAggWrapper(baseAgg)
		agg := &redisTestAgg{TestAggWrapper: *wrapper}
		_, err := agg.SingleEventCommand("persistent-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		time.Sleep(500 * time.Millisecond)

		baseLoadedAgg := testpkg.NewTestAgg("")
		loadedWrapper := testpkg.NewTestAggWrapper(baseLoadedAgg)
		loadedAgg := &redisTestAgg{TestAggWrapper: *loadedWrapper}
		err = repo.Load(ctx, "no-expire-test-id", loadedAgg)
		require.NoError(t, err)
		state := testpkg.GetState[testpkg.TestAggState](loadedAgg)
		require.Equal(t, "persistent-value", state.String)
	})

	t.Run("WithExpiration panics on zero duration", func(t *testing.T) {
		// Verify that WithExpiration panics when zero duration is passed
		require.Panics(t, func() {
			WithExpiration(0)
		}, "WithExpiration should panic when zero duration is passed")

		// Verify that WithExpiration panics when negative duration is passed
		require.Panics(t, func() {
			WithExpiration(-1 * time.Second)
		}, "WithExpiration should panic when negative duration is passed")
	})

	t.Run("Update expiration on existing aggregate", func(t *testing.T) {
		baseAgg := testpkg.NewTestAgg("update-expire-test-id")
		wrapper := testpkg.NewTestAggWrapper(baseAgg)
		agg := &redisTestAgg{TestAggWrapper: *wrapper}
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		baseLoadedAgg := testpkg.NewTestAgg("")
		loadedWrapper := testpkg.NewTestAggWrapper(baseLoadedAgg)
		loadedAgg := &redisTestAgg{TestAggWrapper: *loadedWrapper}
		err = repo.Load(ctx, "update-expire-test-id", loadedAgg)
		require.NoError(t, err)

		_, err = agg.SingleEventCommand("updated-with-expire")
		require.NoError(t, err)
		err = repo.Save(ctx, agg, WithExpiration(1*time.Second))
		require.NoError(t, err)

		err = repo.Load(ctx, "update-expire-test-id", loadedAgg)
		require.NoError(t, err)
		state := testpkg.GetState[testpkg.TestAggState](loadedAgg)
		require.Equal(t, "updated-with-expire", state.String)

		time.Sleep(1100 * time.Millisecond)

		err = repo.Load(ctx, "update-expire-test-id", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run("Second save prolongs existing expiration", func(t *testing.T) {
		baseAgg := testpkg.NewTestAgg("prolong-expire-test-id")
		wrapper := testpkg.NewTestAggWrapper(baseAgg)
		agg := &redisTestAgg{TestAggWrapper: *wrapper}
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg, WithExpiration(1*time.Second))
		require.NoError(t, err)

		time.Sleep(600 * time.Millisecond)

		baseLoadedAgg := testpkg.NewTestAgg("")
		loadedWrapper := testpkg.NewTestAggWrapper(baseLoadedAgg)
		loadedAgg := &redisTestAgg{TestAggWrapper: *loadedWrapper}
		err = repo.Load(ctx, "prolong-expire-test-id", loadedAgg)
		require.NoError(t, err)
		state := testpkg.GetState[testpkg.TestAggState](loadedAgg)
		require.Equal(t, "initial-value", state.String)

		_, err = agg.SingleEventCommand("updated-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg, WithExpiration(2*time.Second))
		require.NoError(t, err)

		time.Sleep(600 * time.Millisecond)

		err = repo.Load(ctx, "prolong-expire-test-id", loadedAgg)
		require.NoError(t, err)
		updatedState := testpkg.GetState[testpkg.TestAggState](loadedAgg)
		require.Equal(t, "updated-value", updatedState.String)

		time.Sleep(1500 * time.Millisecond)

		err = repo.Load(ctx, "prolong-expire-test-id", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})
}

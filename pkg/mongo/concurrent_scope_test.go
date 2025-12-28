package mongo

import (
	"context"
	"errors"
	"testing"
	"time"

	core "github.com/aqaliarept/go-ddd/pkg/core"
	"github.com/avast/retry-go/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// MongoConcurrentScope and related types (using ConcurrentScope from core)
type ConcurrentScopeConfig struct {
	Retry RetryConfig
}

type RetryConfig struct {
	Attempts  uint
	Delay     time.Duration
	MaxJitter time.Duration
}

const (
	DefaultRetryAttempts  = 3
	DefaultRetryDelay     = 100 * time.Millisecond
	DefaultRetryMaxJitter = 50 * time.Millisecond
)

type MongoConcurrentScope struct {
	*core.ConcurrentScope
}

func NewMongoConcurrentScope(factory core.RepositoryFactory, config ConcurrentScopeConfig) *MongoConcurrentScope {
	retryOpts := []retry.Option{
		retry.Attempts(config.Retry.Attempts),
		retry.Delay(config.Retry.Delay),
		retry.MaxJitter(config.Retry.MaxJitter),
	}
	return &MongoConcurrentScope{
		ConcurrentScope: core.NewConcurrentScope(factory, retryOpts...),
	}
}

func TestMongoRepository_ConcurrentScope(t *testing.T) {
	container := SetupMongoTestContainer(t)
	defer container.Cleanup()

	// Use a specific database name
	database := container.Client().Database("user_management")
	factory := NewRepositoryFactory(database)
	config := ConcurrentScopeConfig{
		Retry: RetryConfig{
			Attempts:  DefaultRetryAttempts,
			Delay:     DefaultRetryDelay,
			MaxJitter: DefaultRetryMaxJitter,
		},
	}
	concurrentScope := NewMongoConcurrentScope(factory, config)

	t.Run("Overlapping commits - first commit succeeds, second fails", func(t *testing.T) {
		ctx := context.Background()

		id := core.ID(uuid.NewString())

		func() {
			cs := NewMongoConcurrentScope(factory, config)
			repo := cs.Factory.Create(ctx)

			tx, ok := repo.(core.Transactional)
			require.True(t, ok)

			tctx, err := tx.Begin(ctx)
			require.NoError(t, err)

			agg := newTestAgg(id)
			_, err = agg.SingleEventCommand("test-agg-value-0")
			require.NoError(t, err)

			err = repo.Save(tctx, agg)
			require.NoError(t, err)

			err = tx.Commit(tctx)
			require.NoError(t, err)
		}()

		// Transaction 0.
		cs0 := NewMongoConcurrentScope(factory, config)
		repo0 := cs0.Factory.Create(ctx)

		tx0, ok := repo0.(core.Transactional)
		require.True(t, ok)

		tctx, err := tx0.Begin(ctx)
		require.NoError(t, err)

		agg0 := &testAgg{}
		err = repo0.Load(tctx, id, agg0)
		require.NoError(t, err)

		_, err = agg0.SingleEventCommand("test-agg-value-0")
		require.NoError(t, err)

		// Transaction 1.
		cs1 := NewMongoConcurrentScope(factory, config)
		repo1 := cs1.Factory.Create(ctx)

		tx1, ok := repo1.(core.Transactional)
		require.True(t, ok)

		tctx, err = tx1.Begin(ctx)
		require.NoError(t, err)

		agg1 := &testAgg{}
		err = repo0.Load(tctx, id, agg1)
		require.NoError(t, err)

		_, err = agg1.SingleEventCommand("test-agg-value-1")
		require.NoError(t, err)

		// Here both aggregares both the same version,
		// but different state.
		err = repo0.Save(tctx, agg0)
		require.NoError(t, err)

		err = repo1.Save(tctx, agg1)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)

		// Commit transaction 0 - succeeds.
		err = tx0.Commit(tctx)
		require.NoError(t, err)

		// Commit transaction 1 - should fail.
		err = tx1.Commit(tctx)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrTransactionNotFound)
	})

	t.Run("Save aggregate within concurrent scope - retry success", func(t *testing.T) {
		ctx := context.Background()
		attempts := DefaultRetryAttempts - 1

		// Execute transaction that will fail using ConcurrentScope
		err := concurrentScope.Run(ctx,
			func(ctx context.Context, repo Repository) error {
				// Create and save aggregate within concurrent scope
				agg := newTestAgg("concurrent-scope-retry-success-id")
				_, err := agg.SingleEventCommand("concurrent-scope-retry-success-value")
				require.NoError(t, err)

				err = repo.Save(ctx, agg)
				require.NoError(t, err)

				// Verify aggregate is visible within concurrent scope
				loadedAgg := &testAgg{}
				err = repo.Load(ctx, "concurrent-scope-retry-success-id", loadedAgg)
				require.NoError(t, err)
				require.Equal(t, "concurrent-scope-retry-success-value", loadedAgg.State().String)

				attempts--
				if attempts >= 0 {
					// Return error to trigger retry
					return core.ErrConcurrentModification
				}

				return nil
			})
		require.NoError(t, err)

		// Verify aggregate is visible after transaction retry success
		repo := factory.Create(ctx)
		loadedAgg := &testAgg{}
		err = repo.Load(ctx, "concurrent-scope-retry-success-id", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, "concurrent-scope-retry-success-value", loadedAgg.State().String)
	})

	t.Run("Save aggregate within concurrent scope - retry failure", func(t *testing.T) {
		ctx := context.Background()

		// Execute transaction that will fail using ConcurrentScope
		err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			// Create and save aggregate within concurrent scope
			agg := newTestAgg("concurrent-scope-retry-id")
			_, err := agg.SingleEventCommand("concurrent-scope-retry-value")
			require.NoError(t, err)

			err = repo.Save(ctx, agg)
			require.NoError(t, err)

			// Verify aggregate is visible within concurrent scope
			loadedAgg := &testAgg{}
			err = repo.Load(ctx, "concurrent-scope-retry-id", loadedAgg)
			require.NoError(t, err)
			require.Equal(t, "concurrent-scope-retry-value", loadedAgg.State().String)

			// Return error to trigger retry
			return core.ErrConcurrentModification
		})
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification, "expected error to be ErrConcurrentModification")
		retryError := &retry.Error{}
		require.ErrorAs(t, err, retryError)
		require.ErrorIs(t, retryError, core.ErrConcurrentModification, "expected retry error to be ErrConcurrentModification")

		// Verify aggregate is NOT visible after transaction retry failure
		repo := factory.Create(ctx)
		loadedAgg := &testAgg{}
		err = repo.Load(ctx, "concurrent-scope-retry-id", loadedAgg)
		require.Error(t, err)
		require.Equal(t, core.ErrAggregateNotFound, err)
	})

	t.Run("Concurrent scope with aggregate updates and events", func(t *testing.T) {
		ctx := context.Background()

		// Execute transaction with aggregate updates using ConcurrentScope
		err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			// Create initial aggregate
			agg := newTestAgg("concurrent-scope-updates-id")
			_, err := agg.SingleEventCommand("initial-value")
			require.NoError(t, err)

			err = repo.Save(ctx, agg)
			require.NoError(t, err)

			// Load and update the aggregate within concurrent scope
			loadedAgg := &testAgg{}
			err = repo.Load(ctx, "concurrent-scope-updates-id", loadedAgg)
			require.NoError(t, err)
			require.Equal(t, "initial-value", loadedAgg.State().String)

			// Add more events
			_, err = loadedAgg.SingleEventCommand("updated-value")
			require.NoError(t, err)
			_, err = loadedAgg.SingleEventCommand("final-value")
			require.NoError(t, err)

			// Save the updated aggregate
			err = repo.Save(ctx, loadedAgg)
			require.NoError(t, err)

			// Verify the final state within concurrent scope
			finalAgg := &testAgg{}
			err = repo.Load(ctx, "concurrent-scope-updates-id", finalAgg)
			require.NoError(t, err)
			require.Equal(t, "final-value", finalAgg.State().String)

			return nil
		})
		require.NoError(t, err)

		// Verify the final state after transaction commit
		repo := factory.Create(ctx)
		finalAgg := &testAgg{}
		err = repo.Load(ctx, "concurrent-scope-updates-id", finalAgg)
		require.NoError(t, err)
		require.Equal(t, "final-value", finalAgg.State().String)
	})

	t.Run("Concurrent scope with concurrent access simulation", func(t *testing.T) {
		ctx := context.Background()

		// Create an aggregate outside concurrent scope
		repo := factory.Create(ctx)

		agg := newTestAgg("concurrent-scope-concurrent-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		// Execute transaction that tries to modify the same aggregate using ConcurrentScope
		err = concurrentScope.Run(ctx,
			func(ctx context.Context, repo Repository) error {
				// Load the aggregate within concurrent scope
				txAgg := &testAgg{}
				err = repo.Load(ctx, "concurrent-scope-concurrent-id", txAgg)
				require.NoError(t, err)

				// Simulate concurrent access by modifying outside concurrent scope
				outsideRepo := factory.Create(ctx)
				outsideAgg := &testAgg{}
				err = outsideRepo.Load(ctx, "concurrent-scope-concurrent-id", outsideAgg)
				require.NoError(t, err)

				_, err = outsideAgg.SingleEventCommand("outside-value")
				require.NoError(t, err)

				err = outsideRepo.Save(ctx, outsideAgg)
				require.NoError(t, err)

				// Try to save the transaction aggregate (should fail due to version conflict)
				_, err = txAgg.SingleEventCommand("tx-value")
				require.NoError(t, err)

				err = repo.Save(ctx, txAgg)
				require.Error(t, err)
				// Should return our custom ErrConcurrentModification error or MongoDB WriteConflict
				if !errors.Is(err, core.ErrConcurrentModification) {
					var mongoErr mongo.CommandError
					require.True(t, errors.As(err, &mongoErr))
					require.Equal(t, 112, mongoErr.Code,
						"Expected ErrConcurrentModification or WriteConflict (112), got %v", err)
				}

				return err
			})

		// The transaction should fail due to concurrent modification, which is expected
		require.Error(t, err)

		// The Save method correctly returns ErrConcurrentModification, but the ConcurrentScope
		// may return a different error when rolling back an already-aborted transaction.
		// We check for both cases:
		if errors.Is(err, core.ErrConcurrentModification) {
			// This is the ideal case - our custom error is preserved
		} else {
			// This is the MongoDB transaction abort case - also acceptable
			var mongoErr mongo.CommandError
			require.True(t, errors.As(err, &mongoErr))
			require.Equal(t, 251, mongoErr.Code,
				"Expected ErrConcurrentModification or NoSuchTransaction (251), got %v", err)
		}

		// Verify the outside modification was not successful
		loadedAgg := &testAgg{}
		err = repo.Load(ctx, "concurrent-scope-concurrent-id", loadedAgg)
		require.NoError(t, err)
		require.NotEqual(t, "outside-value", loadedAgg.State().String)
		require.Equal(t, "initial-value", loadedAgg.State().String)
	})
}






package test

import (
	"context"
	"fmt"
	"testing"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	"github.com/stretchr/testify/require"
)

// TestAggregate defines the interface for test aggregates
type TestAggregate interface {
	core.Restorer
	core.Storer
	SingleEventCommand(val string) (core.EventPack, error)
	Remove() (core.EventPack, error)
	ID() core.ID
	Version() core.Version
	State() TestAggState
	Events() core.EventPack
	Error() error
}

// TestRunner defines the interface for running repository tests
type TestRunner interface {
	// SetupRepository creates and returns a repository instance
	SetupRepository(t *testing.T) core.Repository
	// SetupContext returns the context to use for repository operations
	SetupContext(t *testing.T) context.Context
	// NewAggregate creates a new test aggregate instance
	NewAggregate(id core.ID) TestAggregate
}

// ConcurrentTestRunner defines the interface for running concurrent scope tests
type ConcurrentTestRunner interface {
	TestRunner
	// SetupConcurrentScope creates and returns a ConcurrentScope for concurrent testing
	SetupConcurrentScope(t *testing.T) *core.ConcurrentScope
	// SetupRepositoryFactory creates and returns a RepositoryFactory
	SetupRepositoryFactory(t *testing.T) core.RepositoryFactory
}

// RunBaseTests runs all base repository tests
func RunBaseTests(t *testing.T, runner TestRunner) {
	repo := runner.SetupRepository(t)
	ctx := runner.SetupContext(t)

	t.Run(`Scenario: Save and Load aggregate with single event
  Given a new aggregate with ID "test-id-1"
  When I execute a single event command with value "test-value"
  And I save the aggregate to the repository
  Then the aggregate version should be 1
  And the aggregate should have no pending events
  And the aggregate state should reflect the event value
  When I load the aggregate from the repository
  Then the loaded aggregate should have the correct ID
  And the loaded aggregate should have version 1
  And the loaded aggregate state should match the saved state`, func(t *testing.T) {
		agg := runner.NewAggregate("test-id-1")
		pack, err := agg.SingleEventCommand("test-value")
		require.NoError(t, err)
		require.Len(t, pack, 1)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		require.Equal(t, core.Version(1), agg.Version())
		require.Empty(t, agg.Events())
		require.Equal(t, "test-value", agg.State().String)

		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "test-id-1", loadedAgg)
		require.NoError(t, err)

		require.Equal(t, core.ID("test-id-1"), loadedAgg.ID())
		require.Equal(t, core.Version(1), loadedAgg.Version())
		require.Equal(t, "test-value", loadedAgg.State().String)
		require.Empty(t, loadedAgg.Events())
		require.NoError(t, loadedAgg.Error())
	})

	t.Run(`Scenario: Save aggregate with multiple events
  Given a new aggregate with ID "test-id-2"
  When I execute a first event command with value "first-value"
  And I execute a second event command with value "second-value"
  And I save the aggregate to the repository
  Then the aggregate version should be 1
  And the aggregate should have no pending events
  And the aggregate state should reflect the last event value
  When I load the aggregate from the repository
  Then the loaded aggregate state should match the final state`, func(t *testing.T) {
		agg := runner.NewAggregate("test-id-2")
		pack1, err := agg.SingleEventCommand("first-value")
		require.NoError(t, err)
		require.Len(t, pack1, 1)

		pack2, err := agg.SingleEventCommand("second-value")
		require.NoError(t, err)
		require.Len(t, pack2, 1)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		require.Equal(t, core.Version(1), agg.Version())
		require.Empty(t, agg.Events())
		require.Equal(t, "second-value", agg.State().String)

		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "test-id-2", loadedAgg)
		require.NoError(t, err)

		require.Equal(t, core.ID("test-id-2"), loadedAgg.ID())
		require.Equal(t, core.Version(1), loadedAgg.Version())
		require.Equal(t, "second-value", loadedAgg.State().String)
	})

	t.Run(`Scenario: Save and Load aggregate preserves complex state
  Given a new aggregate with ID "test-id-3"
  When I execute an event command with value "complex-value"
  And I save the aggregate to the repository
  When I load the aggregate from the repository
  Then the loaded aggregate state should preserve complex nested structures
  And the entity slice should be initialized
  And the map structure should be preserved if present`, func(t *testing.T) {
		agg := runner.NewAggregate("test-id-3")
		_, err := agg.SingleEventCommand("complex-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "test-id-3", loadedAgg)
		require.NoError(t, err)

		require.Equal(t, "complex-value", loadedAgg.State().String)
		require.NotNil(t, loadedAgg.State().EntitySlice)
		if loadedAgg.State().Map != nil {
			require.IsType(t, map[string]NestedEntity{}, loadedAgg.State().Map)
		}
	})

	t.Run(`Scenario: Save duplicate aggregate returns ErrAggregateExists
  Given an aggregate with ID "test-id-duplicate" that has been saved
  When I try to save another aggregate with the same ID
  Then the save operation should fail
  And the error should be ErrAggregateExists`, func(t *testing.T) {
		// Create and save first aggregate
		agg1 := runner.NewAggregate("test-id-duplicate")
		_, err := agg1.SingleEventCommand("first-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg1)
		require.NoError(t, err)

		// Try to save another aggregate with the same ID
		agg2 := runner.NewAggregate("test-id-duplicate")
		_, err = agg2.SingleEventCommand("second-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg2)
		require.ErrorIs(t, err, core.ErrAggregateExists)
	})

	t.Run(`Scenario: Load non-existent aggregate
  Given an aggregate ID "non-existent-id" that does not exist in the repository
  When I try to load the aggregate
  Then the load operation should fail
  And the error should be ErrAggregateNotFound`, func(t *testing.T) {
		loadedAgg := runner.NewAggregate("")
		err := repo.Load(ctx, "non-existent-id", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Save aggregate without events
  Given a new aggregate with ID "test-id-4" that has no pending events
  When I save the aggregate to the repository
  Then the save operation should succeed
  And the aggregate version should be 1 (from the Created event)
  When I load the aggregate from the repository
  Then the loaded aggregate should have version 1`, func(t *testing.T) {
		agg := runner.NewAggregate("test-id-4")
		err := repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		// Verify it was saved
		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "test-id-4", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), loadedAgg.Version())
	})

	t.Run(`Scenario: Update existing aggregate
  Given an aggregate with ID "test-id-5" that has been saved with version 1
  When I execute another event command
  And I save the aggregate again
  Then the aggregate version should increment to 2
  When I load the aggregate from the repository
  Then the loaded aggregate should have version 2
  And the loaded aggregate state should reflect the latest event`, func(t *testing.T) {
		// Create and save initial aggregate
		agg := runner.NewAggregate("test-id-5")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		// Execute another command and save again
		_, err = agg.SingleEventCommand("updated-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), agg.Version())

		// Load and verify the updated aggregate
		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "test-id-5", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), loadedAgg.Version())
		require.Equal(t, "updated-value", loadedAgg.State().String)
	})

	t.Run(`Scenario: Save new aggregate with tombstone event
  Given a new aggregate with ID "test-id-6"
  When I remove the aggregate (generates tombstone event)
  And I save the aggregate to the repository
  Then the save operation should succeed
  When I try to load the aggregate from the repository
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  And the document should not have been created`, func(t *testing.T) {
		agg := runner.NewAggregate("test-id-6")
		pack, err := agg.Remove()
		require.NoError(t, err)
		require.Len(t, pack, 1)
		require.True(t, agg.State().Removed)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		// Verify document was not created
		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "test-id-6", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Save existing aggregate with tombstone event
  Given an aggregate with ID "test-id-7" that has been saved with version 1
  When I remove the aggregate (generates tombstone event)
  And I save the aggregate to the repository
  Then the save operation should succeed
  When I try to load the aggregate from the repository
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  And the document should have been deleted`, func(t *testing.T) {
		agg := runner.NewAggregate("test-id-7")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		pack, err := agg.Remove()
		require.NoError(t, err)
		require.Len(t, pack, 1)
		require.True(t, agg.State().Removed)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		// Verify document was deleted
		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "test-id-7", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Atomic concurrency control - ErrConcurrentModification
  Given an aggregate with ID "concurrency-test-id" that has been saved with version 1
  And a second instance of the same aggregate loaded at version 1
  When I modify and save the first aggregate (version becomes 2)
  And I try to save the second aggregate with stale version 1
  Then the save operation should fail
  And the error should be ErrConcurrentModification`, func(t *testing.T) {
		// Create and save an aggregate
		agg := runner.NewAggregate("concurrency-test-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		// Create a second instance of the same aggregate (simulating concurrent access)
		agg2 := runner.NewAggregate("")
		err = repo.Load(ctx, "concurrency-test-id", agg2)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg2.Version())

		// Modify and save the first aggregate (increments version to 2)
		_, err = agg.SingleEventCommand("updated-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), agg.Version())

		// Try to save the second aggregate with stale version (should fail)
		_, err = agg2.SingleEventCommand("stale-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg2)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)
	})

	t.Run(`Scenario: Atomic concurrency control - version conflict on stale aggregate
  Given an aggregate with ID "successful-concurrency-test-id" that has been saved with version 1
  And two instances of the same aggregate loaded at version 1
  When I modify and save the second aggregate first (version becomes 2)
  And I try to save the first aggregate with stale version 1
  Then the save operation should fail
  And the error should be ErrConcurrentModification`, func(t *testing.T) {
		// Create and save an aggregate
		agg := runner.NewAggregate("successful-concurrency-test-id")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg.Version())

		// Create a second instance and modify it
		agg2 := runner.NewAggregate("")
		err = repo.Load(ctx, "successful-concurrency-test-id", agg2)
		require.NoError(t, err)
		require.Equal(t, core.Version(1), agg2.Version())

		// Modify and save the second aggregate first
		_, err = agg2.SingleEventCommand("second-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg2)
		require.NoError(t, err)
		require.Equal(t, core.Version(2), agg2.Version())

		// Now modify and save the first aggregate (should fail due to version mismatch)
		_, err = agg.SingleEventCommand("first-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)
	})

	t.Run(`Scenario: Concurrent modification wins over removal
  Given an aggregate with ID "concurrent-modify-remove-1" that has been saved
  And two instances of the same aggregate loaded concurrently
  When I modify and save the first instance
  And I try to remove and save the second instance
  Then the removal save operation should fail
  And the error should be ErrConcurrentModification
  When I load the aggregate from the repository
  Then the aggregate should exist with the modified state
  And the aggregate should not be removed`, func(t *testing.T) {
		agg := runner.NewAggregate("concurrent-modify-remove-1")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		modifyAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "concurrent-modify-remove-1", modifyAgg)
		require.NoError(t, err)

		removeAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "concurrent-modify-remove-1", removeAgg)
		require.NoError(t, err)

		_, err = modifyAgg.SingleEventCommand("modified-value")
		require.NoError(t, err)
		err = repo.Save(ctx, modifyAgg)
		require.NoError(t, err)

		_, err = removeAgg.Remove()
		require.NoError(t, err)
		err = repo.Save(ctx, removeAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrConcurrentModification)

		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "concurrent-modify-remove-1", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, "modified-value", loadedAgg.State().String)
		require.False(t, loadedAgg.State().Removed)
	})

	t.Run(`Scenario: Concurrent removal wins over modification
  Given an aggregate with ID "concurrent-modify-remove-2" that has been saved
  And two instances of the same aggregate loaded concurrently
  When I remove and save the first instance
  And I try to modify and save the second instance
  Then the modification save operation should fail
  And the error should be ErrAggregateNotFound
  When I try to load the aggregate from the repository
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  And the aggregate should have been deleted`, func(t *testing.T) {
		agg := runner.NewAggregate("concurrent-modify-remove-2")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		removeAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "concurrent-modify-remove-2", removeAgg)
		require.NoError(t, err)

		modifyAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "concurrent-modify-remove-2", modifyAgg)
		require.NoError(t, err)

		_, err = removeAgg.Remove()
		require.NoError(t, err)
		err = repo.Save(ctx, removeAgg)
		require.NoError(t, err)

		_, err = modifyAgg.SingleEventCommand("modified-value")
		require.NoError(t, err)
		err = repo.Save(ctx, modifyAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)

		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "concurrent-modify-remove-2", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Concurrent removals - first wins
  Given an aggregate with ID "concurrent-remove-remove-1" that has been saved
  And two instances of the same aggregate loaded concurrently
  When I remove and save the first instance
  And I try to remove and save the second instance
  Then the second removal save operation should fail
  And the error should be ErrAggregateNotFound
  When I try to load the aggregate from the repository
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  And the aggregate should have been deleted by the first removal`, func(t *testing.T) {
		agg := runner.NewAggregate("concurrent-remove-remove-1")
		_, err := agg.SingleEventCommand("initial-value")
		require.NoError(t, err)
		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		firstRemoveAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "concurrent-remove-remove-1", firstRemoveAgg)
		require.NoError(t, err)

		secondRemoveAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "concurrent-remove-remove-1", secondRemoveAgg)
		require.NoError(t, err)

		// First removal succeeds
		_, err = firstRemoveAgg.Remove()
		require.NoError(t, err)
		err = repo.Save(ctx, firstRemoveAgg)
		require.NoError(t, err)

		// Second removal fails because aggregate was already deleted
		_, err = secondRemoveAgg.Remove()
		require.NoError(t, err)
		err = repo.Save(ctx, secondRemoveAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)

		// Verify the first removal was successful
		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "concurrent-remove-remove-1", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})
}

// RunBaseConcurrentTests runs all base concurrent scope tests
func RunBaseConcurrentTests(t *testing.T, runner ConcurrentTestRunner) {
	ctx := runner.SetupContext(t)
	concurrentScope := runner.SetupConcurrentScope(t)
	factory := runner.SetupRepositoryFactory(t)

	t.Run(`Scenario: Save and Load aggregate within concurrent scope - successful commit
  Given a concurrent scope
  When I create and save an aggregate within the concurrent scope
  And I load the aggregate within the concurrent scope
  Then the aggregate should be visible within the concurrent scope
  When the transaction commits successfully
  Then the aggregate should be visible after transaction commit
  And the aggregate state should match the saved state`, func(t *testing.T) {
		err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			agg := runner.NewAggregate("tx-scope-success-id")
			_, err := agg.SingleEventCommand("tx-scope-success-value")
			require.NoError(t, err)

			err = repo.Save(ctx, agg)
			require.NoError(t, err)

			loadedAgg := runner.NewAggregate("")
			err = repo.Load(ctx, "tx-scope-success-id", loadedAgg)
			require.NoError(t, err)
			require.Equal(t, "tx-scope-success-value", loadedAgg.State().String)

			return nil
		})
		require.NoError(t, err)

		repo := factory.Create(ctx)
		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "tx-scope-success-id", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, "tx-scope-success-value", loadedAgg.State().String)
	})

	t.Run(`Scenario: Save aggregate within concurrent scope - rollback on error
  Given a concurrent scope
  When I create and save an aggregate within the concurrent scope
  And I verify the aggregate is visible within the concurrent scope
  And I return an error to trigger rollback
  Then the transaction should rollback
  And the error should be returned
  When I try to load the aggregate after rollback
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  And the aggregate should not have been persisted`, func(t *testing.T) {
		err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			agg := runner.NewAggregate("tx-scope-rollback-id")
			_, err := agg.SingleEventCommand("tx-scope-rollback-value")
			require.NoError(t, err)

			err = repo.Save(ctx, agg)
			require.NoError(t, err)

			loadedAgg := runner.NewAggregate("")
			err = repo.Load(ctx, "tx-scope-rollback-id", loadedAgg)
			require.NoError(t, err)
			require.Equal(t, "tx-scope-rollback-value", loadedAgg.State().String)

			return fmt.Errorf("intentional rollback error")
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "intentional rollback error")

		repo := factory.Create(ctx)
		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "tx-scope-rollback-id", loadedAgg)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})

	t.Run(`Scenario: Multiple aggregates in single concurrent scope
  Given a concurrent scope
  When I create and save multiple aggregates within the concurrent scope
  And I verify all aggregates are visible within the concurrent scope
  Then the transaction should commit successfully
  When I load all aggregates after transaction commit
  Then all aggregates should be visible
  And all aggregate states should match the saved states`, func(t *testing.T) {
		err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			for i := 0; i < 3; i++ {
				agg := runner.NewAggregate(core.ID(fmt.Sprintf("tx-scope-multi-id-%d", i)))
				_, err := agg.SingleEventCommand(fmt.Sprintf("tx-scope-multi-value-%d", i))
				require.NoError(t, err)

				err = repo.Save(ctx, agg)
				require.NoError(t, err)
			}

			for i := 0; i < 3; i++ {
				loadedAgg := runner.NewAggregate("")
				err := repo.Load(ctx, core.ID(fmt.Sprintf("tx-scope-multi-id-%d", i)), loadedAgg)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("tx-scope-multi-value-%d", i), loadedAgg.State().String)
			}

			return nil
		})
		require.NoError(t, err)

		repo := factory.Create(ctx)
		for i := 0; i < 3; i++ {
			loadedAgg := runner.NewAggregate("")
			err := repo.Load(ctx, core.ID(fmt.Sprintf("tx-scope-multi-id-%d", i)), loadedAgg)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("tx-scope-multi-value-%d", i), loadedAgg.State().String)
		}
	})

	t.Run(`Scenario: Concurrent scope with read-only operations
  Given an aggregate that has been saved outside concurrent scope
  When I execute a read-only transaction using concurrent scope
  And I load the aggregate within the concurrent scope
  Then the aggregate should be visible within the concurrent scope
  And the aggregate state should match the saved state
  When I try to load a non-existent aggregate within the concurrent scope
  Then the load operation should fail
  And the error should be ErrAggregateNotFound
  When the transaction commits
  Then the original aggregate should be unchanged`, func(t *testing.T) {
		repo := factory.Create(ctx)
		agg := runner.NewAggregate("tx-scope-readonly-id")
		_, err := agg.SingleEventCommand("readonly-value")
		require.NoError(t, err)

		err = repo.Save(ctx, agg)
		require.NoError(t, err)

		err = concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			loadedAgg := runner.NewAggregate("")
			err = repo.Load(ctx, "tx-scope-readonly-id", loadedAgg)
			require.NoError(t, err)
			require.Equal(t, "readonly-value", loadedAgg.State().String)

			nonExistentAgg := runner.NewAggregate("")
			err = repo.Load(ctx, "non-existent-id", nonExistentAgg)
			require.Error(t, err)
			require.ErrorIs(t, err, core.ErrAggregateNotFound)

			return nil
		})
		require.NoError(t, err)

		loadedAgg := runner.NewAggregate("")
		err = repo.Load(ctx, "tx-scope-readonly-id", loadedAgg)
		require.NoError(t, err)
		require.Equal(t, "readonly-value", loadedAgg.State().String)
	})

	t.Run(`Scenario: Concurrent scope with error handling and cleanup
  Given a concurrent scope
  When I create and save multiple aggregates within the concurrent scope
  And I simulate a failure during the transaction
  Then the transaction should rollback
  And the error should be returned
  When I try to load all aggregates after rollback
  Then all load operations should fail
  And all errors should be ErrAggregateNotFound
  And no aggregates should have been persisted`, func(t *testing.T) {
		err := concurrentScope.Run(ctx, func(ctx context.Context, repo core.Repository) error {
			agg1 := runner.NewAggregate("tx-scope-cleanup-1")
			_, err := agg1.SingleEventCommand("value-1")
			require.NoError(t, err)

			err = repo.Save(ctx, agg1)
			require.NoError(t, err)

			agg2 := runner.NewAggregate("tx-scope-cleanup-2")
			_, err = agg2.SingleEventCommand("value-2")
			require.NoError(t, err)

			err = repo.Save(ctx, agg2)
			require.NoError(t, err)

			agg3 := runner.NewAggregate("tx-scope-cleanup-3")
			_, err = agg3.SingleEventCommand("value-3")
			require.NoError(t, err)

			return fmt.Errorf("simulated failure during third aggregate save")
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "simulated failure during third aggregate save")

		repo := factory.Create(ctx)
		loadedAgg1 := runner.NewAggregate("")
		err = repo.Load(ctx, "tx-scope-cleanup-1", loadedAgg1)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)

		loadedAgg2 := runner.NewAggregate("")
		err = repo.Load(ctx, "tx-scope-cleanup-2", loadedAgg2)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)

		loadedAgg3 := runner.NewAggregate("")
		err = repo.Load(ctx, "tx-scope-cleanup-3", loadedAgg3)
		require.Error(t, err)
		require.ErrorIs(t, err, core.ErrAggregateNotFound)
	})
}

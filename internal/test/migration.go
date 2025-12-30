package test

// // RunStateMigrationTestsV1 runs V1 aggregate lifecycle tests
// func RunStateMigrationTestsV1(t *testing.T, repo core.Repository, ctx context.Context) {
// 	t.Run(`Scenario: V1 aggregate lifecycle - create, save, load
//   Given a new V1 aggregate with ID "migration-v1-1"
//   When I save the aggregate to the repository
//   And I load the aggregate from the repository
//   Then the loaded aggregate should have ValueV1 = "version 1"
//   And the aggregate version should be 1`, func(t *testing.T) {
// 		agg := NewStateTestAggV1("migration-v1-1")
// 		err := repo.Save(ctx, agg)
// 		require.NoError(t, err)
// 		require.Equal(t, core.Version(1), agg.Version())

// 		loadedAgg := NewStateTestAggV1("")
// 		err = repo.Load(ctx, "migration-v1-1", loadedAgg)
// 		require.NoError(t, err)
// 		require.Equal(t, core.ID("migration-v1-1"), loadedAgg.ID())
// 		require.Equal(t, core.Version(1), loadedAgg.Version())
// 		require.Equal(t, "version 1", loadedAgg.State().ValueV2)
// 	})

// 	t.Run(`Scenario: V1 aggregate - save again without changes
//   Given a V1 aggregate that has been saved and loaded
//   When I save the aggregate again without changes
//   And I load the aggregate from the repository
//   Then the aggregate state should persist correctly`, func(t *testing.T) {
// 		agg := NewStateTestAggV1("migration-v1-2")
// 		err := repo.Save(ctx, agg)
// 		require.NoError(t, err)

// 		loadedAgg := NewStateTestAggV1("")
// 		err = repo.Load(ctx, "migration-v1-2", loadedAgg)
// 		require.NoError(t, err)
// 		require.Equal(t, "version 1", loadedAgg.State().ValueV2)

// 		// Save again without changes
// 		err = repo.Save(ctx, loadedAgg)
// 		require.NoError(t, err)

// 		// Load again and verify persistence
// 		loadedAgg2 := NewStateTestAggV1("")
// 		err = repo.Load(ctx, "migration-v1-2", loadedAgg2)
// 		require.NoError(t, err)
// 		require.Equal(t, "version 1", loadedAgg2.State().ValueV2)
// 		require.Equal(t, core.Version(1), loadedAgg2.Version())
// 	})
// }

// // RunStateMigrationTestsV2 runs V2 aggregate lifecycle tests
// func RunStateMigrationTestsV2(t *testing.T, repo core.Repository, ctx context.Context) {
// 	t.Run(`Scenario: V2 aggregate lifecycle - create, save, load
//   Given a new V2 aggregate with ID "migration-v2-1"
//   When I save the aggregate to the repository
//   And I load the aggregate from the repository
//   Then the loaded aggregate should have ValueV2 = "version 2"
//   And the aggregate version should be 1`, func(t *testing.T) {
// 		agg := NewStateTestAggV2("migration-v2-1")
// 		err := repo.Save(ctx, agg)
// 		require.NoError(t, err)
// 		require.Equal(t, core.Version(1), agg.Version())

// 		loadedAgg := NewStateTestAggV2("")
// 		err = repo.Load(ctx, "migration-v2-1", loadedAgg)
// 		require.NoError(t, err)
// 		require.Equal(t, core.ID("migration-v2-1"), loadedAgg.ID())
// 		require.Equal(t, core.Version(1), loadedAgg.Version())
// 		require.Equal(t, "version 2", loadedAgg.State().ValueV2)
// 	})

// 	t.Run(`Scenario: V2 aggregate - update, save, load
//   Given a V2 aggregate that has been saved
//   When I update the aggregate value
//   And I save the aggregate to the repository
//   And I load the aggregate from the repository
//   Then the loaded aggregate should have the updated value
//   And the aggregate version should be 2`, func(t *testing.T) {
// 		agg := NewStateTestAggV2("migration-v2-2")
// 		err := repo.Save(ctx, agg)
// 		require.NoError(t, err)

// 		agg.UpdateValue("updated-value")
// 		err = repo.Save(ctx, agg)
// 		require.NoError(t, err)
// 		require.Equal(t, core.Version(2), agg.Version())

// 		loadedAgg := NewStateTestAggV2("")
// 		err = repo.Load(ctx, "migration-v2-2", loadedAgg)
// 		require.NoError(t, err)
// 		require.Equal(t, core.Version(2), loadedAgg.Version())
// 		require.Equal(t, "updated-value", loadedAgg.State().ValueV2)
// 	})
// }

// // RunStateMigrationForwardTests runs forward migration tests (V1 → V2)
// func RunStateMigrationForwardTests(t *testing.T, repo core.Repository, ctx context.Context) {
// 	t.Run(`Scenario: V1 to V2 migration - load V1 state as V2
//   Given a V1 aggregate that has been saved with schema version 1
//   When I load the aggregate as V2
//   Then the V2 aggregate should have ValueV2 = ValueV1 from V1
//   And the migration should succeed`, func(t *testing.T) {
// 		aggV1 := NewStateTestAggV1("migration-forward-1")
// 		err := repo.Save(ctx, aggV1)
// 		require.NoError(t, err)

// 		aggV2 := NewStateTestAggV2("")
// 		err = repo.Load(ctx, "migration-forward-1", aggV2)
// 		require.NoError(t, err)
// 		require.Equal(t, core.ID("migration-forward-1"), aggV2.ID())
// 		require.Equal(t, core.Version(1), aggV2.Version())
// 		require.Equal(t, "version 1", aggV2.State().ValueV2)
// 	})

// 	t.Run(`Scenario: V1 to V2 migration then update
//   Given a V1 aggregate that has been saved
//   When I load it as V2 and update the value
//   And I save the V2 aggregate
//   And I load the V2 aggregate again
//   Then the updated value should be persisted
//   And the aggregate version should be 2`, func(t *testing.T) {
// 		aggV1 := NewStateTestAggV1("migration-forward-2")
// 		err := repo.Save(ctx, aggV1)
// 		require.NoError(t, err)

// 		aggV2 := NewStateTestAggV2("")
// 		err = repo.Load(ctx, "migration-forward-2", aggV2)
// 		require.NoError(t, err)
// 		require.Equal(t, "version 1", aggV2.State().ValueV2)

// 		aggV2.UpdateValue("migrated-and-updated")
// 		err = repo.Save(ctx, aggV2)
// 		require.NoError(t, err)
// 		require.Equal(t, core.Version(2), aggV2.Version())

// 		loadedAggV2 := NewStateTestAggV2("")
// 		err = repo.Load(ctx, "migration-forward-2", loadedAggV2)
// 		require.NoError(t, err)
// 		require.Equal(t, core.Version(2), loadedAggV2.Version())
// 		require.Equal(t, "migrated-and-updated", loadedAggV2.State().ValueV2)
// 	})

// 	t.Run(`Scenario: V1 to V2 migration, update, save, load
//   Given a V1 aggregate that has been saved
//   When I load it as V2, update, and save (schema upgraded to 2)
//   And I load the V2 aggregate again
//   Then the state should be persisted with schema version 2
//   And the value should be correct`, func(t *testing.T) {
// 		aggV1 := NewStateTestAggV1("migration-forward-3")
// 		err := repo.Save(ctx, aggV1)
// 		require.NoError(t, err)

// 		aggV2 := NewStateTestAggV2("")
// 		err = repo.Load(ctx, "migration-forward-3", aggV2)
// 		require.NoError(t, err)

// 		aggV2.UpdateValue("final-value")
// 		err = repo.Save(ctx, aggV2)
// 		require.NoError(t, err)

// 		loadedAggV2 := NewStateTestAggV2("")
// 		err = repo.Load(ctx, "migration-forward-3", loadedAggV2)
// 		require.NoError(t, err)
// 		require.Equal(t, "final-value", loadedAggV2.State().ValueV2)
// 		require.Equal(t, core.Version(2), loadedAggV2.Version())
// 	})
// }

// // RunStateMigrationBackwardTests runs backward compatibility tests (V2 → V1)
// func RunStateMigrationBackwardTests(t *testing.T, repo core.Repository, ctx context.Context) {
// 	t.Run(`Scenario: V2 to V1 backward compatibility - V1 cannot load schema version 2
//   Given a V2 aggregate that has been saved with schema version 2
//   When I try to load it as V1
//   Then the load operation should fail
//   And the error should indicate unsupported schema version`, func(t *testing.T) {
// 		aggV2 := NewStateTestAggV2("migration-backward-1")
// 		err := repo.Save(ctx, aggV2)
// 		require.NoError(t, err)

// 		aggV1 := NewStateTestAggV1("")
// 		err = repo.Load(ctx, "migration-backward-1", aggV1)
// 		require.Error(t, err)
// 		require.Contains(t, err.Error(), "unsupported schema version")
// 	})

// 	t.Run(`Scenario: V2 to V1 backward compatibility - updated V2 cannot be loaded as V1
//   Given a V2 aggregate that has been updated and saved with schema version 2
//   When I try to load it as V1
//   Then the load operation should fail
//   And the error should indicate unsupported schema version`, func(t *testing.T) {
// 		aggV2 := NewStateTestAggV2("migration-backward-2")
// 		err := repo.Save(ctx, aggV2)
// 		require.NoError(t, err)

// 		aggV2.UpdateValue("updated-value")
// 		err = repo.Save(ctx, aggV2)
// 		require.NoError(t, err)

// 		aggV1 := NewStateTestAggV1("")
// 		err = repo.Load(ctx, "migration-backward-2", aggV1)
// 		require.Error(t, err)
// 		require.Contains(t, err.Error(), "unsupported schema version")
// 	})
// }

// // RunStateMigrationMixedTests runs mixed migration scenarios
// func RunStateMigrationMixedTests(t *testing.T, repo core.Repository, ctx context.Context) {
// 	t.Run(`Scenario: V1 can load schema 1 even after V2 has read it
//   Given a V1 aggregate that has been saved with schema version 1
//   When I load it as V2 (migration read)
//   And I try to load the same ID as V1
//   Then V1 should still be able to load the aggregate
//   And the state should be correct`, func(t *testing.T) {
// 		aggV1 := NewStateTestAggV1("migration-mixed-1")
// 		err := repo.Save(ctx, aggV1)
// 		require.NoError(t, err)

// 		// Load as V2 (migration read, doesn't change schema)
// 		aggV2 := NewStateTestAggV2("")
// 		err = repo.Load(ctx, "migration-mixed-1", aggV2)
// 		require.NoError(t, err)
// 		require.Equal(t, "version 1", aggV2.State().ValueV2)

// 		// V1 should still be able to load schema 1
// 		aggV1Loaded := NewStateTestAggV1("")
// 		err = repo.Load(ctx, "migration-mixed-1", aggV1Loaded)
// 		require.NoError(t, err)
// 		require.Equal(t, "version 1", aggV1Loaded.State().ValueV2)
// 		require.Equal(t, core.Version(1), aggV1Loaded.Version())
// 	})

// 	t.Run(`Scenario: V1 cannot load after schema upgrade to version 2
//   Given a V1 aggregate that has been saved
//   When I load it as V2, update, and save (schema upgraded to 2)
//   And I try to load it as V1
//   Then the load operation should fail
//   And the error should indicate unsupported schema version`, func(t *testing.T) {
// 		aggV1 := NewStateTestAggV1("migration-mixed-2")
// 		err := repo.Save(ctx, aggV1)
// 		require.NoError(t, err)

// 		// Load as V2, update, and save (upgrades schema to 2)
// 		aggV2 := NewStateTestAggV2("")
// 		err = repo.Load(ctx, "migration-mixed-2", aggV2)
// 		require.NoError(t, err)

// 		aggV2.UpdateValue("upgraded-value")
// 		err = repo.Save(ctx, aggV2)
// 		require.NoError(t, err)

// 		// V1 should no longer be able to load (schema upgraded)
// 		aggV1Loaded := NewStateTestAggV1("")
// 		err = repo.Load(ctx, "migration-mixed-2", aggV1Loaded)
// 		require.Error(t, err)
// 		require.Contains(t, err.Error(), "unsupported schema version")
// 	})
// }

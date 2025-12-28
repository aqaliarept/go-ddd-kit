package mongo

import (
	"context"
	"errors"
	"testing"
	"time"

	core "github.com/aqaliarept/go-ddd/core"
	testpkg "github.com/aqaliarept/go-ddd/internal/test"
	"github.com/avast/retry-go/v4"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Type aliases for convenience
type (
	ID         = core.ID
	Version    = core.Version
	Repository = core.Repository
)

// testAgg wraps testpkg.TestAgg and implements CollectionStore for MongoDB
type testAgg struct {
	testpkg.TestAgg
}

func (t *testAgg) CollectionName() CollectionName {
	return "test_agg"
}

func newTestAgg(id core.ID) *testAgg {
	baseAgg := testpkg.NewTestAgg(id)
	return &testAgg{TestAgg: baseAgg}
}

// mongoTestAgg embeds the base TestAgg and implements CollectionStore for MongoDB
type mongoTestAgg struct {
	testpkg.TestAgg
}

func (m *mongoTestAgg) CollectionName() CollectionName {
	return "test_agg"
}

func newMongoTestAgg(id core.ID) *mongoTestAgg {
	baseAgg := testpkg.NewTestAgg(id)
	return &mongoTestAgg{TestAgg: baseAgg}
}

// mongoTestContainer wraps a MongoDB testcontainer
type mongoTestContainer struct {
	container testcontainers.Container
	client    *mongo.Client
	database  *mongo.Database
	connStr   string
}

// ConnectionString returns the MongoDB connection string
func (m *mongoTestContainer) ConnectionString() string {
	return m.connStr
}

// Database returns the MongoDB database instance
func (m *mongoTestContainer) Database() *mongo.Database {
	return m.database
}

// Client returns the MongoDB client
func (m *mongoTestContainer) Client() *mongo.Client {
	return m.client
}

// Cleanup terminates the container
func (m *mongoTestContainer) Cleanup() {
	ctx := context.Background()
	if m.client != nil {
		//nolint:errcheck // Cleanup errors in test teardown are non-fatal
		_ = m.client.Disconnect(ctx)
	}
	if m.container != nil {
		//nolint:errcheck // Cleanup errors in test teardown are non-fatal
		_ = m.container.Terminate(ctx)
	}
}

// SetupMongoTestContainer creates and starts a MongoDB testcontainer
func SetupMongoTestContainer(t *testing.T) *mongoTestContainer {
	ctx := context.Background()

	// Start MongoDB container
	mongoContainer, err := mongodb.Run(ctx,
		"mongo:7.0",
		testcontainers.WithWaitStrategy(
			wait.ForLog("Waiting for connections").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second),
		),
		mongodb.WithReplicaSet("rs0"),
	)
	if err != nil {
		t.Fatalf("failed to start MongoDB container: %v", err)
	}

	// Get connection string
	connStr, err := mongoContainer.ConnectionString(ctx)

	if err != nil {
		termErr := mongoContainer.Terminate(ctx)
		t.Fatalf("failed to get MongoDB connection string: %v", errors.Join(err, termErr))
	}

	// Connect to MongoDB
	clientOptions := options.Client().ApplyURI(connStr)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		termErr := mongoContainer.Terminate(ctx)
		t.Fatalf("failed to connect to MongoDB: %v", errors.Join(err, termErr))
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		discErr := client.Disconnect(ctx)
		termErr := mongoContainer.Terminate(ctx)
		t.Fatalf("failed to ping MongoDB: %v", errors.Join(err, discErr, termErr))
	}

	// Get or create a test database
	database := client.Database("test_db")

	// Register cleanup
	t.Cleanup(func() {
		if err := client.Disconnect(ctx); err != nil {
			t.Logf("failed to disconnect MongoDB client: %v", err)
		}
		if err := mongoContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate MongoDB container: %v", err)
		}
	})

	return &mongoTestContainer{
		container: mongoContainer,
		client:    client,
		database:  database,
		connStr:   connStr,
	}
}

// mongoTestRunner implements the TestRunner interface for MongoDB tests
type mongoTestRunner struct {
	container *mongoTestContainer
	repo      core.Repository
}

func (m *mongoTestRunner) SetupRepository(t *testing.T) core.Repository {
	return m.repo
}

func (m *mongoTestRunner) SetupContext(t *testing.T) context.Context {
	return context.Background()
}

func (m *mongoTestRunner) NewAggregate(id core.ID) testpkg.TestAggregate {
	return newMongoTestAgg(id)
}

func (m *mongoTestRunner) SetupConcurrentScope(t *testing.T) *core.ConcurrentScope {
	factory := NewRepositoryFactory(m.container.Database())
	return core.NewConcurrentScope(factory,
		retry.Attempts(DefaultRetryAttempts),
		retry.Delay(DefaultRetryDelay),
		retry.MaxJitter(DefaultRetryMaxJitter),
	)
}

func (m *mongoTestRunner) SetupRepositoryFactory(t *testing.T) core.RepositoryFactory {
	return NewRepositoryFactory(m.container.Database())
}

func TestMongoRepository(t *testing.T) {
	container := SetupMongoTestContainer(t)
	defer container.Cleanup()

	runner := &mongoTestRunner{
		container: container,
		repo:      newRepository(container.Database()),
	}

	testpkg.RunBaseTests(t, runner)
	testpkg.RunBaseConcurrentTests(t, runner)
}

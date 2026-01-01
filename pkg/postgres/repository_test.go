package postgres

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
	testpkg "github.com/aqaliarept/go-ddd-kit/pkg/internal/test"
	"github.com/avast/retry-go/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Type aliases for convenience
type (
	ID         = core.ID
	Version    = core.Version
	Repository = core.Repository
)

const (
	DefaultRetryAttempts  = 5
	DefaultRetryDelay     = 100 * time.Millisecond
	DefaultRetryMaxJitter = 50 * time.Millisecond
)

// postgresTestAgg wraps TestAggWrapper and adds TableName method for PostgreSQL
type postgresTestAgg struct {
	testpkg.TestAggWrapper
}

func (p *postgresTestAgg) StorageOptions() []core.StorageOption {
	return []core.StorageOption{WithTableName("test_agg")}
}

// postgresTestContainer wraps a PostgreSQL testcontainer
type postgresTestContainer struct {
	container testcontainers.Container
	db        *pgxpool.Pool
	connStr   string
}

// ConnectionString returns the PostgreSQL connection string
func (p *postgresTestContainer) ConnectionString() string {
	return p.connStr
}

// Database returns the PostgreSQL database instance
func (p *postgresTestContainer) Database() *pgxpool.Pool {
	return p.db
}

// Cleanup terminates the container and closes the database connection
func (p *postgresTestContainer) Cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if p.db != nil {
		p.db.Close()
	}
	if p.container != nil {
		err := p.container.Terminate(ctx)
		log.Println(err)
	}
}

// SetupPostgresTestContainer creates and starts a PostgreSQL testcontainer
func SetupPostgresTestContainer(t *testing.T) *postgresTestContainer {
	ctx := context.Background()

	// Start PostgreSQL container
	postgresContainer, err := postgres.Run(ctx,
		"postgres:15-alpine",
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
		postgres.WithDatabase("test_db"),
		postgres.WithUsername("test_user"),
		postgres.WithPassword("test_password"),
	)
	if err != nil {
		t.Fatalf("failed to start PostgreSQL container: %v", err)
	}

	// Get connection string
	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		termErr := postgresContainer.Terminate(ctx)
		t.Fatalf("failed to get PostgreSQL connection string: %v", errors.Join(err, termErr))
	}

	// Connect to PostgreSQL using pgxpool
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		termErr := postgresContainer.Terminate(ctx)
		t.Fatalf("failed to parse connection string: %v", errors.Join(err, termErr))
	}

	db, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		termErr := postgresContainer.Terminate(ctx)
		t.Fatalf("failed to connect to PostgreSQL: %v", errors.Join(err, termErr))
	}

	// Ping to verify connection
	if err := db.Ping(ctx); err != nil {
		db.Close()
		termErr := postgresContainer.Terminate(ctx)
		t.Fatalf("failed to ping PostgreSQL: %v", errors.Join(err, termErr))
	}

	// Create test table
	if err := createTestTable(ctx, db); err != nil {
		db.Close()
		termErr := postgresContainer.Terminate(ctx)
		t.Fatalf("failed to create test table: %v", errors.Join(err, termErr))
	}

	// Register cleanup
	t.Cleanup(func() {
		db.Close()
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate PostgreSQL container: %v", err)
		}
	})

	return &postgresTestContainer{
		container: postgresContainer,
		db:        db,
		connStr:   connStr,
	}
}

// createTestTable creates the test_agg table for testing
func createTestTable(ctx context.Context, db *pgxpool.Pool) error {
	query := `
		CREATE TABLE IF NOT EXISTS test_agg (
			id VARCHAR(255) PRIMARY KEY,
			version BIGINT NOT NULL,
			data JSONB NOT NULL,
			schema_version BIGINT,
			CONSTRAINT test_agg_id_unique UNIQUE (id)
		)
	`
	_, err := db.Exec(ctx, query)
	return err
}

// postgresTestRunner implements the TestRunner interface for PostgreSQL tests
type postgresTestRunner struct {
	container *postgresTestContainer
	repo      core.Repository
}

func (p *postgresTestRunner) SetupRepository(t *testing.T) core.Repository {
	return p.repo
}

func (p *postgresTestRunner) SetupContext(t *testing.T) context.Context {
	return context.Background()
}

func (p *postgresTestRunner) NewAggregate(agg any) testpkg.TestAggregate {
	wrapper := testpkg.NewTestAggWrapper(agg)
	return &postgresTestAgg{TestAggWrapper: *wrapper}
}

func (p *postgresTestRunner) SetupConcurrentScope(t *testing.T) *core.ConcurrentScope {
	factory := NewRepositoryFactory(p.container.Database())
	return core.NewConcurrentScope(factory,
		retry.Attempts(DefaultRetryAttempts),
		retry.Delay(DefaultRetryDelay),
		retry.MaxJitter(DefaultRetryMaxJitter),
	)
}

func (p *postgresTestRunner) SetupRepositoryFactory(t *testing.T) core.RepositoryFactory {
	return NewRepositoryFactory(p.container.Database())
}

func TestPostgresRepository(t *testing.T) {
	container := SetupPostgresTestContainer(t)

	runner := &postgresTestRunner{
		container: container,
		repo:      newRepository(container.Database()),
	}

	testpkg.RunBaseTests(t, runner)
	testpkg.RunBaseConcurrentTests(t, runner)
}

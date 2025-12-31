package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/stretchr/testify/require"
)

type mockRepositoryFactory struct {
	createFunc func(ctx context.Context) Repository
	callCount  int
	mu         sync.Mutex
}

func (m *mockRepositoryFactory) Create(ctx context.Context) Repository {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()
	if m.createFunc != nil {
		return m.createFunc(ctx)
	}
	return &mockRepository{}
}

func (m *mockRepositoryFactory) getCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

type mockRepository struct {
	loadFunc func(ctx context.Context, id ID, aggregate Restorer, options ...LoadOption) error
	saveFunc func(ctx context.Context, aggregate Storer, options ...SaveOption) error
}

func (m *mockRepository) Load(ctx context.Context, id ID, aggregate Restorer, options ...LoadOption) error {
	if m.loadFunc != nil {
		return m.loadFunc(ctx, id, aggregate, options...)
	}
	return nil
}

func (m *mockRepository) Save(ctx context.Context, aggregate Storer, options ...SaveOption) error {
	if m.saveFunc != nil {
		return m.saveFunc(ctx, aggregate, options...)
	}
	return nil
}

type mockTransactional struct {
	beginFunc     func(ctx context.Context) (context.Context, error)
	commitFunc    func(ctx context.Context) error
	rollbackFunc  func(ctx context.Context) error
	beginCount    int
	commitCount   int
	rollbackCount int
	mu            sync.Mutex
}

func (m *mockTransactional) Begin(ctx context.Context) (context.Context, error) {
	m.mu.Lock()
	m.beginCount++
	m.mu.Unlock()
	if m.beginFunc != nil {
		return m.beginFunc(ctx)
	}
	return ctx, nil
}

func (m *mockTransactional) Commit(ctx context.Context) error {
	m.mu.Lock()
	m.commitCount++
	m.mu.Unlock()
	if m.commitFunc != nil {
		return m.commitFunc(ctx)
	}
	return nil
}

func (m *mockTransactional) Rollback(ctx context.Context) error {
	m.mu.Lock()
	m.rollbackCount++
	m.mu.Unlock()
	if m.rollbackFunc != nil {
		return m.rollbackFunc(ctx)
	}
	return nil
}

func (m *mockTransactional) getBeginCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.beginCount
}

func (m *mockTransactional) getCommitCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commitCount
}

func (m *mockTransactional) getRollbackCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rollbackCount
}

type mockTransactionalRepository struct {
	mockRepository
	mockTransactional
}

type contextKey string

const testContextKey contextKey = "test-key"

func TestNewConcurrentScope(t *testing.T) {
	t.Run(`Given a repository factory
		When NewConcurrentScope is called with default options
		Then ConcurrentScope should be created with default retry options
		And rollback timeout should be set to 5 seconds
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory)

		require.NotNil(t, scope)
		require.Equal(t, factory, scope.factory)
		require.NotNil(t, scope.retryOpts)
		require.Equal(t, 5*time.Second, scope.rollbackTimeout)
		require.Greater(t, len(scope.retryOpts), 0)
	})

	t.Run(`Given a repository factory
		When NewConcurrentScope is called with custom retry options
		Then ConcurrentScope should be created with merged retry options
		And custom options should be included
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(
			retry.Attempts(5),
			retry.Delay(100*time.Millisecond),
		))

		require.NotNil(t, scope)
		require.Equal(t, factory, scope.factory)
		require.Greater(t, len(scope.retryOpts), 1)
	})

	t.Run(`Given a ConcurrentScope with default RetryIf condition
		When Run is called and returns ErrTransient or ErrConcurrentModification
		Then the operation should be retried
		And eventually succeed
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory)

		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			if callCount == 1 {
				return ErrTransient
			}
			if callCount == 2 {
				return ErrConcurrentModification
			}
			return nil
		})

		require.NoError(t, err)
		require.GreaterOrEqual(t, callCount, 3)
	})

	t.Run(`Given a repository factory
		When NewConcurrentScope is called with WithRollbackTimeout
		Then ConcurrentScope should be created with custom rollback timeout
		And timeout should match the provided value
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		customTimeout := 15 * time.Second
		scope := NewConcurrentScope(factory, WithRollbackTimeout(customTimeout))

		require.NotNil(t, scope)
		require.Equal(t, customTimeout, scope.rollbackTimeout)
	})

	t.Run(`Given a repository factory
		When NewConcurrentScope is called with both retry options and rollback timeout
		Then ConcurrentScope should be created with both options applied
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		customTimeout := 20 * time.Second
		scope := NewConcurrentScope(factory,
			WithRetryOptions(retry.Attempts(3)),
			WithRollbackTimeout(customTimeout),
		)

		require.NotNil(t, scope)
		require.Equal(t, customTimeout, scope.rollbackTimeout)
		require.Greater(t, len(scope.retryOpts), 0)
	})
}

func TestWithRetryOptions(t *testing.T) {
	t.Run(`Given retry options
		When WithRetryOptions is called
		Then retryOptions type should be created
		And should contain the provided options
	`, func(t *testing.T) {
		opts := []retry.Option{retry.Attempts(3)}
		result := WithRetryOptions(opts...)

		require.NotNil(t, result)
		retryOpts, ok := result.(retryOptions)
		require.True(t, ok)
		require.Equal(t, opts, retryOpts.retryOptions)
	})

	t.Run(`Given a ConcurrentScope
		When Run is called with WithRetryOptions
		Then type assertion should work correctly
		And custom retry options should be applied
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory)

		customOpts := WithRetryOptions(retry.Attempts(2))
		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			if callCount < 2 {
				return ErrTransient
			}
			return nil
		}, customOpts)

		require.NoError(t, err)
		require.Equal(t, 2, callCount)
	})
}

func TestConcurrentScope_Run_NonTransactional(t *testing.T) {
	t.Run(`Given a non-transactional repository
		When Run is called with a function that succeeds
		Then the function should execute successfully
		And no retries should occur
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory)

		executed := false
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.True(t, executed)
		require.Equal(t, 1, factory.getCallCount())
	})

	t.Run(`Given a non-transactional repository
		When Run is called with a function that returns a non-retryable error
		Then the error should be returned immediately
		And no retries should occur
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3)))

		expectedErr := errors.New("non-retryable error")
		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			return expectedErr
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, expectedErr) || errors.Unwrap(err) == expectedErr)
		require.Contains(t, err.Error(), "non-retryable error")
		require.Equal(t, 1, callCount)
		require.Equal(t, 1, factory.getCallCount())
	})

	t.Run(`Given a non-transactional repository
		When Run is called and function returns ErrTransient
		Then the operation should be retried
		And eventually succeed
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			if callCount < 3 {
				return ErrTransient
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 3, callCount)
		require.Equal(t, 3, factory.getCallCount())
	})

	t.Run(`Given a non-transactional repository
		When Run is called and function returns ErrConcurrentModification
		Then the operation should be retried
		And eventually succeed
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			if callCount < 2 {
				return ErrConcurrentModification
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 2, callCount)
		require.Equal(t, 2, factory.getCallCount())
	})

	t.Run(`Given a non-transactional repository with limited retry attempts
		When Run is called and function always returns ErrTransient
		Then retries should be exhausted
		And ErrTransient should be returned
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(2), retry.Delay(10*time.Millisecond)))

		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			return ErrTransient
		})

		require.Error(t, err)
		require.ErrorIs(t, err, ErrTransient)
		require.Equal(t, 2, callCount)
		require.Equal(t, 2, factory.getCallCount())
	})

	t.Run(`Given a non-transactional repository
		When Run is called and function returns mixed retryable errors
		Then the operation should be retried for each error type
		And eventually succeed
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(5), retry.Delay(10*time.Millisecond)))

		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			if callCount == 1 {
				return ErrTransient
			}
			if callCount == 2 {
				return ErrConcurrentModification
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 3, callCount)
		require.Equal(t, 3, factory.getCallCount())
	})

	t.Run(`Given a ConcurrentScope with default retry options
		When Run is called with custom retry options via RunOptions
		Then custom options should override default options
		And be applied to the retry logic
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(10)))

		customOpts := WithRetryOptions(retry.Attempts(2), retry.Delay(10*time.Millisecond))
		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			return ErrTransient
		}, customOpts)

		require.Error(t, err)
		require.Equal(t, 2, callCount)
	})
}

func TestConcurrentScope_Run_Transactional(t *testing.T) {
	t.Run(`Given a transactional repository
		When Run is called and Begin succeeds
		Then transaction should begin
		And runFunc should execute
		And transaction should commit
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		executed := false
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.True(t, executed)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Begin fails with non-retryable error
		Then Begin error should be returned
		And runFunc should not be called
		And no commit or rollback should occur
	`, func(t *testing.T) {
		beginErr := errors.New("begin failed")
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				beginFunc: func(ctx context.Context) (context.Context, error) {
					return nil, beginErr
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3)))

		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			return nil
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, beginErr) || errors.Unwrap(err) == beginErr)
		require.Contains(t, err.Error(), "begin failed")
		require.Equal(t, 0, callCount)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 0, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Begin returns ErrTransient
		Then Begin should be retried
		And eventually succeed
		And transaction should commit
	`, func(t *testing.T) {
		beginCallCount := 0
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				beginFunc: func(ctx context.Context) (context.Context, error) {
					beginCallCount++
					if beginCallCount < 2 {
						return nil, ErrTransient
					}
					return ctx, nil
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		executed := false
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.True(t, executed)
		require.Equal(t, 2, beginCallCount)
		require.Equal(t, 1, txRepo.getCommitCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc succeeds
		Then transaction should commit successfully
		And no rollback should occur
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Commit fails
		Then Commit error should be returned
		And no rollback should occur
	`, func(t *testing.T) {
		commitErr := errors.New("commit failed")
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				commitFunc: func(ctx context.Context) error {
					return commitErr
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3)))

		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			return nil
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, commitErr) || errors.Unwrap(err) == commitErr)
		require.Contains(t, err.Error(), "commit failed")
		require.Equal(t, 1, callCount)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Commit returns ErrTransient
		Then Commit should be retried
		And eventually succeed
	`, func(t *testing.T) {
		commitCallCount := 0
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				commitFunc: func(ctx context.Context) error {
					commitCallCount++
					if commitCallCount < 2 {
						return ErrTransient
					}
					return nil
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 2, commitCallCount)
		require.Equal(t, 2, txRepo.getBeginCount())
		require.Equal(t, 2, txRepo.getCommitCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns an error
		Then transaction should be rolled back
		And runFunc error should be returned
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		runErr := errors.New("run function error")
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return runErr
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, runErr))
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 0, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository with rollback timeout
		When Run is called and runFunc returns an error
		And Rollback takes longer than timeout
		Then rollback should be cancelled after timeout
		And runFunc error should be returned
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(ctx context.Context) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(2 * time.Second):
						return nil
					}
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)
		scope.rollbackTimeout = 100 * time.Millisecond

		runErr := errors.New("run function error")
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return runErr
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, runErr))
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns an error
		And Rollback also returns an error
		Then both errors should be joined
		And returned together
	`, func(t *testing.T) {
		rollbackErr := errors.New("rollback failed")
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(ctx context.Context) error {
					return rollbackErr
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		runErr := errors.New("run function error")
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return runErr
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, runErr))
		require.True(t, errors.Is(err, rollbackErr))
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns an error
		And Rollback succeeds
		Then runFunc error should be returned
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		runErr := errors.New("run function error")
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return runErr
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, runErr))
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called with a cancelled context
		And runFunc returns an error
		Then Rollback should use independent context
		And not be affected by original context cancellation
	`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		rollbackCalled := false
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(rollbackCtx context.Context) error {
					rollbackCalled = true
					select {
					case <-rollbackCtx.Done():
						return rollbackCtx.Err()
					default:
						return nil
					}
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)
		scope.rollbackTimeout = 100 * time.Millisecond

		runErr := errors.New("run function error")
		err := scope.Run(ctx, func(ctx context.Context, repo Repository) error {
			return runErr
		})

		require.Error(t, err)
		require.True(t, rollbackCalled)
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called with successful runFunc
		Then Begin should be called
		And runFunc should execute
		And Commit should be called
		And no Rollback should occur
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		executed := false
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.True(t, executed)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 0, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns an error
		Then Begin should be called
		And runFunc should execute
		And Rollback should be called
		And no Commit should occur
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		runErr := errors.New("run function error")
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return runErr
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, runErr))
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 0, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Begin succeeds
		And runFunc returns ErrTransient
		Then transaction should be rolled back
		And operation should be retried
		And eventually succeed
	`, func(t *testing.T) {
		runCallCount := 0
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			runCallCount++
			if runCallCount < 2 {
				return ErrTransient
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 2, runCallCount)
		require.Equal(t, 2, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and Begin succeeds
		And runFunc returns ErrConcurrentModification
		Then transaction should be rolled back
		And operation should be retried
		And eventually succeed
	`, func(t *testing.T) {
		runCallCount := 0
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			runCallCount++
			if runCallCount < 2 {
				return ErrConcurrentModification
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 2, runCallCount)
		require.Equal(t, 2, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 1, txRepo.getRollbackCount())
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns ErrTransient multiple times
		Then new transaction should be created on each retry
		And eventually succeed
	`, func(t *testing.T) {
		runCallCount := 0
		createCount := 0
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				createCount++
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			runCallCount++
			if runCallCount < 3 {
				return ErrTransient
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 3, runCallCount)
		require.Equal(t, 3, createCount)
		require.Equal(t, 3, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
		require.Equal(t, 2, txRepo.getRollbackCount())
	})
}

func TestConcurrentScope_Run_ContextHandling(t *testing.T) {
	t.Run(`Given a cancelled context
		When Run is called
		Then context.Canceled error should be returned
	`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory)

		err := scope.Run(ctx, func(runCtx context.Context, repo Repository) error {
			select {
			case <-runCtx.Done():
				return runCtx.Err()
			default:
				return nil
			}
		})

		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run(`Given a context
		When Run is called and context is cancelled during execution
		Then context.Canceled error should be returned
	`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory)

		err := scope.Run(ctx, func(ctx context.Context, repo Repository) error {
			cancel()
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		})

		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run(`Given a context with timeout
		When Run is called and execution exceeds timeout
		Then context.DeadlineExceeded error should be returned
	`, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory)

		err := scope.Run(ctx, func(runCtx context.Context, repo Repository) error {
			select {
			case <-runCtx.Done():
				return runCtx.Err()
			case <-time.After(100 * time.Millisecond):
				return nil
			}
		})

		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run(`Given a context with values
		When Run is called
		Then context should be propagated to factory
		And to runFunc
	`, func(t *testing.T) {
		ctx := context.WithValue(context.Background(), testContextKey, "test-value")
		var receivedCtx context.Context

		factory := &mockRepositoryFactory{
			createFunc: func(factoryCtx context.Context) Repository {
				receivedCtx = factoryCtx
				return &mockRepository{}
			},
		}
		scope := NewConcurrentScope(factory)

		var runFuncCtx context.Context
		err := scope.Run(ctx, func(runCtx context.Context, repo Repository) error {
			runFuncCtx = runCtx
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, ctx, receivedCtx)
		require.NotNil(t, runFuncCtx)
	})

	t.Run(`Given a transactional repository
		When Run is called with a cancelled context
		And runFunc returns an error
		Then Rollback should use independent context
		And not be affected by original context cancellation
	`, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var rollbackCtx context.Context

		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(rbCtx context.Context) error {
					rollbackCtx = rbCtx
					select {
					case <-rbCtx.Done():
						return rbCtx.Err()
					default:
						return nil
					}
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)
		scope.rollbackTimeout = 100 * time.Millisecond

		runErr := errors.New("run function error")
		err := scope.Run(ctx, func(ctx context.Context, repo Repository) error {
			cancel()
			return runErr
		})

		require.Error(t, err)
		require.NotNil(t, rollbackCtx)
		require.NotEqual(t, ctx, rollbackCtx)
	})
}

func TestConcurrentScope_Run_RetryOptionsMerging(t *testing.T) {
	t.Run(`Given a ConcurrentScope with default retry options
		When Run is called without RunOptions
		Then default retry options should be used
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory)

		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			if callCount < 2 {
				return ErrTransient
			}
			return nil
		})

		require.NoError(t, err)
		require.GreaterOrEqual(t, callCount, 2)
	})

	t.Run(`Given a ConcurrentScope created with custom retry options
		When Run is called
		Then custom options should be used
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(2), retry.Delay(10*time.Millisecond)))

		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			return ErrTransient
		})

		require.Error(t, err)
		require.Equal(t, 2, callCount)
	})

	t.Run(`Given a ConcurrentScope with default retry options
		When Run is called with RunOptions
		Then RunOptions should override default options
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(10)))

		customOpts := WithRetryOptions(retry.Attempts(2), retry.Delay(10*time.Millisecond))
		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			return ErrTransient
		}, customOpts)

		require.Error(t, err)
		require.Equal(t, 2, callCount)
	})

	t.Run(`Given a ConcurrentScope
		When Run is called with multiple RunOptions
		Then all RunOptions should be merged
		And applied together
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(10)))

		opts1 := WithRetryOptions(retry.Attempts(5))
		opts2 := WithRetryOptions(retry.Delay(10 * time.Millisecond))
		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			if callCount < 3 {
				return ErrTransient
			}
			return nil
		}, opts1, opts2)

		require.NoError(t, err)
		require.Equal(t, 3, callCount)
	})

	t.Run(`Given a ConcurrentScope
		When Run is called with invalid RunOptions type
		Then invalid options should be ignored gracefully
		And default options should be used
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(2)))

		invalidOpt := "not a retryOptions"
		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			if callCount < 2 {
				return ErrTransient
			}
			return nil
		}, invalidOpt)

		require.NoError(t, err)
		require.Equal(t, 2, callCount)
	})
}

func TestConcurrentScope_Run_ErrorPropagation(t *testing.T) {
	t.Run(`Given a ConcurrentScope
		When Run is called and runFunc returns an error
		Then the error should be propagated correctly
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory)

		expectedErr := errors.New("run function error")
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return expectedErr
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, expectedErr) || errors.Unwrap(err) == expectedErr)
		require.Contains(t, err.Error(), "run function error")
	})

	t.Run(`Given a transactional repository
		When Run is called and Begin returns an error
		Then Begin error should be propagated correctly
	`, func(t *testing.T) {
		beginErr := errors.New("begin error")
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				beginFunc: func(ctx context.Context) (context.Context, error) {
					return nil, beginErr
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return nil
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, beginErr) || errors.Unwrap(err) == beginErr)
		require.Contains(t, err.Error(), "begin error")
	})

	t.Run(`Given a transactional repository
		When Run is called and Commit returns an error
		Then Commit error should be propagated correctly
	`, func(t *testing.T) {
		commitErr := errors.New("commit error")
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				commitFunc: func(ctx context.Context) error {
					return commitErr
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return nil
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, commitErr) || errors.Unwrap(err) == commitErr)
		require.Contains(t, err.Error(), "commit error")
	})

	t.Run(`Given a transactional repository
		When Run is called and runFunc returns an error
		And Rollback also returns an error
		Then both errors should be joined
		And propagated together
	`, func(t *testing.T) {
		rollbackErr := errors.New("rollback error")
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(ctx context.Context) error {
					return rollbackErr
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		runErr := errors.New("run function error")
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return runErr
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, runErr))
		require.True(t, errors.Is(err, rollbackErr))
	})

	t.Run(`Given a transactional repository
		When Run is called and multiple errors occur
		Then all errors should be joined
		And propagated together
	`, func(t *testing.T) {
		rollbackErr1 := errors.New("rollback error 1")
		rollbackErr2 := errors.New("rollback error 2")
		rollbackCallCount := 0
		txRepo := &mockTransactionalRepository{
			mockTransactional: mockTransactional{
				rollbackFunc: func(ctx context.Context) error {
					rollbackCallCount++
					if rollbackCallCount == 1 {
						return rollbackErr1
					}
					return rollbackErr2
				},
			},
		}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		runErr := errors.New("run function error")
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return runErr
		})

		require.Error(t, err)
		require.True(t, errors.Is(err, runErr))
		require.True(t, errors.Is(err, rollbackErr1))
	})
}

func TestConcurrentScope_Run_EdgeCases(t *testing.T) {
	t.Run(`Given a ConcurrentScope
		When Run is called with empty runOptions
		Then operation should execute successfully
		And default options should be used
	`, func(t *testing.T) {
		factory := &mockRepositoryFactory{}
		scope := NewConcurrentScope(factory)

		executed := false
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.True(t, executed)
	})

	t.Run(`Given a repository that implements both Repository and Transactional
		When Run is called
		Then transactional path should be taken
		And Begin and Commit should be called
	`, func(t *testing.T) {
		txRepo := &mockTransactionalRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return txRepo
			},
		}
		scope := NewConcurrentScope(factory)

		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 1, txRepo.getBeginCount())
		require.Equal(t, 1, txRepo.getCommitCount())
	})

	t.Run(`Given a repository that implements only Repository
		When Run is called
		Then non-transactional path should be taken
		And no Begin or Commit should be called
	`, func(t *testing.T) {
		repo := &mockRepository{}
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				return repo
			},
		}
		scope := NewConcurrentScope(factory)

		executed := false
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			executed = true
			return nil
		})

		require.NoError(t, err)
		require.True(t, executed)
	})

	t.Run(`Given a factory that returns different repository instances
		When Run is called and retries occur
		Then new repository instance should be created on each retry
	`, func(t *testing.T) {
		createCount := 0
		factory := &mockRepositoryFactory{
			createFunc: func(ctx context.Context) Repository {
				createCount++
				return &mockRepository{}
			},
		}
		scope := NewConcurrentScope(factory, WithRetryOptions(retry.Attempts(3), retry.Delay(10*time.Millisecond)))

		callCount := 0
		err := scope.Run(context.Background(), func(ctx context.Context, repo Repository) error {
			callCount++
			if callCount < 2 {
				return ErrTransient
			}
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, 2, callCount)
		require.Equal(t, 2, createCount)
	})
}

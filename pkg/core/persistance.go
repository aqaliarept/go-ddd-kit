package core

import (
	"context"
	"errors"
	"time"

	"github.com/avast/retry-go/v4"
)

var ErrConcurrentModification = errors.New("aggregate modified concurrently")
var ErrAggregateExists = errors.New("aggregate already exists")
var ErrAggregateNotFound = errors.New("aggregate not found")
var ErrTransient = errors.New("transient error")
var ErrTransactionNotFound = errors.New("transaction not found")

type (
	LoadOption    any
	SaveOption    any
	SchemaVersion uint64
)

const DefaultSchemaVersion = SchemaVersion(0)
const defaultRollbackTimeout = 5 * time.Second

type Storer interface {
	Store(storeFunc func(id ID, aggregate AggregatePtr, storageState StatePtr, events EventPack, version Version, schemaVersion SchemaVersion) error) error
}

type Restorer interface {
	Restore(id ID, version Version, schemaVersion SchemaVersion, restoreFunc func(state StatePtr) error) error
}

type StateRestorer interface {
	Restore(schemaVersion SchemaVersion, restoreFunc func(state StatePtr) error) error
}

type StateStorer interface {
	Store(storeFunc func(state StatePtr, schemaVersion SchemaVersion) error) error
}

type Repository interface {
	Load(ctx context.Context, id ID, aggregate Restorer, options ...LoadOption) error
	Save(ctx context.Context, aggregate Storer, options ...SaveOption) error
}

type RepositoryFactory interface {
	Create(ctx context.Context) Repository
}

type Transactional interface {
	Begin(ctx context.Context) (context.Context, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type ConcurrentScope struct {
	factory         RepositoryFactory
	retryOpts       []retry.Option
	rollbackTimeout time.Duration
}

func NewConcurrentScope(factory RepositoryFactory, runOptions ...RunOptions) *ConcurrentScope {
	retryIf := retry.RetryIf(func(err error) bool { return errors.Is(err, ErrTransient) || errors.Is(err, ErrConcurrentModification) })
	retryOpts := []retry.Option{retryIf}
	rollbackTimeout := defaultRollbackTimeout
	for _, opt := range runOptions {
		if opts, ok := opt.(retryOptions); ok {
			retryOpts = append(retryOpts, opts.retryOptions...)
		}
		if opts, ok := opt.(rollbackTimeoutOption); ok {
			rollbackTimeout = opts.rollbackTimeout
		}
	}
	return &ConcurrentScope{
		factory:         factory,
		retryOpts:       retryOpts,
		rollbackTimeout: rollbackTimeout,
	}
}

type RunOptions any

type retryOptions struct {
	retryOptions []retry.Option
}

type rollbackTimeoutOption struct {
	rollbackTimeout time.Duration
}

func WithRollbackTimeout(timeout time.Duration) RunOptions {
	return rollbackTimeoutOption{rollbackTimeout: timeout}
}

func WithRetryOptions(opts ...retry.Option) RunOptions {
	return retryOptions{retryOptions: opts}
}

type repoDecorator struct {
	inner   Repository
	changes map[AggregatePtr][]EventPack
}

var _ Repository = (*repoDecorator)(nil)

func (c *repoDecorator) Load(ctx context.Context, id ID, aggregate Restorer, options ...LoadOption) error {
	return c.inner.Load(ctx, id, aggregate, options...)
}

func (c *repoDecorator) Save(ctx context.Context, aggregate Storer, options ...SaveOption) error {
	return c.inner.Save(ctx, aggregate, options...)
}

type storerDecorator struct {
	inner Storer
}

var _ Storer = (*storerDecorator)(nil)

func (c *storerDecorator) Store(storeFunc func(id ID, aggregate AggregatePtr, storageState StatePtr, events EventPack, version Version, schemaVersion SchemaVersion) error) error {
	return c.inner.Store(storeFunc)
}

func (c *ConcurrentScope) Run(ctx context.Context, runFunc func(ctx context.Context, repo Repository) error, runOptions ...RunOptions) error {
	retryOpts := c.retryOpts
	for _, opt := range runOptions {
		if opts, ok := opt.(retryOptions); ok {
			retryOpts = append(retryOpts, opts.retryOptions...)
		}
	}
	return retry.Do(
		func() error {
			repo := c.factory.Create(ctx)
			if transactional, ok := repo.(Transactional); ok {
				var err error
				ctx, err = transactional.Begin(ctx)
				if err != nil {
					return err
				}
				err = runFunc(ctx, repo)
				if err != nil {
					rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.rollbackTimeout)
					defer cancel()
					rollbackErr := transactional.Rollback(rollbackCtx)
					return errors.Join(err, rollbackErr)
				}
				return transactional.Commit(ctx)
			}
			return runFunc(ctx, repo)
		},
		retryOpts...,
	)
}

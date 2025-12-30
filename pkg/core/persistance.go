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

type Storer interface {
	Store(storeFunc func(id ID, state StatePtr, events EventPack, version Version, schemaVersion SchemaVersion) error) error
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
	Factory         RepositoryFactory
	retryOpts       []retry.Option
	rollbackTimeout time.Duration
}

func NewConcurrentScope(factory RepositoryFactory, defaultRetryOpts ...retry.Option) *ConcurrentScope {
	retryIf := retry.RetryIf(func(err error) bool { return errors.Is(err, ErrTransient) || errors.Is(err, ErrConcurrentModification) })
	opts := append([]retry.Option{retryIf}, defaultRetryOpts...)
	return &ConcurrentScope{
		Factory:         factory,
		retryOpts:       opts,
		rollbackTimeout: 10 * time.Second,
	}
}

type RunOptions any

type retryOptions struct {
	retryOptions []retry.Option
}

func WithRetryOptions(opts ...retry.Option) RunOptions {
	return retryOptions{retryOptions: opts}
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
			repo := c.Factory.Create(ctx)
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

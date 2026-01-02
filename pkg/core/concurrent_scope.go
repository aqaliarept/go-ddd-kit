package core

import (
	"context"
	"errors"
	"time"

	"github.com/avast/retry-go/v4"
)

const defaultRollbackTimeout = 5 * time.Second

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

func (c *repoDecorator) Load(ctx context.Context, id ID, restorer Restorer, options ...LoadOption) error {
	return c.inner.Load(ctx, id, restorer, options...)
}

func (c *repoDecorator) Save(ctx context.Context, storer Storer, options ...SaveOption) error {
	decoratedStorer := &storerDecorator{
		Storer: storer,
	}
	err := c.inner.Save(ctx, decoratedStorer, options...)
	if err != nil {
		return err
	}
	aggregatePtr := AggregatePtr(storer)
	c.changes[aggregatePtr] = append(c.changes[aggregatePtr], decoratedStorer.pack)
	return nil
}

type storerDecorator struct {
	Storer
	pack EventPack
}

var _ Storer = (*storerDecorator)(nil)

func (c *storerDecorator) Store(storeFunc func(id ID, aggregate AggregatePtr, storageState StatePtr, events EventPack, version Version, schemaVersion SchemaVersion) error) error {
	return c.Storer.Store(func(id ID, aggregate AggregatePtr, storageState StatePtr, events EventPack, version Version, schemaVersion SchemaVersion) error {
		err := storeFunc(id, aggregate, storageState, events, version, schemaVersion)
		if err != nil {
			return err
		}
		c.pack = events
		return nil
	})
}

func (c *ConcurrentScope) Run(ctx context.Context, runFunc func(ctx context.Context, repo Repository) error, runOptions ...RunOptions) (map[AggregatePtr][]EventPack, error) {
	retryOpts := c.retryOpts
	for _, opt := range runOptions {
		if opts, ok := opt.(retryOptions); ok {
			retryOpts = append(retryOpts, opts.retryOptions...)
		}
	}
	var changes map[AggregatePtr][]EventPack
	return changes, retry.Do(
		func() error {
			changes = make(map[AggregatePtr][]EventPack)
			repo := &repoDecorator{
				changes: changes,
				inner:   c.factory.Create(ctx),
			}
			if transactional, ok := repo.inner.(Transactional); ok {
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

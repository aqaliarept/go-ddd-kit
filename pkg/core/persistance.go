package core

import (
	"context"
	"errors"

	"github.com/avast/retry-go/v4"
)

var ErrConcurrentModification = errors.New("aggregate modified concurrently")
var ErrAggregateExists = errors.New("aggregate already exists")
var ErrAggregateNotFound = errors.New("aggregate not found")
var ErrTransient = errors.New("transient error")
var ErrTransactionNotFound = errors.New("transaction not found")

type LoadOption any
type SaveOption any

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
	Factory   RepositoryFactory
	retryOpts []retry.Option
}

func NewConcurrentScope(factory RepositoryFactory, defaultRetryOpts ...retry.Option) *ConcurrentScope {
	retryIf := retry.RetryIf(func(err error) bool { return errors.Is(err, ErrTransient) || errors.Is(err, ErrConcurrentModification) })
	opts := append([]retry.Option{retryIf}, defaultRetryOpts...)
	return &ConcurrentScope{Factory: factory, retryOpts: opts}
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
			retryOpts = append(c.retryOpts, opts.retryOptions...)
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
					rollbackErr := transactional.Rollback(ctx)
					return errors.Join(err, rollbackErr)
				}
				return transactional.Commit(ctx)
			}
			return runFunc(ctx, repo)
		},
		retryOpts...,
	)
}

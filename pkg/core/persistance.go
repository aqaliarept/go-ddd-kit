package core

import (
	"context"
	"errors"
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
	Store(storeFunc func(id ID, aggregate AggregatePtr, storageState StatePtr, events EventPack, version Version, schemaVersion SchemaVersion) error) error
	StorageOptions() []StorageOption
}

type StorageOption any

type Restorer interface {
	Restore(id ID, version Version, schemaVersion SchemaVersion, restoreFunc func(state StatePtr) error) error
	StorageOptions() []StorageOption
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

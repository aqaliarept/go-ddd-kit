// Package redis provides Redis repository implementation for domain-driven design aggregates.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aqaliarept/go-ddd-kit/pkg/core"
	"github.com/redis/go-redis/v9"
)

var _ core.Repository = (*repository)(nil)

type Namespace string

type NamespacedEntity interface {
	Namespace() Namespace
}

type repository struct {
	conn *redis.Client
}

type AggregateDocument[T any] struct {
	State         T      `json:"state"`
	ID            string `json:"id"`
	Version       uint64 `json:"version"`
	SchemaVersion uint64 `json:"schema_version,omitempty"`
}

func buildStorageKey(ns Namespace, identifier core.ID) string {
	return fmt.Sprintf("%s:%s", string(ns), string(identifier))
}

func (r *repository) Load(ctx context.Context, id core.ID, target core.Restorer, options ...core.LoadOption) error {
	entity, ok := target.(NamespacedEntity)
	if !ok {
		panic("target must implement NamespacedEntity")
	}

	storageKey := buildStorageKey(entity.Namespace(), id)

	rawData, err := r.conn.Get(ctx, storageKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return core.ErrAggregateNotFound
		}
		return fmt.Errorf("retrieval failed: %w", err)
	}

	var doc AggregateDocument[[]byte]
	if err := json.Unmarshal([]byte(rawData), &doc); err != nil {
		return fmt.Errorf("deserialization failed: %w", err)
	}

	return target.Restore(id, core.Version(doc.Version), core.SchemaVersion(doc.SchemaVersion), func(statePtr core.StatePtr) error {
		if err := json.Unmarshal(doc.State, statePtr); err != nil {
			return fmt.Errorf("state deserialization error: %w", err)
		}
		return nil
	})
}

type ttlConfig struct {
	ttl time.Duration
}

func WithExpiration(duration time.Duration) core.SaveOption {
	if duration <= 0 {
		panic("duration must be positive")
	}
	return ttlConfig{ttl: duration}
}

func (r *repository) Save(ctx context.Context, source core.Storer, options ...core.SaveOption) error {
	var ttl time.Duration
	for _, opt := range options {
		if cfg, ok := opt.(ttlConfig); ok {
			ttl = cfg.ttl
			break
		}
	}

	return source.Store(func(identifier core.ID, statePtr core.StatePtr, events core.EventPack, currentVersion core.Version, schemaVersion core.SchemaVersion) error {
		entity, ok := source.(NamespacedEntity)
		if !ok {
			panic("source must implement NamespacedEntity")
		}

		storageKey := buildStorageKey(entity.Namespace(), identifier)

		deletionEvents := core.EventsOfType[core.Tombstone](events)
		if len(deletionEvents) > 0 {
			if currentVersion == 0 {
				return nil
			}

			return r.removeWithVersionCheck(ctx, storageKey, currentVersion)
		}

		stateBytes, err := json.Marshal(statePtr)
		if err != nil {
			return fmt.Errorf("state encoding failed: %w", err)
		}

		nextVersion := currentVersion.Next()

		doc := AggregateDocument[[]byte]{
			ID:            string(identifier),
			Version:       uint64(nextVersion),
			State:         stateBytes,
			SchemaVersion: uint64(schemaVersion),
		}

		docBytes, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("record encoding failed: %w", err)
		}

		if currentVersion == 0 {
			return r.insertNew(ctx, storageKey, docBytes, ttl)
		}

		return r.updateExisting(ctx, storageKey, docBytes, currentVersion, ttl)
	})
}

func (r *repository) removeWithVersionCheck(ctx context.Context, key string, expectedVersion core.Version) error {
	return r.executeWithVersionCheck(ctx, key, expectedVersion, "removal", func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, key)
			return nil
		})
		return err
	})
}

func (r *repository) insertNew(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	created, err := r.conn.SetNX(ctx, key, data, ttl).Result()
	if err != nil {
		return fmt.Errorf("insertion failed: %w", err)
	}
	if !created {
		return core.ErrAggregateExists
	}
	return nil
}

func (r *repository) updateExisting(ctx context.Context, key string, data []byte, expectedRev core.Version, ttl time.Duration) error {
	return r.executeWithVersionCheck(ctx, key, expectedRev, "update", func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, key, data, ttl)
			return nil
		})
		return err
	})
}

func (r *repository) executeWithVersionCheck(ctx context.Context, key string, expectedRev core.Version, operation string, execute func(*redis.Tx) error) error {
	err := r.conn.Watch(ctx, func(tx *redis.Tx) error {
		rawData, err := tx.Get(ctx, key).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return core.ErrAggregateNotFound
			}
			return fmt.Errorf("version check failed: %w", err)
		}

		var doc AggregateDocument[[]byte]
		if err = json.Unmarshal([]byte(rawData), &doc); err != nil {
			return fmt.Errorf("document parsing failed: %w", err)
		}

		if doc.Version != uint64(expectedRev) {
			return core.ErrConcurrentModification
		}

		return execute(tx)
	}, key)

	if err != nil {
		if errors.Is(err, redis.TxFailedErr) {
			return core.ErrConcurrentModification
		}
		if errors.Is(err, core.ErrAggregateNotFound) || errors.Is(err, core.ErrConcurrentModification) {
			return err
		}
		return fmt.Errorf("%s failed: %w", operation, err)
	}

	return nil
}

func newRepository(conn *redis.Client) core.Repository {
	return &repository{conn: conn}
}

type Factory struct {
	conn *redis.Client
}

func NewRepositoryFactory(conn *redis.Client) core.RepositoryFactory {
	return &Factory{conn: conn}
}

func (f *Factory) Create(ctx context.Context) core.Repository {
	return newRepository(f.conn)
}

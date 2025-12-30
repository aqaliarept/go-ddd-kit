package test

import core "github.com/aqaliarept/go-ddd-kit/pkg/core"

type TestAggregate2 interface {
	core.Restorer
	core.Storer
	SingleEventCommand(val string) (core.EventPack, error)
	Remove() (core.EventPack, error)
	ID() core.ID
	Version() core.Version
	State() any
	Events() core.EventPack
	Error() error
}

var _ TestAggregate2 = (*TestAggWrapper[core.Aggregate[any]])(nil)

type TestAggWrapper[T core.Aggregate[any]] struct {
	t T
}

func (ta *TestAggWrapper[T]) agg() *core.Aggregate[any] {
	return any(&ta.t).(*core.Aggregate[any])
}

func (ta *TestAggWrapper[T]) Restore(id core.ID, version core.Version, schemaVersion core.SchemaVersion, restoreFunc func(state core.StatePtr) error) error {
	return ta.agg().Restore(id, version, schemaVersion, restoreFunc)
}

func (ta *TestAggWrapper[T]) Store(storeFunc func(id core.ID, state core.StatePtr, events core.EventPack, version core.Version, schemaVersion core.SchemaVersion) error) error {
	return ta.agg().Store(storeFunc)
}

func (ta *TestAggWrapper[T]) SingleEventCommand(val string) (core.EventPack, error) {
	if cmd, ok := any(&ta.t).(interface {
		SingleEventCommand(string) (core.EventPack, error)
	}); !ok {
		panic("aggregate must implement SingleEventCommand")
	} else {
		return cmd.SingleEventCommand(val)
	}
}

func (ta *TestAggWrapper[T]) Remove() (core.EventPack, error) {
	return ta.agg().Remove()
}

func (ta *TestAggWrapper[T]) ID() core.ID {
	return ta.agg().ID()
}

func (ta *TestAggWrapper[T]) Version() core.Version {
	return ta.agg().Version()
}

func (ta *TestAggWrapper[T]) State() any {
	return ta.agg().State()
}

func (ta *TestAggWrapper[T]) Events() core.EventPack {
	return ta.agg().Events()
}

func (ta *TestAggWrapper[T]) Error() error {
	return ta.agg().Error()
}

// Package test provides testing utilities and types for repository implementations.
// TestAggWrapper uses panic-based error handling for missing method implementations,
// which is intentional for test code to fail fast with clear error messages.
package test

import (
	"reflect"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
)

type TestAggregate interface {
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

var _ TestAggregate = (*TestAggWrapper)(nil)

type TestAggWrapper struct {
	t any
}

func NewTestAggWrapper(agg any) *TestAggWrapper {
	return &TestAggWrapper{t: agg}
}

func (ta *TestAggWrapper) Restore(id core.ID, version core.Version, schemaVersion core.SchemaVersion, restoreFunc func(state core.StatePtr) error) error {
	if cmd, ok := ta.t.(interface {
		Restore(core.ID, core.Version, core.SchemaVersion, func(core.StatePtr) error) error
	}); !ok {
		panic("aggregate must implement Restore")
	} else {
		return cmd.Restore(id, version, schemaVersion, restoreFunc)
	}
}

func (ta *TestAggWrapper) StorageOptions() []core.StorageOption {
	if cmd, ok := ta.t.(interface {
		StorageOptions() []core.StorageOption
	}); !ok {
		return []core.StorageOption{}
	} else {
		return cmd.StorageOptions()
	}
}

func (ta *TestAggWrapper) Store(storeFunc func(id core.ID, aggregate core.AggregatePtr, storageState core.StatePtr, events core.EventPack, version core.Version, schemaVersion core.SchemaVersion) error) error {
	if cmd, ok := ta.t.(interface {
		Store(func(core.ID, core.AggregatePtr, core.StatePtr, core.EventPack, core.Version, core.SchemaVersion) error) error
	}); !ok {
		panic("aggregate must implement Store")
	} else {
		return cmd.Store(storeFunc)
	}
}

func (ta *TestAggWrapper) SingleEventCommand(val string) (core.EventPack, error) {
	if cmd, ok := ta.t.(interface {
		SingleEventCommand(string) (core.EventPack, error)
	}); !ok {
		panic("aggregate must implement SingleEventCommand")
	} else {
		return cmd.SingleEventCommand(val)
	}
}

func (ta *TestAggWrapper) Remove() (core.EventPack, error) {
	if cmd, ok := ta.t.(interface {
		Remove() (core.EventPack, error)
	}); !ok {
		panic("aggregate must implement Remove")
	} else {
		return cmd.Remove()
	}
}

func (ta *TestAggWrapper) ID() core.ID {
	if cmd, ok := ta.t.(interface {
		ID() core.ID
	}); !ok {
		panic("aggregate must implement ID")
	} else {
		return cmd.ID()
	}
}

func (ta *TestAggWrapper) Version() core.Version {
	if cmd, ok := ta.t.(interface {
		Version() core.Version
	}); !ok {
		panic("aggregate must implement Version")
	} else {
		return cmd.Version()
	}
}

// State uses reflection to call the State() method on the wrapped aggregate.
// This is necessary because State() returns different types depending on the aggregate,
// and type assertions cannot be used for methods with different return types.
func (ta *TestAggWrapper) State() any {
	if ta.t == nil {
		panic("wrapped aggregate is nil")
	}
	val := reflect.ValueOf(ta.t)
	var ptrVal reflect.Value
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			panic("aggregate must implement State")
		}
		ptrVal = val
	} else {
		ptrVal = val.Addr()
	}
	method := ptrVal.MethodByName("State")
	if !method.IsValid() {
		panic("aggregate must implement State")
	}
	result := method.Call(nil)
	if len(result) == 0 {
		panic("State method must return a value")
	}
	return result[0].Interface()
}

func (ta *TestAggWrapper) Events() core.EventPack {
	if cmd, ok := ta.t.(interface {
		Events() core.EventPack
	}); !ok {
		panic("aggregate must implement Events")
	} else {
		return cmd.Events()
	}
}

func (ta *TestAggWrapper) Error() error {
	if cmd, ok := ta.t.(interface {
		Error() error
	}); !ok {
		panic("aggregate must implement Error")
	} else {
		return cmd.Error()
	}
}

// GetState extracts and returns the state of the aggregate with the specified type.
// It panics if the state cannot be asserted to the requested type.
func GetState[TState any](agg TestAggregate) TState {
	state, ok := agg.State().(TState)
	if !ok {
		panic("state type assertion failed")
	}
	return state
}

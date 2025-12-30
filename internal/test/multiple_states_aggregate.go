// Package test provides testing utilities and types for repository implementations.
package test

import (
	"fmt"
	"strings"

	core "github.com/aqaliarept/go-ddd-kit/pkg/core"
)

/// StateV1

type StateV1 struct {
	ValueV1 string
	value   string
}

func (s *StateV1) Apply(event core.Event) {
	switch e := event.(type) {
	case Created:
		s.ValueV1 = "version 1"
	case ValueUpdated:
		s.ValueV1 = e.Value
	default:
		core.PanicUnsupportedEvent(event)
	}
}

// Restore implements StateRestorer interface
func (s *StateV1) Restore(schemaVersion core.SchemaVersion, restoreFunc func(state core.StatePtr) error) error {
	switch schemaVersion {
	case core.DefaultSchemaVersion:
		err := restoreFunc(s)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported schema version: %d", schemaVersion)
	}
	s.value = strings.ToUpper(s.ValueV1)
	return nil
}

func (t *StateTestAggV1) SingleEventCommand(val string) (core.EventPack, error) {
	return t.ProcessCommand(func(s *StateV1, er core.EventRiser) error {
		er.Raise(ValueUpdated{Value: val})
		return nil
	})
}

/// StateV2

const StateV2SchemaVersion = core.SchemaVersion(2)

var _ core.StateRestorer = (*StateV2)(nil)
var _ core.StateStorer = (*StateV2)(nil)

type StateV2 struct {
	ValueV2 string
	value   string
}

func (s *StateV2) Apply(event core.Event) {
	switch e := event.(type) {
	case Created:
		s.ValueV2 = "version 2"
	case ValueUpdated:
		s.ValueV2 = e.Value
	default:
		core.PanicUnsupportedEvent(event)
	}
}

func (t *StateTestAggV2) SingleEventCommand(val string) (core.EventPack, error) {
	return t.ProcessCommand(func(s *StateV2, er core.EventRiser) error {
		er.Raise(ValueUpdated{Value: val})
		return nil
	})
}

func (s *StateV2) Restore(schemaVersion core.SchemaVersion, restoreFunc func(state core.StatePtr) error) error {
	switch schemaVersion {
	case core.DefaultSchemaVersion:
		stateV1 := &StateV1{}
		err := restoreFunc(stateV1)
		if err != nil {
			return err
		}
		s.ValueV2 = stateV1.ValueV1
	case StateV2SchemaVersion:
		err := restoreFunc(s)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported schema version: %d", schemaVersion)
	}
	s.value = strings.ToUpper(s.ValueV2)
	return nil
}

// Store implements StateStorer interface
func (s *StateV2) Store(storeFunc func(state core.StatePtr, schemaVersion core.SchemaVersion) error) error {
	return storeFunc(s, StateV2SchemaVersion)
}

// AggV2
type StateTestAggV1 struct {
	core.Aggregate[StateV1]
}

func NewStateTestAggV1(id core.ID) *StateTestAggV1 {
	agg := &StateTestAggV1{}
	agg.Initialize(id, Created{})
	return agg
}

// AggV2
type StateTestAggV2 struct {
	core.Aggregate[StateV2]
}

func NewStateTestAggV2(id core.ID) *StateTestAggV2 {
	agg := &StateTestAggV2{}
	agg.Initialize(id, Created{})
	return agg
}

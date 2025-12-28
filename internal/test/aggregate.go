// Package test provides testing utilities and types for repository implementations.
package test

import (
	"fmt"

	core "github.com/aqaliarept/go-ddd/pkg/core"
)

// NestedEntity represents a nested entity in test state
type NestedEntity struct {
	Map         map[string]string
	String      string
	StringSlice []string
}

// TestAggState represents the state of a test aggregate
type TestAggState struct {
	Map         map[string]NestedEntity
	String      string
	EntitySlice []NestedEntity
	Removed     bool
}

func (t *TestAggState) Apply(event core.Event) {
	switch e := event.(type) {
	case Created:
		t.EntitySlice = make([]NestedEntity, 0)
		t.String = "created"
	case ValueUpdated:
		t.String = e.Value
	case core.Tombstone:
		t.Removed = true
	default:
		panic(fmt.Sprintf("unsupported event %T", event))
	}
}

// Created event
type Created struct{}

// ValueUpdated event
type ValueUpdated struct {
	Value string
}

// TestAgg is the base test aggregate that can be embedded by repository-specific implementations
type TestAgg struct {
	core.Aggregate[TestAggState]
}

// SingleEventCommand executes a command that generates a single event
func (t *TestAgg) SingleEventCommand(val string) (core.EventPack, error) {
	return t.ProcessCommand(func(s *TestAggState, er core.EventRiser) error {
		er.Raise(ValueUpdated{Value: val})
		return nil
	})
}

// NewTestAgg creates a new test aggregate with the given ID
func NewTestAgg(id core.ID) TestAgg {
	agg := TestAgg{}
	agg.Initialize(id, Created{})
	return agg
}

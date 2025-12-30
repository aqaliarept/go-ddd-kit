//go:build !integration

package core

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var _ EventApplier = &testAggState{}
var _ Storer = &testAgg{}
var _ Restorer = &testAgg{}

type nestedEntity struct {
	MyMap    map[string]string
	MyString string
	MySlice  []string
}

type testAggState struct {
	MyMap    map[string]nestedEntity
	MyString string
	MySlice  []nestedEntity
	Removed  bool
}

func newTestAgg(id ID) *testAgg {
	agg := testAgg{}
	agg.Initialize(id, Created{})
	return &agg
}

type Created struct {
}

type ValueUpdated struct {
	value string
}

func (t *testAggState) Apply(event Event) {
	switch e := event.(type) {
	case Created:
		t.MySlice = make([]nestedEntity, 0)
		t.MyString = "created"
	case ValueUpdated:
		t.MyString = e.value
	case Tombstone:
		t.Removed = true
	default:
		PanicUnsupportedEvent(event)
	}
}

type testAgg struct {
	Aggregate[testAggState]
}

const (
	guardErrorValue = "guard_error"
)

func (t *testAgg) SingleEventCommand(val string) (EventPack, error) {
	return t.ProcessCommand(func(s *testAggState, er EventRiser) error {
		if val == guardErrorValue {
			return errors.New("guard error")
		}
		er.Raise(ValueUpdated{val})
		return nil
	})
}

func (t *testAgg) MultipleEventsCommand(val string) (EventPack, error) {
	return t.ProcessCommand(func(s *testAggState, er EventRiser) error {
		evt := ValueUpdated{val}
		er.RaiseTrue(true, evt)
		if val == guardErrorValue {
			return errors.New("guard error")
		}
		return nil
	})
}

func TestAggregate(t *testing.T) {
	t.Run(`Given an aggregate
		When Remove is called
		Then Tombstone event is produced
		And state Removed set to true
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		pack, err := agg.Remove()
		require.NoError(t, err)
		require.Equal(t, EventPack{Tombstone{}}, pack)
		require.True(t, agg.State().Removed)
	})

	t.Run(`Given aggregate with non-zero version
			And with non-empty state
			And events are not empty
			And error is not empty
			When Initialize is called
			Then reset state to after Created event is applied
			And events to Created
			And version to zero
			And error to nil`, func(t *testing.T) {
		agg := testAgg{}
		agg.id = "id"
		agg.state.MyMap = map[string]nestedEntity{"val": {}}
		agg.events = append(agg.events, ValueUpdated{"val"})
		agg.err = errors.New("error")

		newID := ID("new-id")
		agg.Initialize(newID, Created{})

		require.Equal(t, Version(0), agg.version)
		require.Equal(t, 1, len(agg.events))
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, agg.state)
		require.Nil(t, agg.Error())
	})

	t.Run(`Given a newly created aggregate
		When the allowed command is called
		Then event is produced
		And state is updated`, func(t *testing.T) {

		agg := newTestAgg("id")
		const val = "allowed_value"
		pack, err := agg.SingleEventCommand(val)
		require.NoError(t, err)
		evt, err := EventOfType[ValueUpdated](pack)
		require.NoError(t, err)
		require.Equal(t, ValueUpdated{val}, evt)
		require.Equal(t, val, agg.State().MyString)
	})

	t.Run(`Given a newly created aggregate
		When command is called and guard statement fails
		And the command hasn't produced events before the failure
		Then the error is returned
		And the aggregate is still valid`, func(t *testing.T) {
		agg := newTestAgg("id")
		pack, err := agg.SingleEventCommand(guardErrorValue)
		require.Error(t, err)
		require.True(t, IsEmpty(pack))
		require.Nil(t, agg.Error())
	})

	t.Run(`Given a newly created aggregate
		When command is called and guard statement fails
		And the command has produced event before the failure
		Then the error is returned
		And the aggregate is in invalid state`, func(t *testing.T) {
		agg := newTestAgg("id")
		pack, err := agg.MultipleEventsCommand(guardErrorValue)
		require.Error(t, err)
		require.True(t, IsEmpty(pack))
		require.NotNil(t, agg.Error())
	})

	t.Run(`Given a newly created aggregate
		When Store is called
		And persistFunc doesn't return an error
		Then should pass correct valued into persistFunc
		And increment version
		And cleanup events
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		var pState *testAggState
		var pEventPack EventPack
		var pVersion Version
		err := agg.Store(func(id ID, as StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
			var ok bool
			pState, ok = as.(*testAggState)
			if !ok {
				return fmt.Errorf("invalid state type")
			}
			pEventPack = ep
			pVersion = v
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, *pState)
		require.Equal(t, EventPack{Created{}}, pEventPack)
		require.Equal(t, Version(0), pVersion)
		require.Empty(t, agg.events)
		require.Equal(t, Version(1), agg.version)
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, agg.State())
	})

	t.Run(`Given an aggregate without events
		When Store is called
		Then persistFunc shouldn't be called
		And aggregate version shouldn't be changed
	`, func(t *testing.T) {
		agg := testAgg{}
		persistFuncCalled := false
		err := agg.Store(func(id ID, as StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
			persistFuncCalled = true
			return nil
		})
		require.NoError(t, err)
		require.False(t, persistFuncCalled)
		require.Equal(t, Version(0), agg.version)
	})

	t.Run(`Given a newly created aggregate
		When Store is called
		And persistFunc returns an error
		Then aggregate's state shouldn't be changed
	`, func(t *testing.T) {
		agg := newTestAgg("id")
		err := agg.Store(func(id ID, as StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
			return errors.New("error")
		})
		require.Error(t, err)
		require.Equal(t, testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}, agg.State())
		require.Equal(t, EventPack{Created{}}, agg.events)
		require.Equal(t, Version(0), agg.version)
	})

	t.Run(`Given an empty aggregate
			When Restore is called
			Then aggregate's state is restored from params of Restore
		`, func(t *testing.T) {
		agg := testAgg{}
		id := ID("id")
		state := testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}
		err := agg.Restore(id, Version(100), DefaultSchemaVersion, func(state StatePtr) error {
			s, ok := state.(*testAggState)
			if !ok {
				t.Fatalf("invalid state type")
			}
			s.MyString = "created"
			s.MySlice = make([]nestedEntity, 0)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, state, agg.State())
		require.Empty(t, agg.events)
		require.Equal(t, Version(100), agg.version)
	})

	t.Run(`Given a newly created aggregate
		And Error is not nil
		When Restore is called
		Then aggregate's state is restored from params of Restore
		And Error is set to nil
	
		`, func(t *testing.T) {
		id := ID("id")
		agg := newTestAgg("id2")
		agg.err = errors.New("error")
		state := testAggState{MyString: "created", MySlice: make([]nestedEntity, 0)}
		err := agg.Restore(id, Version(100), DefaultSchemaVersion, func(state StatePtr) error {
			s, ok := state.(*testAggState)
			if !ok {
				t.Fatalf("invalid state type")
			}
			s.MyString = "created"
			s.MySlice = make([]nestedEntity, 0)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, id, agg.ID())
		require.Equal(t, state, agg.State())
		require.Empty(t, agg.events)
		require.Equal(t, Version(100), agg.version)
		require.NoError(t, agg.Error())
	})
}

//go:noinline
func Restore(r Restorer) {
	if err := r.Restore("id", 100, DefaultSchemaVersion, func(state StatePtr) error {
		s, ok := state.(*testAggState)
		if !ok {
			panic("invalid state type")
		}
		s.MyString = "created"
		return nil
	}); err != nil {
		panic(err)
	}
}

func BenchmarkAggregate(b *testing.B) {

	b.Run("command allocations", func(b *testing.B) {
		b.ReportAllocs()
		agg := newTestAgg("id")
		for i := 0; i < b.N; i++ {
			_, err := agg.MultipleEventsCommand("val")
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

	b.Run("command restore allocations", func(b *testing.B) {
		b.ReportAllocs()

		agg := newTestAgg("id")
		r := Restorer(agg)
		for i := 0; i < b.N; i++ {
			Restore(r)
		}
	})

	b.Run("command store allocations", func(b *testing.B) {
		b.ReportAllocs()
		agg := newTestAgg("id")
		for i := 0; i < b.N; i++ {
			err := agg.Store(func(i ID, sp StatePtr, ep EventPack, v Version, sv SchemaVersion) error {
				_, ok := sp.(*testAggState)
				if !ok {
					return fmt.Errorf("invalid state type")
				}
				return nil
			})
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})

}

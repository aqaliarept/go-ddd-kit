// Package core provides the core domain-driven design abstractions for aggregates, events, and repositories.
package core

import (
	"errors"
	"fmt"

	"github.com/google/go-cmp/cmp"
)

var (
	ErrNoEvents      = errors.New("no events")
	ErrTooManyEvents = errors.New("too many events")
)

type (
	State     any
	StatePtr  any
	Event     any
	EventPack []Event
	ID        string
	Version   uint64
)

func (v Version) Next() Version {
	return v + 1
}

type Tombstone struct {
}

type EventRiser interface {
	Raise(event Event)
	RaisePack(pack EventPack)
	RaiseNotEqual(first any, second any, event Event)
	RaiseTrue(predicate bool, event Event)
}

type EventApplier interface {
	Apply(event Event)
}

type Storer interface {
	Store(storeFunc func(id ID, state StatePtr, events EventPack, version Version) error) error
}

type Restorer interface {
	Restore(id ID, version Version, restoreFunc func(state StatePtr))
}

type raiser[T State] struct {
	a *Aggregate[T]
}

func (r *raiser[T]) Raise(event Event) {
	r.a.raise(event)
}

func (r *raiser[T]) RaisePack(pack EventPack) {
	for _, e := range pack {
		r.Raise(e)
	}
}

func (r *raiser[T]) RaiseNotEqual(first any, second any, event Event) {
	if !cmp.Equal(first, second) {
		r.Raise(event)
	}
}

func (r *raiser[T]) RaiseTrue(predicate bool, event Event) {
	if predicate {
		r.Raise(event)
	}
}

type Aggregate[T State] struct {
	err     error
	state   T
	raiser  raiser[T]
	id      ID
	events  EventPack
	version Version
}

func (a *Aggregate[T]) checkError() {
	if a.err != nil {
		panic("aggregate state corrupted")
	}
}

func (a *Aggregate[T]) raise(event Event) {
	applier, ok := any(&a.state).(EventApplier)
	if !ok {
		panic("state must implement EventApplier")
	}
	applier.Apply(event)
	a.events = append(a.events, event)
}

func (a *Aggregate[T]) ProcessCommand(handler func(state *T, er EventRiser) error) (EventPack, error) {
	a.checkError()
	eventsCount := len(a.events)
	// this trick removes heap allocation for the raiser struct
	a.raiser = raiser[T]{a}
	err := handler(&a.state, &a.raiser)
	if err == nil {
		return a.events[eventsCount:], nil
	}
	// if an error occur after command has spawned any events
	// then the  aggregate is considered in corrupted state and can't be used anymore
	if eventsCount == len(a.events) {
		return nil, err
	}
	a.err = err
	a.events = nil
	return nil, err
}

func (a *Aggregate[T]) ID() ID {
	a.checkError()
	return a.id
}

func (a *Aggregate[T]) State() T {
	a.checkError()
	return a.state
}

func (a *Aggregate[T]) Initialize(id ID, created Event) {
	if a.version > 0 {
		panic(fmt.Errorf("aggregate is already initialized"))
	}
	a.id = id
	a.version = 0
	var state T
	a.state = state
	a.events = nil
	a.err = nil
	a.raise(created)
}

func (a *Aggregate[T]) Remove() (EventPack, error) {
	return a.ProcessCommand(func(_ *T, er EventRiser) error {
		er.Raise(Tombstone{})
		return nil
	})
}

func (a *Aggregate[T]) Store(storeFunc func(ID, StatePtr, EventPack, Version) error) error {
	a.checkError()
	if len(a.events) == 0 {
		return nil
	}
	err := storeFunc(a.id, &a.state, a.events, a.version)
	if err != nil {
		return err
	}
	a.events = nil
	a.version++
	return nil
}

func (a *Aggregate[TState]) Restore(id ID, version Version, restoreFunc func(state StatePtr)) {
	a.id = id
	a.version = version
	restoreFunc(&a.state)
	a.events = nil
	a.err = nil
}

func (a *Aggregate[TState]) Error() error {
	return a.err
}

func (a *Aggregate[T]) Version() Version {
	a.checkError()
	return a.version
}

func (a *Aggregate[T]) Events() EventPack {
	a.checkError()
	return a.events
}

func PanicUnsupportedEvent(event Event) {
	panic(fmt.Sprintf("unsupported event %T", event))
}

func EventOfType[T any](pack EventPack) (T, error) {
	e := EventsOfType[T](pack)
	var evt T
	if len(e) == 0 {
		return evt, ErrNoEvents
	} else if len(e) > 1 {
		return evt, ErrTooManyEvents
	} else {
		return e[0], nil
	}
}

func EventsOfType[T any](pack EventPack) []T {
	res := make([]T, 0)
	for _, e := range pack {
		switch evt := e.(type) {
		case T:
			res = append(res, evt)
		}
	}
	return res
}

func IsEmpty(pack EventPack) bool {
	return len(pack) == 0
}

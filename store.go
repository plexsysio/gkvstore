package gkvstore

import (
	"context"
	"errors"
	"io"
)

const (
	// SortNatural use natural order
	SortNatural Sort = iota
	// SortCreatedDesc created newest to oldest
	SortCreatedDesc
	// SortCreatedAsc created oldest to newset
	SortCreatedAsc
	// SortUpdatedDesc updated newest to oldest
	SortUpdatedDesc
	// SortUpdatedAsc updated oldest to newset
	SortUpdatedAsc
)

var (
	ErrRecordNotFound      = errors.New("record not found")
	ErrRecordAlreadyExists = errors.New("record already exists")
)

type (
	// Store is a generic KV Store interface which provides an easier
	// interface to access underlying database. It is mainly used to abstract
	// the database used underneath so we can have a uniform API to use for clients
	Store interface {
		Create(context.Context, Item) error
		Read(context.Context, Item) error
		Update(context.Context, Item) error
		Delete(context.Context, Item) error
		List(context.Context, Factory, ListOpt) (<-chan *Result, error)

		io.Closer
	}

	// Item is a generic object which can be used to interact with the store.
	// Users can create their own 'Item' for using the store
	Item interface {
		Serializable

		GetNamespace() string
		GetID() string
	}

	// Serializable interface for the Items to store/retrieve data to/from DB as bytes
	Serializable interface {
		Marshal() ([]byte, error)
		Unmarshal([]byte) error
	}

	// Factory interface provides a way to construct the object while returning results
	// in the list method
	Factory func() Item

	// Result contains result of a single result in a list operation
	Result struct {
		Val Item
		Err error
	}

	// Sort is an enum for using different sorting methods on the query
	Sort int

	// ListOpt provides different options for querying the DB
	// Pagination can be used if supported by underlying DB
	ListOpt struct {
		Page    int64
		Limit   int64
		Sort    Sort
		Version int64
		Filter  ItemFilter
	}

	ItemFilter interface {
		Compare(Item) bool
	}

	// TimeTracker interface implements basic time tracking functionality
	// for the objects. If Item supports this interface, additional indexes
	// can be maintained to support queries based on this
	TimeTracker interface {
		SetCreated(t int64)
		GetCreated() int64
		SetUpdated(t int64)
		GetUpdated() int64
	}

	// IDSetter interface can be used by the DB to provide new IDs for objects.
	// If Item supports this, when we Create the new item we can set a unique ID
	// based on different DB implementations
	IDSetter interface {
		SetID(string)
	}
)

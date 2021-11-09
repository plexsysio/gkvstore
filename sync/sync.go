package syncstore

import (
	"context"
	"sync"

	"github.com/plexsysio/gkvstore"
)

type syncStore struct {
	gkvstore.Store

	mu sync.RWMutex
}

func New(st gkvstore.Store) gkvstore.Store {
	return &syncStore{
		Store: st,
	}
}

func (t *syncStore) Create(ctx context.Context, item gkvstore.Item) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.Store.Create(context.TODO(), item)
}

func (t *syncStore) Read(ctx context.Context, item gkvstore.Item) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.Store.Read(context.TODO(), item)
}

func (t *syncStore) Update(ctx context.Context, item gkvstore.Item) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.Store.Update(context.TODO(), item)
}

func (t *syncStore) Delete(ctx context.Context, item gkvstore.Item) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.Store.Delete(context.TODO(), item)
}

func (t *syncStore) List(ctx context.Context, factory gkvstore.Factory, opts gkvstore.ListOpt) (<-chan *gkvstore.Result, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.Store.List(context.TODO(), factory, opts)
}

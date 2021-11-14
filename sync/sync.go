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

	return t.Store.Create(ctx, item)
}

func (t *syncStore) Read(ctx context.Context, item gkvstore.Item) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.Store.Read(ctx, item)
}

func (t *syncStore) Update(ctx context.Context, item gkvstore.Item) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.Store.Update(ctx, item)
}

func (t *syncStore) Delete(ctx context.Context, item gkvstore.Item) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.Store.Delete(ctx, item)
}

func (t *syncStore) List(ctx context.Context, factory gkvstore.Factory, opts gkvstore.ListOpt) (<-chan *gkvstore.Result, error) {
	t.mu.RLock()

	resChan, err := t.Store.List(ctx, factory, opts)
	if err != nil {
		t.mu.RUnlock()
		return resChan, err
	}

	relayChan := make(chan *gkvstore.Result)
	go func() {
		defer close(relayChan)
		defer t.mu.RUnlock()

		for res := range resChan {
			relayChan <- res
		}
	}()

	return relayChan, nil
}

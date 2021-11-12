package prefixstore

import (
	"context"
	"errors"
	"strings"

	"github.com/plexsysio/gkvstore"
	"go.uber.org/multierr"
)

type prefixStore struct {
	stores map[string]gkvstore.Store
}

var ErrStoreNotConfigured error = errors.New("prefix store not configured")

type Mount struct {
	Prefix string
	Store  gkvstore.Store
}

func New(mnts ...Mount) gkvstore.Store {
	stores := make(map[string]gkvstore.Store)
	for _, mnt := range mnts {
		stores[mnt.Prefix] = mnt.Store
	}
	return &prefixStore{
		stores: stores,
	}
}

func (t *prefixStore) getStore(prefix string) (gkvstore.Store, bool) {
	for k, v := range t.stores {
		if strings.HasPrefix(prefix, k) {
			return v, true
		}
	}
	return nil, false
}

func (t *prefixStore) Create(ctx context.Context, item gkvstore.Item) error {
	st, found := t.getStore(item.GetNamespace())
	if !found {
		return ErrStoreNotConfigured
	}
	return st.Create(ctx, item)
}

func (t *prefixStore) Read(ctx context.Context, item gkvstore.Item) error {
	st, found := t.getStore(item.GetNamespace())
	if !found {
		return ErrStoreNotConfigured
	}
	return st.Read(ctx, item)
}

func (t *prefixStore) Update(ctx context.Context, item gkvstore.Item) error {
	st, found := t.getStore(item.GetNamespace())
	if !found {
		return ErrStoreNotConfigured
	}
	return st.Update(ctx, item)
}

func (t *prefixStore) Delete(ctx context.Context, item gkvstore.Item) error {
	st, found := t.getStore(item.GetNamespace())
	if !found {
		return ErrStoreNotConfigured
	}
	return st.Delete(ctx, item)
}

func (t *prefixStore) List(ctx context.Context, factory gkvstore.Factory, opts gkvstore.ListOpt) (<-chan *gkvstore.Result, error) {
	st, found := t.getStore(factory().GetNamespace())
	if !found {
		return nil, ErrStoreNotConfigured
	}
	return st.List(ctx, factory, opts)
}

func (t *prefixStore) Close() error {
	var err error
	for _, st := range t.stores {
		multierr.AppendInto(&err, st.Close())
	}
	return err
}

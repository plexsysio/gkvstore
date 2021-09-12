package inmem

import (
	"context"
	"errors"
	"fmt"
	"github.com/plexsysio/gkvstore"
	"go.uber.org/atomic"
	"sort"
	"strings"
	"sync"
	"time"
)

type inmemStore struct {
	lk    sync.Mutex
	nonce atomic.Int64
	mp    map[string][]byte
	ttIdx map[string]*ttIndex
}

func New() gkvstore.Store {
	return &inmemStore{
		mp:    make(map[string][]byte, 1000),
		ttIdx: make(map[string]*ttIndex, 10),
	}
}

func (i *inmemStore) synchronize() func() {
	i.lk.Lock()
	return func() {
		i.lk.Unlock()
	}
}

func key(item gkvstore.Item) string {
	return fmt.Sprintf("/%s/%s", item.GetNamespace(), item.GetID())
}

func (i *inmemStore) Create(ctx context.Context, item gkvstore.Item) error {
	defer i.synchronize()()

	if ids, ok := item.(gkvstore.IDSetter); ok {
		ids.SetID(fmt.Sprintf("%d", i.nonce.Inc()))
	}

	if _, found := i.mp[key(item)]; found {
		return gkvstore.ErrRecordAlreadyExists
	}

	if tt, ok := item.(gkvstore.TimeTracker); ok {
		ttIdx, exists := i.ttIdx[item.GetNamespace()]
		if !exists {
			ttIdx = newTTIndex()
			i.ttIdx[item.GetNamespace()] = ttIdx
		}
		timestamp := time.Now().UnixNano()
		tt.SetCreated(timestamp)
		tt.SetUpdated(timestamp)
		ttIdx.created.insert(key(item), timestamp)
		ttIdx.updated.insert(key(item), timestamp)
	}

	itemBuf, err := item.Marshal()
	if err != nil {
		return err
	}
	i.mp[key(item)] = itemBuf

	return nil
}

func (i *inmemStore) Read(ctx context.Context, item gkvstore.Item) error {
	defer i.synchronize()()

	itemBuf, found := i.mp[key(item)]
	if !found {
		return gkvstore.ErrRecordNotFound
	}

	return item.Unmarshal(itemBuf)
}

func (i *inmemStore) Update(ctx context.Context, item gkvstore.Item) error {
	defer i.synchronize()()

	if tt, ok := item.(gkvstore.TimeTracker); ok {
		ttIdx, exists := i.ttIdx[item.GetNamespace()]
		if !exists {
			return errors.New("timetracker index not found")
		}
		old := tt.GetUpdated()
		timestamp := time.Now().UnixNano()
		tt.SetUpdated(timestamp)
		ttIdx.updated.remove(key(item), old)
		ttIdx.updated.insert(key(item), timestamp)
	}

	itemBuf, err := item.Marshal()
	if err != nil {
		return err
	}
	i.mp[key(item)] = itemBuf

	return nil
}

func (i *inmemStore) Delete(ctx context.Context, item gkvstore.Item) error {
	defer i.synchronize()()

	itemBuf, found := i.mp[key(item)]
	if found {
		ttIdx, found := i.ttIdx[item.GetNamespace()]
		if found {
			err := item.Unmarshal(itemBuf)
			if err == nil {
				ttIdx.created.remove(key(item), item.(gkvstore.TimeTracker).GetCreated())
				ttIdx.updated.remove(key(item), item.(gkvstore.TimeTracker).GetUpdated())
			}
		}
		delete(i.mp, key(item))
	}
	return nil
}

func (i *inmemStore) List(
	ctx context.Context,
	factory gkvstore.Factory,
	opts gkvstore.ListOpt,
) (<-chan *gkvstore.Result, error) {

	skip := opts.Page * opts.Limit
	res := make(chan *gkvstore.Result)

	sendResults := func(keyChan <-chan string) {
		defer close(res)

		count := 0
		for k := range keyChan {
			if skip > 0 {
				skip--
				continue
			}
			i.lk.Lock()
			val, found := i.mp[k]
			i.lk.Unlock()
			if !found {
				// best effort continue
				continue
			}
			it := factory()
			err := it.Unmarshal(val)
			select {
			case <-ctx.Done():
				return
			case res <- &gkvstore.Result{Val: it, Err: err}:
			}
			if int64(count) == opts.Limit {
				return
			}
		}
	}

	switch opts.Sort {
	case gkvstore.SortNatural:
		go func() {
			defer i.synchronize()()
			defer close(res)

			count := 0
			for k, v := range i.mp {
				it := factory()
				if !strings.HasPrefix(k, "/"+it.GetNamespace()) {
					continue
				}
				err := it.Unmarshal(v)
				if opts.Filter != nil && err == nil && !opts.Filter.Compare(it) {
					continue
				}
				if skip > 0 {
					skip--
					continue
				}
				select {
				case <-ctx.Done():
					return
				case res <- &gkvstore.Result{Val: it, Err: err}:
					count++
				}
				if int64(count) == opts.Limit {
					return
				}
			}
		}()
	case gkvstore.SortCreatedAsc:
		ttIdx, found := i.ttIdx[factory().GetNamespace()]
		if !found {
			return nil, errors.New("timetracker index not found")
		}
		go sendResults(ttIdx.created.asc())
	case gkvstore.SortCreatedDesc:
		ttIdx, found := i.ttIdx[factory().GetNamespace()]
		if !found {
			return nil, errors.New("timetracker index not found")
		}
		go sendResults(ttIdx.created.desc())
	case gkvstore.SortUpdatedAsc:
		ttIdx, found := i.ttIdx[factory().GetNamespace()]
		if !found {
			return nil, errors.New("timetracker index not found")
		}
		go sendResults(ttIdx.updated.asc())
	case gkvstore.SortUpdatedDesc:
		ttIdx, found := i.ttIdx[factory().GetNamespace()]
		if !found {
			return nil, errors.New("timetracker index not found")
		}
		go sendResults(ttIdx.updated.desc())
	default:
		return nil, errors.New("invalid sort type")
	}
	return res, nil
}

func (i *inmemStore) Close() error {
	return nil
}

type ttIndex struct {
	created intIndex
	updated intIndex
}

func newTTIndex() *ttIndex {
	return &ttIndex{
		created: intIndex{items: make([]indexItem, 0, 100)},
		updated: intIndex{items: make([]indexItem, 0, 100)},
	}
}

type indexItem struct {
	index int64
	keys  []string
}

type intIndex struct {
	lk    sync.Mutex
	items []indexItem
}

func (i *intIndex) synchronize() func() {
	i.lk.Lock()
	return func() {
		i.lk.Unlock()
	}
}

const sizeThreshold = 15

func (i *intIndex) insert(key string, index int64) {
	if cap(i.items)-len(i.items) < sizeThreshold {
		i.items = append(i.items, make([]indexItem, 100)...)
	}
	defer i.synchronize()()
	if len(i.items) == 0 || i.items[len(i.items)-1].index < index {
		i.items = append(i.items, indexItem{index, []string{key}})
		return
	}
	idx := sort.Search(len(i.items), func(idx int) bool { return i.items[idx].index >= index })
	if i.items[idx].index == index {
		i.items[idx].keys = append(i.items[idx].keys, key)
	} else {
		i.items = append(i.items, indexItem{})
		copy(i.items[idx+1:], i.items[idx:])
		i.items[idx] = indexItem{
			index: index,
			keys:  []string{key},
		}
	}
}

func (i *intIndex) remove(key string, oldIndex int64) {
	defer i.synchronize()()
	idx := sort.Search(len(i.items), func(idx int) bool { return i.items[idx].index >= oldIndex })
	if idx < len(i.items) && i.items[idx].index == oldIndex {
		if len(i.items[idx].keys) == 1 {
			copy(i.items[idx:], i.items[idx+1:])
			i.items = i.items[:len(i.items)-1]
		} else {
			for k, v := range i.items[idx].keys {
				if v == key {
					i.items[idx].keys = append(i.items[idx].keys[:k], i.items[idx].keys[k+1:]...)
					break
				}
			}
		}
	}
}

func (i *intIndex) asc() <-chan string {
	res := make(chan string)
	go func() {
		defer i.synchronize()()
		defer close(res)

		for idx := 0; idx < len(i.items); idx++ {
			for _, v := range i.items[idx].keys {
				res <- v
			}
		}
	}()
	return res
}

func (i *intIndex) desc() <-chan string {
	res := make(chan string)
	go func() {
		defer i.synchronize()()
		defer close(res)

		for idx := len(i.items) - 1; idx >= 0; idx-- {
			for _, v := range i.items[idx].keys {
				res <- v
			}
		}
	}()
	return res
}

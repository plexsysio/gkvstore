package testsuite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	store "github.com/plexsysio/gkvstore"
)

type Testsuite int

const (
	Basic Testsuite = iota
	Advanced
)

type testStruct struct {
	Namespace string
	Id        string
	RandStr   string
	CreatedAt int64
	UpdatedAt int64
}

func testFactory() store.Item {
	return &testStruct{
		Namespace: "StreamSpace",
	}
}

func (t *testStruct) GetNamespace() string { return t.Namespace }

func (t *testStruct) GetID() string { return t.Id }

func (t *testStruct) Marshal() ([]byte, error) { return json.Marshal(t) }

func (t *testStruct) Unmarshal(val []byte) error { return json.Unmarshal(val, t) }

func (t *testStruct) SetCreated(unixTime int64) { t.CreatedAt = unixTime }

func (t *testStruct) SetUpdated(unixTime int64) { t.UpdatedAt = unixTime }

func (t *testStruct) GetCreated() int64 { return t.CreatedAt }

func (t *testStruct) GetUpdated() int64 { return t.UpdatedAt }

func RunTestsuite(t *testing.T, impl store.Store, suite Testsuite) {
	switch suite {
	case Basic:
		t.Run("Basic", func(st *testing.T) {
			st.Run("NilStore", func(st2 *testing.T) {
				TestCloseStore(st2, impl)
			})
			st.Run("CRUD", func(st2 *testing.T) {
				TestSimpleCRUD(st2, impl)
			})
			st.Run("NaturalLIST", func(st2 *testing.T) {
				TestSortNaturalLIST(st2, impl)
			})
		})
	case Advanced:
		t.Run("Advanced", func(st *testing.T) {
			st.Run("NilStore", func(st2 *testing.T) {
				TestCloseStore(st2, impl)
			})
			st.Run("CRUD", func(st2 *testing.T) {
				TestSimpleCRUD(st2, impl)
			})
			st.Run("NaturalLIST", func(st2 *testing.T) {
				TestSortNaturalLIST(st2, impl)
			})
			st.Run("CreatedAscLIST", func(st2 *testing.T) {
				TestSortCreatedAscLIST(st2, impl)
			})
			st.Run("CreatedDscLIST", func(st2 *testing.T) {
				TestSortCreatedDscLIST(st2, impl)
			})
			st.Run("UpdatedAscLIST", func(st2 *testing.T) {
				TestSortUpdatedAscLIST(st2, impl)
			})
			st.Run("UpdatedDscLIST", func(st2 *testing.T) {
				TestSortUpdatedDscLIST(st2, impl)
			})
			st.Run("FilterLIST", func(st2 *testing.T) {
				TestFilterLIST(st2, impl)
			})
		})
	}
}

func TestCloseStore(t *testing.T, s store.Store) {
	if s == nil {
		t.Fatal("Store should not be nil")
	}
	err := s.Close()
	if err != nil {
		t.Fatal("error closing store:" + err.Error())
	}
}

func TestSimpleCRUD(t *testing.T, s store.Store) {
	// Create new object
	d := &testStruct{
		Namespace: "SS",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
		RandStr:   "totally random",
	}
	err := s.Create(context.TODO(), d)
	if err != nil {
		t.Fatalf(err.Error())
	}
	// Read object and verify contents
	nd := &testStruct{
		Namespace: "SS",
		Id:        "04791e92-0b85-11ea-8d71-362b9e155667",
	}
	err = s.Read(context.TODO(), nd)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if nd.RandStr != d.RandStr {
		t.Fatalf("Incorrect contents during read")
	}
	// Update object and verify contents again on reading
	d.RandStr = "not totally random"
	err = s.Update(context.TODO(), d)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = s.Read(context.TODO(), nd)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if nd.RandStr != d.RandStr || nd.RandStr != "not totally random" {
		t.Fatalf("Incorrect contents during read after update")
	}
	// Delete object and make sure its not readable
	err = s.Delete(context.TODO(), d)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = s.Read(context.TODO(), d)
	if !errors.Is(err, store.ErrRecordNotFound) {
		t.Fatalf("Expected not found error on read after delete")
	}
}

func TestSortNaturalLIST(t *testing.T, s store.Store) {
	// Create some dummies with StreamSpace namespace
	for i := 0; i < 5; i++ {
		d := testStruct{
			Namespace: "StreamSpace",
			Id:        uuid.New().String(),
			RandStr:   fmt.Sprintf("random %d", i),
		}
		err := s.Create(context.TODO(), &d)
		if err != nil {
			t.Fatalf(err.Error())
		}
		// Required for varying timestamps
		time.Sleep(time.Second)
	}
	// Create some dummies with Other namespace
	for i := 0; i < 5; i++ {
		d := testStruct{
			Namespace: "Other",
			Id:        uuid.New().String(),
			RandStr:   fmt.Sprintf("random %d", i),
		}
		err := s.Create(context.TODO(), &d)
		if err != nil {
			t.Fatalf(err.Error())
		}
		// Required for varying timestamps
		time.After(time.Second)
	}
	// Sort '0' is Natural
	opts := store.ListOpt{
		Page:  0,
		Limit: 2,
	}
	// Verify no. of entries with "StreamSpace" NS in list
	var ssEntries, otherEntries = 0, 0
	for i := 0; i < 3; i++ {
		ds, err := s.List(context.TODO(), testFactory, opts)
		if err != nil {
			t.Fatalf(err.Error())
		}
		count := 0
		for v := range ds {
			if v.Err != nil {
				t.Fatal(v.Err)
			}
			if v.Val.GetNamespace() == "StreamSpace" {
				ssEntries++
			}
			if v.Val.GetNamespace() == "Other" {
				otherEntries++
			}
			count++
		}
		// Although we say limit is '3', we should only get 1 item in the last
		// iteration as there are in all only 10 items
		if (i != 2 && count != 2) || (i == 2 && count != 1) {
			t.Fatalf("Unexpected entries in query i: %d Count: %d", i, count)
		}
		opts.Page++
	}
	if ssEntries != 5 || otherEntries != 0 {
		t.Fatalf("Incorrect entries in List")
	}
}

// Test uses entries from the previous List test
func TestSortCreatedAscLIST(t *testing.T, s store.Store) {
	opts := store.ListOpt{
		Page:  0,
		Limit: 10,
		Sort:  store.SortCreatedAsc,
	}
	ds, err := s.List(context.TODO(), testFactory, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	count := 0
	var created int64 = 0
	for item := range ds {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Val.(store.TimeTracker).GetCreated() < created {
			t.Fatalf("Found older created timestamp in ASC List")
		}
		created = item.Val.(store.TimeTracker).GetCreated()
		count++
	}
	if count != 5 {
		t.Fatal("Invalid no of entries", count, "expected 5")
	}
}

// Test uses entries from the previous List test
func TestSortCreatedDscLIST(t *testing.T, s store.Store) {
	opts := store.ListOpt{
		Page:  0,
		Limit: 10,
		Sort:  store.SortCreatedDesc,
	}
	ds, err := s.List(context.TODO(), testFactory, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	count := 0
	var created int64 = 0
	for item := range ds {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Val.(store.TimeTracker).GetCreated() > created && created != 0 {
			t.Fatalf("Found older created timestamp in ASC List")
		}
		created = item.Val.(store.TimeTracker).GetCreated()
		count++
	}
	if count != 5 {
		t.Fatal("Invalid no of entries", count, "expected 5")
	}
}

// Test uses entries from the previous List test
func TestSortUpdatedAscLIST(t *testing.T, s store.Store) {
	opts := store.ListOpt{
		Page:  0,
		Limit: 10,
		Sort:  store.SortUpdatedAsc,
	}
	ds, err := s.List(context.TODO(), testFactory, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	count := 0
	var updated int64 = 0
	for item := range ds {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Val.(store.TimeTracker).GetUpdated() < updated {
			t.Fatalf("Found older updated timestamp in ASC List")
		}
		updated = item.Val.(store.TimeTracker).GetUpdated()
		count++
	}
	if count != 5 {
		t.Fatal("Invalid no of entries", count, "expected 5")
	}
}

// Test uses entries from the previous List test
func TestSortUpdatedDscLIST(t *testing.T, s store.Store) {
	opts := store.ListOpt{
		Page:  0,
		Limit: 10,
		Sort:  store.SortUpdatedDesc,
	}
	ds, err := s.List(context.TODO(), testFactory, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	count := 0
	var updated int64 = 0
	for item := range ds {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Val.(store.TimeTracker).GetUpdated() > updated && updated != 0 {
			t.Fatalf("Found older updated timestamp in ASC List")
		}
		updated = item.Val.(store.TimeTracker).GetUpdated()
		count++
	}
	if count != 5 {
		t.Fatal("Invalid no of entries", count, "expected 5")
	}
}

type filterRandStr struct {
	str string
}

func (f filterRandStr) Compare(i store.Item) bool {
	st, ok := i.(*testStruct)
	if !ok {
		return false
	}
	return st.RandStr == f.str
}

// Test uses entries from the previous List test
func TestFilterLIST(t *testing.T, s store.Store) {
	opts := store.ListOpt{
		Page:  0,
		Limit: 3,
		Filter: filterRandStr{
			str: "random 3",
		},
	}
	ds, err := s.List(context.TODO(), testFactory, opts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	count := 0
	for item := range ds {
		if item.Val.(*testStruct).RandStr != "random 3" {
			t.Fatalf("Invalid filter value in results")
		}
		count++
	}
	if count != 1 {
		t.Fatalf("Filter should find only 1 entry Found: %d", count)
	}
}

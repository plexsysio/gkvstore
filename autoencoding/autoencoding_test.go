package autoencoding_test

import (
	"testing"
	"time"

	"github.com/plexsysio/gkvstore"
	"github.com/plexsysio/gkvstore/autoencoding"
	"github.com/plexsysio/gkvstore/autoencoding/internal/pbtest"
)

func TestNew(t *testing.T) {
	t.Run("no id field", func(st *testing.T) {
		type struct1 struct {
			Namespace string
			Val       string
		}
		_, err := autoencoding.New(&struct1{})
		if err == nil {
			st.Fatal("expected error for no id")
		}
	})
	t.Run("no namespace field", func(st *testing.T) {
		type struct1 struct {
			Id  string
			Val string
		}
		_, err := autoencoding.New(&struct1{})
		if err == nil {
			st.Fatal("expected error for no namespace")
		}
	})
	t.Run("success without tags", func(st *testing.T) {
		type struct1 struct {
			Id        string
			Namespace string
			Val       string
		}
		it, err := autoencoding.New(&struct1{})
		if err != nil {
			st.Fatal("expected no error")
		}
		it.(gkvstore.IDSetter).SetID("testID")
		if it.GetID() != "testID" {
			st.Fatal("unable to set ID")
		}
	})
	t.Run("success with tags", func(st *testing.T) {
		type struct1 struct {
			IdField        string `aenc:"id"`
			NamespaceField string `aenc:"namespace"`
			Val            string
		}
		it, err := autoencoding.New(&struct1{})
		if err != nil {
			st.Fatal("expected no error")
		}
		it.(gkvstore.IDSetter).SetID("testID")
		if it.GetID() != "testID" {
			st.Fatal("unable to set ID")
		}
	})
	t.Run("success with tags and additional tags", func(st *testing.T) {
		type struct1 struct {
			IdField        string `json:"id" aenc:"id"`
			NamespaceField string `json:"namespace,omitempty" aenc:"namespace"`
			Val            string
		}
		_, err := autoencoding.New(&struct1{})
		if err != nil {
			st.Fatal("expected no error")
		}
	})
	t.Run("duplicate field and tag", func(st *testing.T) {
		type struct1 struct {
			Id             string
			IdField        string `aenc:"id"`
			NamespaceField string `aenc:"namespace"`
			Val            string
		}
		_, err := autoencoding.New(&struct1{})
		if err == nil {
			st.Fatal("expected error for duplicate id")
		}
	})
	t.Run("MustNew panics on incorrect", func(st *testing.T) {
		type struct1 struct {
			IdField string
			Ns      string
			Val     string
		}
		panics := false
		done := make(chan struct{})
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panics = true
				}
				close(done)
			}()
			_ = autoencoding.MustNew(&struct1{})
		}()
		<-done
		if !panics {
			st.Fatal("expected MustNew to panic")
		}
	})
	t.Run("timeTracker", func(st *testing.T) {
		type struct1 struct {
			ID        string
			Namespace string
			Created   int64
			Updated   int64
		}
		it, err := autoencoding.New(&struct1{})
		if err != nil {
			st.Fatal("expected no error")
		}
		tt, ok := it.(gkvstore.TimeTracker)
		if !ok {
			st.Fatal("timetracker implementation expected")
		}
		ts := time.Now().UnixNano()
		tt.SetCreated(ts)
		tt.SetUpdated(ts)
		if tt.GetCreated() != tt.GetUpdated() || tt.GetCreated() != ts {
			st.Fatal("invalid timetracker implementation")
		}
	})
}

func TestEncoding(t *testing.T) {
	t.Run("JSON", func(st *testing.T) {
		type jsonItem struct {
			Name string `json:"name" aenc:"namespace"`
			ID   string `json:"id"`
			Val1 int    `json:"val1"`
			Val2 string `json:"val2"`
		}
		t1 := &jsonItem{
			Name: "test1",
			ID:   "1",
			Val1: 100,
			Val2: "test1 JSON",
		}
		ae1 := autoencoding.MustNew(t1)

		buf, err := ae1.Marshal()
		if err != nil {
			st.Fatalf("failed marshalling %v", err)
		}

		st.Log("Size of packing", len(buf))

		t2 := &jsonItem{}
		ae2 := autoencoding.MustNew(t2)

		err = ae2.Unmarshal(buf)
		if err != nil {
			st.Fatalf("failed unmarshaling %v", err)
		}

		if t1.Name != t2.Name || t1.ID != t2.ID || t1.Val1 != t2.Val1 || t1.Val2 != t2.Val2 {
			st.Fatalf("read incorrect value expected %v found %v", t1, t2)
		}
	})
	t.Run("Gob", func(st *testing.T) {
		type gobItem struct {
			Name string `aenc:"namespace"`
			ID   string
			Val1 int
			Val2 string
		}
		t1 := &gobItem{
			Name: "test1",
			ID:   "1",
			Val1: 100,
			Val2: "test1 JSON",
		}
		ae1 := autoencoding.MustNew(t1)

		buf, err := ae1.Marshal()
		if err != nil {
			st.Fatalf("failed marshalling %v", err)
		}

		st.Log("Size of packing", len(buf))

		t2 := &gobItem{}
		ae2 := autoencoding.MustNew(t2)

		err = ae2.Unmarshal(buf)
		if err != nil {
			st.Fatalf("failed unmarshaling %v", err)
		}

		if t1.Name != t2.Name || t1.ID != t2.ID || t1.Val1 != t2.Val1 || t1.Val2 != t2.Val2 {
			st.Fatalf("read incorrect value expected %v found %v", t1, t2)
		}
	})
	t.Run("MsgPack", func(st *testing.T) {
		type msgpackItem struct {
			Name string `msgpack:"name" aenc:"namespace"`
			ID   string `msgpack:"id"`
			Val1 int    `msgpack:"val1"`
			Val2 string `msgpack:"val2"`
		}
		t1 := &msgpackItem{
			Name: "test1",
			ID:   "1",
			Val1: 100,
			Val2: "test1 JSON",
		}
		ae1 := autoencoding.MustNew(t1)

		buf, err := ae1.Marshal()
		if err != nil {
			st.Fatalf("failed marshalling %v", err)
		}

		st.Log("Size of packing", len(buf))

		t2 := &msgpackItem{}
		ae2 := autoencoding.MustNew(t2)

		err = ae2.Unmarshal(buf)
		if err != nil {
			st.Fatalf("failed unmarshaling %v", err)
		}

		if t1.Name != t2.Name || t1.ID != t2.ID || t1.Val1 != t2.Val1 || t1.Val2 != t2.Val2 {
			st.Fatalf("read incorrect value expected %v found %v", t1, t2)
		}
	})
	t.Run("Protobuf", func(st *testing.T) {
		t1 := &pbtest.TestItem{
			Namespace: "test1",
			Id:        "1",
			Val1:      100,
			Val2:      "test1 JSON",
		}
		ae1 := autoencoding.MustNew(t1)

		buf, err := ae1.Marshal()
		if err != nil {
			st.Fatalf("failed marshalling %v", err)
		}

		st.Log("Size of packing", len(buf))

		t2 := &pbtest.TestItem{}
		ae2 := autoencoding.MustNew(t2)

		err = ae2.Unmarshal(buf)
		if err != nil {
			st.Fatalf("failed unmarshaling %v", err)
		}

		if t1.Namespace != t2.Namespace || t1.Id != t2.Id || t1.Val1 != t2.Val1 || t1.Val2 != t2.Val2 {
			st.Fatalf("read incorrect value expected %v found %v", t1, t2)
		}

	})
}

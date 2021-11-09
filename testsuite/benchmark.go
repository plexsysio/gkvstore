package testsuite

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"math/rand"
	"testing"

	"github.com/plexsysio/gkvstore"
)

type testBStruct struct {
	Key     string
	Val     []byte
	Size    int64
	Created int64
	Updated int64
}

func newStruct(size int) *testBStruct {
	return &testBStruct{Size: int64(size)}
}

func (t *testBStruct) GetNamespace() string { return "testStruct" }

func (t *testBStruct) GetID() string { return t.Key }

func (t *testBStruct) SetID(id string) { t.Key = id }

func (t *testBStruct) GetCreated() int64 { return t.Created }

func (t *testBStruct) SetCreated(created int64) { t.Created = created }

func (t *testBStruct) GetUpdated() int64 { return t.Updated }

func (t *testBStruct) SetUpdated(updated int64) { t.Updated = updated }

func (t *testBStruct) Marshal() ([]byte, error) {
	if t.Val == nil {
		t.Val = make([]byte, t.Size-32)
		_, err := rand.Read(t.Val)
		if err != nil {
			return nil, err
		}
	}
	var buf bytes.Buffer
	defer buf.Reset()
	err := gob.NewEncoder(&buf).Encode(t)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (t *testBStruct) Unmarshal(buf []byte) error {
	bw := bytes.NewBuffer(buf)
	defer bw.Reset()
	return gob.NewDecoder(bw).Decode(t)
}

func (t *testBStruct) edit(buf []byte) {
	idx := rand.Intn(len(t.Val) - len(buf) - 1)
	copy(t.Val[idx:idx+len(buf)], buf)
}

func (t *testBStruct) setKey(key string) {
	t.Key = key
}

type testCStruct struct {
	Key  string
	Val  []byte
	Size int64
}

func newStructC(size int) *testCStruct {
	return &testCStruct{Size: int64(size)}
}

func (t *testCStruct) GetNamespace() string { return "testStruct" }

func (t *testCStruct) GetID() string { return t.Key }

func (t *testCStruct) SetID(id string) { t.Key = id }

func (t *testCStruct) Marshal() ([]byte, error) {
	if t.Val == nil {
		t.Val = make([]byte, t.Size-32)
		_, err := rand.Read(t.Val)
		if err != nil {
			return nil, err
		}
	}
	var buf bytes.Buffer
	defer buf.Reset()
	err := gob.NewEncoder(&buf).Encode(t)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (t *testCStruct) edit(buf []byte) {
	idx := rand.Intn(len(t.Val) - len(buf) - 1)
	copy(t.Val[idx:idx+len(buf)], buf)
}

func (t *testCStruct) setKey(key string) {
	t.Key = key
}

type testStructHelper interface {
	edit([]byte)
	setKey(string)
}

func (t *testCStruct) Unmarshal(buf []byte) error {
	bw := bytes.NewBuffer(buf)
	defer bw.Reset()
	return gob.NewDecoder(bw).Decode(t)
}

func BenchmarkCreate(sb *testing.B, st gkvstore.Store, newStruct func() gkvstore.Item) {
	sb.ReportAllocs()
	sb.ResetTimer()

	for n := 0; n < sb.N; n++ {
		it := newStruct()
		err := st.Create(context.TODO(), it)
		if err != nil {
			sb.Fatal(err)
		}
	}
}

func BenchmarkUpdate(sb *testing.B, st gkvstore.Store, newStruct func() gkvstore.Item) {

	var items []gkvstore.Item
	for n := 0; n < sb.N; n++ {
		it := newStruct()
		err := st.Create(context.TODO(), it)
		if err != nil {
			sb.Fatal(err)
		}
		items = append(items, it)
	}

	editBuf := make([]byte, 20)
	_, err := rand.Read(editBuf)
	if err != nil {
		sb.Fatal(err)
	}

	sb.ReportAllocs()
	sb.ResetTimer()

	for _, v := range items {
		v.(testStructHelper).edit(editBuf)
		err := st.Update(context.TODO(), v)
		if err != nil {
			sb.Fatal(err)
		}
	}
}

func BenchmarkRead(sb *testing.B, st gkvstore.Store, newStruct func() gkvstore.Item) {

	for n := 0; n < sb.N; n++ {
		it := newStruct()
		err := st.Create(context.TODO(), it)
		if err != nil {
			sb.Fatal(err)
		}
	}

	sb.ReportAllocs()
	sb.ResetTimer()

	for n := 0; n < sb.N; n++ {
		it := newStruct()
		it.(testStructHelper).setKey(fmt.Sprintf("%d", n+1))

		err := st.Read(context.TODO(), it)
		if err != nil {
			sb.Fatal(err)
		}
	}
}

func BenchmarkSuite(b *testing.B, st gkvstore.Store) {
	b.Run("With time tracker", func(sb1 *testing.B) {
		sb1.Run("64B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, func() gkvstore.Item { return newStruct(64) }) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, func() gkvstore.Item { return newStruct(64) }) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, func() gkvstore.Item { return newStruct(64) }) })
		})
		sb1.Run("128B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, func() gkvstore.Item { return newStruct(128) }) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, func() gkvstore.Item { return newStruct(128) }) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, func() gkvstore.Item { return newStruct(128) }) })
		})
		sb1.Run("256B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, func() gkvstore.Item { return newStruct(256) }) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, func() gkvstore.Item { return newStruct(256) }) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, func() gkvstore.Item { return newStruct(256) }) })
		})
		sb1.Run("512B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, func() gkvstore.Item { return newStruct(512) }) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, func() gkvstore.Item { return newStruct(512) }) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, func() gkvstore.Item { return newStruct(512) }) })
		})
	})
	b.Run("Without time tracker", func(sb1 *testing.B) {
		sb1.Run("64B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, func() gkvstore.Item { return newStructC(64) }) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, func() gkvstore.Item { return newStructC(64) }) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, func() gkvstore.Item { return newStructC(64) }) })
		})
		sb1.Run("128B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, func() gkvstore.Item { return newStructC(128) }) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, func() gkvstore.Item { return newStructC(128) }) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, func() gkvstore.Item { return newStructC(128) }) })
		})
		sb1.Run("256B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, func() gkvstore.Item { return newStructC(256) }) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, func() gkvstore.Item { return newStructC(256) }) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, func() gkvstore.Item { return newStructC(256) }) })
		})
		sb1.Run("512B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, func() gkvstore.Item { return newStructC(512) }) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, func() gkvstore.Item { return newStructC(512) }) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, func() gkvstore.Item { return newStructC(512) }) })
		})
	})
}

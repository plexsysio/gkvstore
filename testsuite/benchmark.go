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

func BenchmarkCreate(sb *testing.B, st gkvstore.Store, size int) {
	sb.ReportAllocs()
	sb.ResetTimer()

	for n := 0; n < sb.N; n++ {
		it := newStruct(size)
		err := st.Create(context.TODO(), it)
		if err != nil {
			sb.Fatal(err)
		}
	}
}

func BenchmarkUpdate(sb *testing.B, st gkvstore.Store, size int) {

	var items []*testBStruct
	for n := 0; n < sb.N; n++ {
		it := newStruct(size)
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
		copy(v.Val[:20], editBuf)
		err := st.Update(context.TODO(), v)
		if err != nil {
			sb.Fatal(err)
		}
	}
}

func BenchmarkRead(sb *testing.B, st gkvstore.Store, size int) {

	for n := 0; n < sb.N; n++ {
		it := newStruct(size)
		err := st.Create(context.TODO(), it)
		if err != nil {
			sb.Fatal(err)
		}
	}

	sb.ReportAllocs()
	sb.ResetTimer()

	for n := 0; n < sb.N; n++ {
		it := newStruct(size)
		it.Key = fmt.Sprintf("%d", n+1)

		err := st.Read(context.TODO(), it)
		if err != nil {
			sb.Fatal(err)
		}
	}
}

func BenchmarkSuite(b *testing.B, st gkvstore.Store) {
	b.Run("64B", func(sb *testing.B) {
		sb.Run("Create", func(sb2 *testing.B) { BenchmarkCreate(sb2, st, 64) })
		sb.Run("Read", func(sb2 *testing.B) { BenchmarkRead(sb2, st, 64) })
		sb.Run("Update", func(sb2 *testing.B) { BenchmarkUpdate(sb2, st, 64) })
	})
	b.Run("128B", func(sb *testing.B) {
		sb.Run("Create", func(sb2 *testing.B) { BenchmarkCreate(sb2, st, 128) })
		sb.Run("Read", func(sb2 *testing.B) { BenchmarkRead(sb2, st, 128) })
		sb.Run("Update", func(sb2 *testing.B) { BenchmarkUpdate(sb2, st, 128) })
	})
	b.Run("256B", func(sb *testing.B) {
		sb.Run("Create", func(sb2 *testing.B) { BenchmarkCreate(sb2, st, 256) })
		sb.Run("Read", func(sb2 *testing.B) { BenchmarkRead(sb2, st, 256) })
		sb.Run("Update", func(sb2 *testing.B) { BenchmarkUpdate(sb2, st, 256) })
	})
	b.Run("512B", func(sb *testing.B) {
		sb.Run("Create", func(sb2 *testing.B) { BenchmarkCreate(sb2, st, 512) })
		sb.Run("Read", func(sb2 *testing.B) { BenchmarkRead(sb2, st, 512) })
		sb.Run("Update", func(sb2 *testing.B) { BenchmarkUpdate(sb2, st, 512) })
	})
}

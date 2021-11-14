package testsuite

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/plexsysio/gkvstore"
	"github.com/plexsysio/gkvstore/autoencoding"
)

type testBStruct struct {
	Key     string `aenc:"id"`
	Val     []byte
	Size    int64
	Created int64
	Updated int64
}

func newStruct(size int) *testBStruct {
	buf := make([]byte, size)
	return &testBStruct{Val: buf, Size: int64(size)}
}

func (t *testBStruct) fillRandom() {
	_, _ = rand.Read(t.Val)
}

func (t *testBStruct) edit(buf []byte) {
	idx := rand.Intn(len(t.Val) - len(buf) - 1)
	copy(t.Val[idx:idx+len(buf)], buf)
}

func (t *testBStruct) setKey(key string) {
	t.Key = key
}

func (t *testBStruct) getKey() string {
	return t.Key
}

type testCStruct struct {
	Key  string `json:"key" aenc:"id"`
	Val  []byte `json:"val"`
	Size int64  `json:"size"`
}

func newStructC(size int) *testCStruct {
	buf := make([]byte, size)
	_, _ = rand.Read(buf)
	return &testCStruct{Val: buf, Size: int64(size)}
}

func (t *testCStruct) fillRandom() {
	_, _ = rand.Read(t.Val)
}

func (t *testCStruct) edit(buf []byte) {
	idx := rand.Intn(len(t.Val) - len(buf) - 1)
	copy(t.Val[idx:idx+len(buf)], buf)
}

func (t *testCStruct) setKey(key string) {
	t.Key = key
}

func (t *testCStruct) getKey() string {
	return t.Key
}

type testStructHelper interface {
	fillRandom()
	edit([]byte)
	setKey(string)
	getKey() string
}

func BenchmarkCreate(sb *testing.B, st gkvstore.Store, newStruct func() interface{}) {
	sb.ReportAllocs()
	sb.ResetTimer()

	for n := 0; n < sb.N; n++ {
		it := newStruct()
		it.(testStructHelper).fillRandom()
		err := st.Create(context.TODO(), autoencoding.MustNew(it))
		if err != nil {
			sb.Fatal(err)
		}
	}
}

func BenchmarkUpdate(sb *testing.B, st gkvstore.Store, newStruct func() interface{}) {

	var items []interface{}
	for n := 0; n < sb.N; n++ {
		it := newStruct()
		it.(testStructHelper).fillRandom()
		err := st.Create(context.TODO(), autoencoding.MustNew(it))
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
		err := st.Update(context.TODO(), autoencoding.MustNew(v))
		if err != nil {
			sb.Fatal(err)
		}
	}
}

func BenchmarkRead(sb *testing.B, st gkvstore.Store, newStruct func() interface{}) {

	ids := []string{}
	for n := 0; n < sb.N; n++ {
		it := newStruct()
		it.(testStructHelper).fillRandom()
		err := st.Create(context.TODO(), autoencoding.MustNew(it))
		if err != nil {
			sb.Fatal(err)
		}
		ids = append(ids, it.(testStructHelper).getKey())
	}

	sb.ReportAllocs()
	sb.ResetTimer()

	for n := 0; n < sb.N; n++ {
		it := newStruct()
		it.(testStructHelper).setKey(ids[0])
		ids = ids[1:]

		err := st.Read(context.TODO(), autoencoding.MustNew(it))
		if err != nil {
			sb.Fatal(err, "key", fmt.Sprintf("%d", n+1))
		}
	}
}

func BenchmarkSuite(b *testing.B, st gkvstore.Store) {
	ctorA := func(size int) func() interface{} {
		return func() interface{} {
			return newStruct(size)
		}
	}
	ctorB := func(size int) func() interface{} {
		return func() interface{} {
			return newStructC(size)
		}
	}
	b.Run("With time tracker", func(sb1 *testing.B) {
		sb1.Run("64B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, ctorA(64)) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, ctorA(64)) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, ctorA(64)) })
		})
		sb1.Run("128B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, ctorA(128)) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, ctorA(128)) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, ctorA(128)) })
		})
		sb1.Run("256B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, ctorA(256)) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, ctorA(256)) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, ctorA(256)) })
		})
		sb1.Run("512B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, ctorA(512)) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, ctorA(512)) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, ctorA(512)) })
		})
	})
	b.Run("Without time tracker", func(sb1 *testing.B) {
		sb1.Run("64B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, ctorB(64)) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, ctorB(64)) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, ctorB(64)) })
		})
		sb1.Run("128B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, ctorB(128)) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, ctorB(128)) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, ctorB(128)) })
		})
		sb1.Run("256B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, ctorB(256)) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, ctorB(256)) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, ctorB(256)) })
		})
		sb1.Run("512B", func(sb2 *testing.B) {
			sb2.Run("Create", func(sb3 *testing.B) { BenchmarkCreate(sb3, st, ctorB(512)) })
			sb2.Run("Read", func(sb3 *testing.B) { BenchmarkRead(sb3, st, ctorB(512)) })
			sb2.Run("Update", func(sb3 *testing.B) { BenchmarkUpdate(sb3, st, ctorB(512)) })
		})
	})
}

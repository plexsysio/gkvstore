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

type testDStruct struct {
	Key  string `msgpack:"key" aenc:"id"`
	Val  []byte `msgpack:"val"`
	Size int64  `msgpack:"size"`
}

func newStructD(size int) *testDStruct {
	buf := make([]byte, size)
	_, _ = rand.Read(buf)
	return &testDStruct{Val: buf, Size: int64(size)}
}

func (t *testDStruct) fillRandom() {
	_, _ = rand.Read(t.Val)
}

func (t *testDStruct) edit(buf []byte) {
	idx := rand.Intn(len(t.Val) - len(buf) - 1)
	copy(t.Val[idx:idx+len(buf)], buf)
}

func (t *testDStruct) setKey(key string) {
	t.Key = key
}

func (t *testDStruct) getKey() string {
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
	for _, tc := range []struct {
		Name string
		Ctor func() interface{}
	}{
		{
			Name: "JSON 64B WithTimeTracker",
			Ctor: func() interface{} { return newStruct(64) },
		},
		{
			Name: "JSON 128B WithTimeTracker",
			Ctor: func() interface{} { return newStruct(128) },
		},
		{
			Name: "JSON 256B WithTimeTracker",
			Ctor: func() interface{} { return newStruct(256) },
		},
		{
			Name: "JSON 512B WithTimeTracker",
			Ctor: func() interface{} { return newStruct(512) },
		},
		{
			Name: "JSON 64B WithoutTimeTracker",
			Ctor: func() interface{} { return newStructC(64) },
		},
		{
			Name: "JSON 128B WithoutTimeTracker",
			Ctor: func() interface{} { return newStructC(128) },
		},
		{
			Name: "JSON 256B WithoutTimeTracker",
			Ctor: func() interface{} { return newStructC(256) },
		},
		{
			Name: "JSON 512B WithoutTimeTracker",
			Ctor: func() interface{} { return newStructC(512) },
		},
		{
			Name: "Msgpack 64B WithoutTimeTracker",
			Ctor: func() interface{} { return newStructD(64) },
		},
		{
			Name: "Msgpack 128B WithoutTimeTracker",
			Ctor: func() interface{} { return newStructD(128) },
		},
		{
			Name: "Msgpack 256B WithoutTimeTracker",
			Ctor: func() interface{} { return newStructD(256) },
		},
		{
			Name: "Msgpack 512B WithoutTimeTracker",
			Ctor: func() interface{} { return newStructD(512) },
		},
	} {
		b.Run(tc.Name, func(sb1 *testing.B) {
			sb1.Run("Create", func(sb2 *testing.B) { BenchmarkCreate(sb2, st, tc.Ctor) })
			sb1.Run("Read", func(sb2 *testing.B) { BenchmarkRead(sb2, st, tc.Ctor) })
			sb1.Run("Update", func(sb2 *testing.B) { BenchmarkUpdate(sb2, st, tc.Ctor) })
		})
	}
}

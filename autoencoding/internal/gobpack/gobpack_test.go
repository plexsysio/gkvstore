package gobpack_test

import (
	"bytes"
	"crypto/rand"
	"encoding/gob"
	"testing"

	"github.com/plexsysio/gkvstore/autoencoding/internal/gobpack"
)

func TestGobpack(t *testing.T) {
	type tItem struct {
		Name string
		ID   string
		Val1 int
		Val2 string
	}

	for _, v := range []*tItem{
		{
			Name: "t1",
			ID:   "1",
			Val1: 1,
			Val2: "first",
		},
		{
			Name: "t2",
			ID:   "2",
			Val1: 2,
			Val2: "second",
		},
		{
			Name: "t3",
			ID:   "3",
			Val1: 3,
			Val2: "third",
		},
		{
			Name: "t4",
			ID:   "4",
			Val1: 4,
			Val2: "fourth",
		},
	} {

		t.Run("create "+v.Name, func(t *testing.T) {
			buf, err := gobpack.Marshal(v)
			if err != nil {
				t.Fatalf("failed marshaling %v", err)
			}

			v2 := &tItem{}
			err = gobpack.Unmarshal(buf, v2)
			if err != nil {
				t.Fatalf("failed unmarshalling %v", err)
			}

			if v2.Name != v.Name || v2.ID != v.ID || v2.Val1 != v.Val1 || v2.Val2 != v.Val2 {
				t.Fatalf("read invalid struct expected %v found %v", v, v2)
			}
		})
	}
}

type bItem struct {
	Val []byte
}

func BenchmarkGobpack(b *testing.B) {

	generateStruct := func(size int) (*bItem, error) {
		it := &bItem{Val: make([]byte, size)}
		_, err := rand.Read(it.Val)
		if err != nil {
			return nil, err
		}
		return it, nil
	}

	b.Run("gobpack marshal", func(sb *testing.B) {
		sb.Run("64B", func(sb2 *testing.B) {
			sb2.ResetTimer()
			sb2.ReportAllocs()

			for n := 0; n < sb2.N; n++ {
				ti, err := generateStruct(64)
				if err != nil {
					sb2.Fatalf("failed generating struct %v", err)
				}
				_, err = gobpack.Marshal(ti)
				if err != nil {
					sb2.Fatalf("failed marshaling %v", err)
				}
			}
		})
		sb.Run("128B", func(sb2 *testing.B) {
			sb2.ReportAllocs()
			sb2.ResetTimer()

			for n := 0; n < sb2.N; n++ {
				ti, err := generateStruct(128)
				if err != nil {
					sb2.Fatalf("failed generating struct %v", err)
				}
				_, err = gobpack.Marshal(ti)
				if err != nil {
					sb2.Fatalf("failed marshaling %v", err)
				}
			}
		})
	})
	// Marshal by creating encoder everytime
	b.Run("unoptimized marshal", func(sb *testing.B) {
		sb.Run("64B", func(sb2 *testing.B) {
			sb2.ReportAllocs()
			sb2.ResetTimer()

			for n := 0; n < sb2.N; n++ {
				ti, err := generateStruct(64)
				if err != nil {
					sb2.Fatalf("failed generating struct %v", err)
				}
				var buf bytes.Buffer
				err = gob.NewEncoder(&buf).Encode(ti)
				if err != nil {
					sb2.Fatalf("failed marshaling %v", err)
				}
				buf.Reset()
			}
		})
		sb.Run("128B", func(sb2 *testing.B) {
			sb2.ReportAllocs()
			sb2.ResetTimer()

			for n := 0; n < sb2.N; n++ {
				ti, err := generateStruct(64)
				if err != nil {
					sb2.Fatalf("failed generating struct %v", err)
				}
				var buf bytes.Buffer
				err = gob.NewEncoder(&buf).Encode(ti)
				if err != nil {
					sb2.Fatalf("failed marshaling %v", err)
				}
				buf.Reset()
			}
		})
	})
}

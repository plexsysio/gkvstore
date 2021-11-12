package autoencoding_test

import (
	"testing"

	"github.com/plexsysio/gkvstore/autoencoding"
	"github.com/plexsysio/gkvstore/autoencoding/internal/pbtest"
)

type jsonItem struct {
	Name string
	ID   string
	Val1 int
	Val2 string
}

func (t *jsonItem) GetNamespace() string { return t.Name }

func (t *jsonItem) GetID() string { return t.ID }

func (t *jsonItem) Encoding() autoencoding.Encoding { return autoencoding.JSON }

func TestJSONEncoding(t *testing.T) {
	t1 := &jsonItem{
		Name: "test1",
		ID:   "1",
		Val1: 100,
		Val2: "test1 JSON",
	}
	ae1 := autoencoding.New(t1)

	buf, err := ae1.Marshal()
	if err != nil {
		t.Fatalf("failed marshalling %v", err)
	}

	t.Log("Size of packing", len(buf))

	t2 := &jsonItem{}
	ae2 := autoencoding.New(t2)

	err = ae2.Unmarshal(buf)
	if err != nil {
		t.Fatalf("failed unmarshaling %v", err)
	}

	if t1.Name != t2.Name || t1.ID != t2.ID || t1.Val1 != t2.Val1 || t1.Val2 != t2.Val2 {
		t.Fatalf("read incorrect value expected %v found %v", t1, t2)
	}
}

type msgpackItem struct {
	Name string
	ID   string
	Val1 int
	Val2 string
}

func (t *msgpackItem) GetNamespace() string { return t.Name }

func (t *msgpackItem) GetID() string { return t.ID }

func (t *msgpackItem) Encoding() autoencoding.Encoding { return autoencoding.MsgPack }

func TestMsgpackEncoding(t *testing.T) {
	t1 := &msgpackItem{
		Name: "test1",
		ID:   "1",
		Val1: 100,
		Val2: "test1 JSON",
	}
	ae1 := autoencoding.New(t1)

	buf, err := ae1.Marshal()
	if err != nil {
		t.Fatalf("failed marshalling %v", err)
	}

	t.Log("Size of packing", len(buf))

	t2 := &msgpackItem{}
	ae2 := autoencoding.New(t2)

	err = ae2.Unmarshal(buf)
	if err != nil {
		t.Fatalf("failed unmarshaling %v", err)
	}

	if t1.Name != t2.Name || t1.ID != t2.ID || t1.Val1 != t2.Val1 || t1.Val2 != t2.Val2 {
		t.Fatalf("read incorrect value expected %v found %v", t1, t2)
	}
}

type gobpackItem struct {
	Name string
	ID   string
	Val1 int
	Val2 string
}

func (t *gobpackItem) GetNamespace() string { return t.Name }

func (t *gobpackItem) GetID() string { return t.ID }

func (t *gobpackItem) Encoding() autoencoding.Encoding { return autoencoding.Gob }

func TestGobpackEncoding(t *testing.T) {
	t1 := &gobpackItem{
		Name: "test1",
		ID:   "1",
		Val1: 100,
		Val2: "test1 JSON",
	}
	ae1 := autoencoding.New(t1)

	buf, err := ae1.Marshal()
	if err != nil {
		t.Fatalf("failed marshalling %v", err)
	}

	t.Log("Size of packing", len(buf))

	t2 := &gobpackItem{}
	ae2 := autoencoding.New(t2)

	err = ae2.Unmarshal(buf)
	if err != nil {
		t.Fatalf("failed unmarshaling %v", err)
	}

	if t1.Name != t2.Name || t1.ID != t2.ID || t1.Val1 != t2.Val1 || t1.Val2 != t2.Val2 {
		t.Fatalf("read incorrect value expected %v found %v", t1, t2)
	}
}

type pbItem struct {
	*pbtest.TestItem
}

func (t *pbItem) GetNamespace() string { return t.Name }

func (t *pbItem) GetID() string { return t.Id }

func (t *pbItem) Encoding() autoencoding.Encoding { return autoencoding.Protobuf }

func TestProtobufEncoding(t *testing.T) {
	t1 := &pbItem{
		&pbtest.TestItem{
			Name: "test1",
			Id:   "1",
			Val1: 100,
			Val2: "test1 JSON",
		},
	}
	ae1 := autoencoding.New(t1)

	buf, err := ae1.Marshal()
	if err != nil {
		t.Fatalf("failed marshalling %v", err)
	}

	t.Log("Size of packing", len(buf))

	t2 := &pbItem{&pbtest.TestItem{}}
	ae2 := autoencoding.New(t2)

	err = ae2.Unmarshal(buf)
	if err != nil {
		t.Fatalf("failed unmarshaling %v", err)
	}

	if t1.Name != t2.Name || t1.Id != t2.Id || t1.Val1 != t2.Val1 || t1.Val2 != t2.Val2 {
		t.Fatalf("read incorrect value expected %v found %v", t1, t2)
	}
}

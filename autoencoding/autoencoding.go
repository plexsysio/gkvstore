package autoencoding

import (
	"encoding/json"
	"errors"

	"github.com/plexsysio/gkvstore"
	"github.com/plexsysio/gkvstore/autoencoding/internal/gobpack"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

type Encoding int

const (
	JSON Encoding = iota
	Gob
	MsgPack
	Protobuf
)

type Item interface {
	Encoding() Encoding
	GetNamespace() string
	GetID() string
}

type item struct {
	Item
}

func New(val Item) gkvstore.Item {
	return &item{Item: val}
}

func (i *item) Marshal() ([]byte, error) {
	switch i.Encoding() {
	case JSON:
		return json.Marshal(i.Item)
	case Gob:
		return gobpack.Marshal(i.Item)
	case MsgPack:
		return msgpack.Marshal(i.Item)
	case Protobuf:
		return proto.Marshal(i.Item.(proto.Message))
	}
	return nil, errors.New("invalid encoding")
}

func (i *item) Unmarshal(buf []byte) error {
	switch i.Encoding() {
	case JSON:
		return json.Unmarshal(buf, i.Item)
	case Gob:
		return gobpack.Unmarshal(buf, i.Item)
	case MsgPack:
		return msgpack.Unmarshal(buf, i.Item)
	case Protobuf:
		return proto.Unmarshal(buf, i.Item.(proto.Message))
	}
	return errors.New("invalid encoding")
}

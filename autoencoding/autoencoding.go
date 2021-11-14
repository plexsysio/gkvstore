package autoencoding

import (
	"encoding/json"
	"errors"
	"reflect"
	"unicode"

	"github.com/plexsysio/gkvstore"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

type Encoding int

const (
	Invalid Encoding = iota
	JSON
	MsgPack
	Protobuf
)

type item struct {
	val       interface{}
	namespace string
	id        string
	encoding  Encoding
}

type itemWithTimeTracker struct {
	*item
	created string
	updated string
}

func getEncoding(f reflect.StructField) Encoding {
	if _, ok := f.Tag.Lookup("protobuf"); ok {
		return Protobuf
	}
	if _, ok := f.Tag.Lookup("json"); ok {
		return JSON
	}
	if _, ok := f.Tag.Lookup("msgpack"); ok {
		return MsgPack
	}
	// If none is provided use JSON as default
	return JSON
}

func newItem(val interface{}) (gkvstore.Item, error) {
	foundId, foundCreated, foundUpdated := false, false, false
	id, ns, created, updated := "", "", "", ""

	rv := reflect.ValueOf(val)
	if rv.Kind() != reflect.Ptr && rv.Kind() != reflect.Interface {
		return nil, errors.New("incorrect value type: use pointer")
	}
	t := rv.Elem().Type()

	if t.Name() == "" {
		return nil, errors.New("incorrect name of type")
	}
	ns = t.Name()

	var encoding Encoding
	// Use first exported field for encoding
	for i := 0; i < t.NumField(); i++ {
		if unicode.IsUpper(rune(t.Field(i).Name[0])) {
			encoding = getEncoding(t.Field(i))
			break
		}
	}
	if encoding == 0 {
		return nil, errors.New("no exported field")
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tagval, ok := field.Tag.Lookup("aenc")
		if (field.Name == "ID" || field.Name == "Id") || (ok && tagval == "id") {
			if foundId {
				return nil, errors.New("duplicate ID field configured")
			}
			if field.Type.Kind() != reflect.String {
				return nil, errors.New("ID field should be string")
			}
			foundId = true
			id = field.Name
		}
		if (field.Name == "Created" || field.Name == "CreatedAt") || (ok && tagval == "created") {
			if foundCreated {
				return nil, errors.New("duplicate Created field configured")
			}
			if field.Type.Kind() != reflect.Int64 {
				return nil, errors.New("Created field should be uint64")
			}
			foundCreated = true
			created = field.Name
		}
		if (field.Name == "Updated" || field.Name == "UpdatedAt") || (ok && tagval == "updated") {
			if foundUpdated {
				return nil, errors.New("duplicate Updated field configured")
			}
			if field.Type.Kind() != reflect.Int64 {
				return nil, errors.New("Updated field should be uint64")
			}
			foundUpdated = true
			updated = field.Name
		}
	}

	if !foundId {
		return nil, errors.New("ID field not configured")
	}

	if foundCreated && foundUpdated {
		return &itemWithTimeTracker{
			item: &item{
				namespace: ns,
				id:        id,
				encoding:  encoding,
				val:       val,
			},
			created: created,
			updated: updated,
		}, nil
	}

	return &item{
		namespace: ns,
		id:        id,
		encoding:  encoding,
		val:       val,
	}, nil
}

func New(val interface{}) (gkvstore.Item, error) {
	return newItem(val)
}

func MustNew(val interface{}) gkvstore.Item {
	it, err := newItem(val)
	if err != nil {
		panic(err.Error())
	}
	return it
}

func (i *item) GetNamespace() string {
	return i.namespace
}

func (i *item) GetID() string {
	v := reflect.ValueOf(i.val).Elem()
	return v.FieldByName(i.id).String()
}

func (i *item) SetID(id string) {
	v := reflect.ValueOf(i.val).Elem()
	v.FieldByName(i.id).SetString(id)
}

func (i *item) Marshal() ([]byte, error) {
	switch i.encoding {
	case JSON:
		return json.Marshal(i.val)
	case MsgPack:
		return msgpack.Marshal(i.val)
	case Protobuf:
		return proto.Marshal(i.val.(proto.Message))
	}
	return nil, errors.New("invalid encoding")
}

func (i *item) Unmarshal(buf []byte) error {
	switch i.encoding {
	case JSON:
		return json.Unmarshal(buf, i.val)
	case MsgPack:
		return msgpack.Unmarshal(buf, i.val)
	case Protobuf:
		return proto.Unmarshal(buf, i.val.(proto.Message))
	}
	return errors.New("invalid encoding")
}

func (i *itemWithTimeTracker) GetCreated() int64 {
	v := reflect.ValueOf(i.val).Elem()
	return v.FieldByName(i.created).Int()
}

func (i *itemWithTimeTracker) GetUpdated() int64 {
	v := reflect.ValueOf(i.val).Elem()
	return v.FieldByName(i.updated).Int()
}

func (i *itemWithTimeTracker) SetCreated(val int64) {
	v := reflect.ValueOf(i.val).Elem()
	v.FieldByName(i.created).SetInt(val)
}

func (i *itemWithTimeTracker) SetUpdated(val int64) {
	v := reflect.ValueOf(i.val).Elem()
	v.FieldByName(i.updated).SetInt(val)
}

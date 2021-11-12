package autoencoding

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"unicode"

	"github.com/plexsysio/gkvstore"
	"github.com/plexsysio/gkvstore/autoencoding/internal/gobpack"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

type Encoding int

const (
	Invalid Encoding = iota
	JSON
	Gob
	MsgPack
	Protobuf
)

type item struct {
	val       interface{}
	id        string
	namespace string
	encoding  Encoding
}

type itemWithTimeTracker struct {
	*item
	created string
	updated string
}

func getEncoding(f reflect.StructField) Encoding {
	if _, ok := f.Tag.Lookup("protobuf"); ok {
		fmt.Println("Using protobuf")
		return Protobuf
	}
	if _, ok := f.Tag.Lookup("json"); ok {
		fmt.Println("Using JSON")
		return JSON
	}
	if _, ok := f.Tag.Lookup("msgpack"); ok {
		fmt.Println("Using MsgPack")
		return MsgPack
	}
	fmt.Println("Using Gob")
	return Gob
}

func newItem(val interface{}) (gkvstore.Item, error) {
	foundId, foundNs, foundCreated, foundUpdated := false, false, false, false
	id, ns, created, updated := "", "", "", ""

	rv := reflect.ValueOf(val)
	if rv.Kind() != reflect.Ptr && rv.Kind() != reflect.Interface {
		return nil, errors.New("incorrect value type: use pointer")
	}
	t := rv.Elem().Type()

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
		if field.Name == "ID" || field.Name == "Id" {
			if foundId {
				return nil, errors.New("duplicate ID field configured")
			}
			foundId = true
			id = field.Name
		}
		if field.Name == "Namespace" {
			if foundNs {
				return nil, errors.New("duplicate Namespace field configured")
			}
			foundNs = true
			ns = field.Name
		}
		if field.Name == "Created" || field.Name == "CreatedAt" {
			if foundCreated {
				return nil, errors.New("duplicate Created field configured")
			}
			foundCreated = true
			created = field.Name
		}
		if field.Name == "Updated" || field.Name == "UpdatedAt" {
			if foundUpdated {
				return nil, errors.New("duplicate Updated field configured")
			}
			foundUpdated = true
			updated = field.Name
		}
		if val, ok := field.Tag.Lookup("aenc"); ok {
			if val == "id" {
				if foundId && id != field.Name {
					return nil, errors.New("duplicate ID field configured")
				}
				foundId = true
				id = field.Name
			}
			if val == "namespace" && !foundNs {
				if foundNs && ns != field.Name {
					return nil, errors.New("duplicate Namespace field configured")
				}
				foundNs = true
				ns = field.Name
			}
			if val == "created" {
				if foundCreated && created != field.Name {
					return nil, errors.New("duplicate Created field configured")
				}
				foundCreated = true
				created = field.Name
			}
			if val == "updated" {
				if foundUpdated && updated != field.Name {
					return nil, errors.New("duplicate Updated field configured")
				}
				foundUpdated = true
				updated = field.Name
			}
		}
	}

	if !foundId || !foundNs {
		return nil, errors.New("ID and Namespace not configured")
	}

	if foundCreated && foundUpdated {
		return &itemWithTimeTracker{
			item: &item{
				id:        id,
				namespace: ns,
				encoding:  encoding,
				val:       val,
			},
			created: created,
			updated: updated,
		}, nil
	}

	return &item{
		id:        id,
		namespace: ns,
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
	v := reflect.ValueOf(i.val).Elem()
	return v.FieldByName(i.namespace).String()
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
	case Gob:
		return gobpack.Marshal(i.val)
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
	case Gob:
		return gobpack.Unmarshal(buf, i.val)
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

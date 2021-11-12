package gobpack

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync"
)

var encPool = sync.Pool{
	New: func() interface{} {
		return newEncoder()
	},
}

var encSem = make(chan struct{}, 100)

type encoder struct {
	*gob.Encoder
	buf *bytes.Buffer
}

func newEncoder() *encoder {
	buf := new(bytes.Buffer)
	return &encoder{
		Encoder: gob.NewEncoder(buf),
		buf:     buf,
	}
}

func getEncoder() *encoder {
	encSem <- struct{}{}
	return encPool.Get().(*encoder)
}

func putEncoder(enc *encoder) {
	<-encSem
	enc.buf.Reset()
	encPool.Put(enc)
}

var decPool = sync.Pool{
	New: func() interface{} {
		return newDecoder()
	},
}

var decSem = make(chan struct{}, 100)

type decoder struct {
	*gob.Decoder
	buf *bytes.Buffer
}

func newDecoder() *decoder {
	buf := new(bytes.Buffer)
	return &decoder{
		Decoder: gob.NewDecoder(buf),
		buf:     buf,
	}
}

func getDecoder(buf []byte) (*decoder, error) {
	dec := decPool.Get().(*decoder)
	n, err := dec.buf.Write(buf)
	if err != nil {
		return nil, err
	}
	if n != len(buf) {
		return nil, errors.New("incorrect write")
	}
	return dec, nil
}

func putDecoder(dec *decoder) {
	dec.buf.Reset()
	decPool.Put(dec)
}

func Marshal(v interface{}) ([]byte, error) {
	enc := getEncoder()

	err := enc.Encode(v)
	b := enc.buf.Bytes()

	putEncoder(enc)

	if err != nil {
		return nil, err
	}

	return b, nil
}

func Unmarshal(buf []byte, v interface{}) error {
	dec, err := getDecoder(buf)
	if err != nil {
		return err
	}

	err = dec.Decode(v)
	putDecoder(dec)

	if err != nil {
		return err
	}

	return nil
}

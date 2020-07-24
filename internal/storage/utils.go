package storage

import (
	"bytes"
	"fmt"
	"io"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

func getEncoder(w io.Writer) *msgpack.Encoder {
	enc := msgpack.NewEncoder(w)
	enc.UseJSONTag(true)

	return enc
}

func encode(object interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := getEncoder(buf).Encode(object)
	if err != nil {
		return nil, fmt.Errorf("%w: could not encode object", err)
	}

	return buf.Bytes(), nil
}

func getDecoder(r io.Reader) *msgpack.Decoder {
	dec := msgpack.NewDecoder(r)
	dec.UseJSONTag(true)

	return dec
}

func decode(b []byte, object interface{}) error {
	err := getDecoder(bytes.NewReader(b)).Decode(&object)
	if err != nil {
		return fmt.Errorf("%w: unable to decode bytes", err)
	}

	return nil
}

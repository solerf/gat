package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/hamba/avro/v2/ocf"
)

func ReadSchema(avroPath string) ([]byte, error) {
	b, err := readFile(avroPath)
	if err != nil {
		return nil, err
	}

	decoder, err := decode(b)
	if err != nil {
		return nil, err
	}
	return indentJson(decoder.Metadata()["avro.schema"])
}

func ReadJson(avroPath string) ([]byte, error) {
	b, err := readFile(avroPath)
	if err != nil {
		return nil, err
	}

	decoder, err := decode(b)
	if err != nil {
		return nil, err
	}

	jsonB, err := parse(decoder)
	if err != nil {
		return nil, err
	}
	return indentJson(jsonB)
}

func indentJson(data []byte) ([]byte, error) {
	buff := new(bytes.Buffer)
	err := json.Indent(buff, data, "", "  ")
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func decode(avroReader io.Reader) (*ocf.Decoder, error) {
	// it reads with the schema from the metadata
	dec, err := ocf.NewDecoder(avroReader)
	if err != nil {
		return nil, fmt.Errorf("failed decoding avro: %w", err)
	}
	return dec, nil
}

func parse(d *ocf.Decoder) ([]byte, error) {
	avroObjs := make([]map[string]interface{}, 0)
	for d.HasNext() {
		piece := make(map[string]interface{})
		err := d.Decode(&piece)
		if err != nil {
			return nil, fmt.Errorf("failed parsing avro: %w", err)
		}
		avroObjs = append(avroObjs, piece)
	}

	if len(avroObjs) == 0 {
		return nil, errors.New("avro is empty")
	}

	marshal, err := json.Marshal(avroObjs)
	if err != nil {
		return nil, fmt.Errorf("failed marshalling: %w", err)
	}

	return marshal, nil
}

func readFile(path string) (io.Reader, error) {
	b, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", path, err)
	}
	return b, nil
}

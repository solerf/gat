package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hamba/avro/v2/ocf"
)

func ReadSchema(avro []byte) ([]byte, error) {
	decoder, err := decode(avro)
	if err != nil {
		return nil, err
	}

	schemaBytes := decoder.Metadata()["avro.schema"]

	var buff bytes.Buffer
	err = json.Indent(&buff, schemaBytes, "", "  ")

	if err != nil {
		return nil, fmt.Errorf("could not parse schema to json: %v", err)
	}
	return buff.Bytes(), err
}

func ReadJson(avro []byte) ([]byte, error) {
	decoded, err := decode(avro)
	if err != nil {
		return nil, err
	}

	parsed, errParse := parse(decoded)
	if errParse != nil {
		return nil, errParse
	}

	b, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("could not parse content to json: %v", err)
	}
	return b, nil
}

func decode(avro []byte) (dec *ocf.Decoder, err error) {
	// it reads with the schema from the metadata
	dec, err = ocf.NewDecoder(bytes.NewReader(avro))
	if err != nil {
		return dec, fmt.Errorf("could not decode avro: %v", err)
	}
	return dec, err
}

func parse(d *ocf.Decoder) ([]map[string]interface{}, error) {
	avroObjs := make([]map[string]interface{}, 0, 10)
	for d.HasNext() {
		piece := make(map[string]interface{}, 20)
		err := d.Decode(&piece)
		if err != nil {
			return nil, fmt.Errorf("could not parse avro: %v", err)
		}
		avroObjs = append(avroObjs, piece)
	}

	if len(avroObjs) == 0 {
		return nil, fmt.Errorf("avro is empty")
	}
	return avroObjs, nil
}

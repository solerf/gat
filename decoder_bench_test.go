package main

import (
	"bytes"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hamba/avro/v2"
)

func Benchmark_ReadJson(b *testing.B) {
	payload := genAvro().Bytes()
	for b.Loop() {
		_, _ = process(bytes.NewReader(payload))
	}
}

var r = rand.New(rand.New(rand.NewPCG(10, 20)))

func genAvro() *bytes.Buffer {
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

	randomString := func(size int) string {
		alphabetSize := len(chars)
		var sb strings.Builder
		for range size {
			ch := chars[r.IntN(alphabetSize)]
			sb.WriteRune(ch)
		}
		return sb.String()
	}

	s := simple{
		A: r.Int64(),
		B: randomString(r.IntN(100)),
		C: nested{
			D: randomString(r.IntN(100)),
		},
	}

	avroSchema, _ := avro.Parse(rawSchema)
	encoded, _ := avro.Marshal(avroSchema, s)
	return bytes.NewBuffer(encoded)
}

var rawSchema = `{
    "type": "record",
    "name": "simple",
    "namespace": "test.gate",
    "fields": [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"},
        {"name": "c", "type": {
                "type": "record",
				"name": "nested",
				"fields": [
					{"name": "d", "type": "string"}
				]
            }
        }
    ]
}`

type simple struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
	C nested `avro:"c"`
}

type nested struct {
	D string `avro:"d"`
}

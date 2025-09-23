package main

import (
	"testing"
)

func Test_ReadBundleOfAvros(t *testing.T) {
	json, err := ReadJson("testdata/bundles.avro")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(json) == 0 {
		t.Fatal("expected non empty json")
	}
}

func Test_ReadAvro(t *testing.T) {
	json, err := ReadJson("testdata/single.avro")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(json) == 0 {
		t.Fatal("expected non empty json")
	}
}

func Test_ReadSchema(t *testing.T) {
	json, err := ReadSchema("testdata/single.avro")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(json) == 0 {
		t.Fatal("expected non empty json schema")
	}
}

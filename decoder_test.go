package main

import (
	"os"
	"testing"
)

func Test_ReadBundleOfAvros(t *testing.T) {
	file, _ := os.ReadFile("testdata/bundles.avro")
	json, err := ReadJson(file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(json) == 0 {
		t.Fatal("expected non empty json")
	}
}

func Test_ReadAvro(t *testing.T) {
	file, _ := os.ReadFile("testdata/single.avro")

	json, err := ReadJson(file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(json) == 0 {
		t.Fatal("expected non empty json")
	}
}

func Test_ReadSchema(t *testing.T) {
	file, _ := os.ReadFile("testdata/single.avro")
	json, err := ReadSchema(file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(json) == 0 {
		t.Fatal("expected non empty json schema")
	}
}

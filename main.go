package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/hamba/avro/v2/ocf"
	"os"
)

type CommandType struct{}

type CliArgs struct {
	ToJson    *CommandType `arg:"subcommand:tojson, required" help:"Extracts JSON from provided AVRO"`
	GetSchema *CommandType `arg:"subcommand:getschema, required" help:"Extracts Schema from provided AVRO"`
	Avro      string       `arg:"-a, required" help:"absolute path to avro file"`
}

var cArgs CliArgs

func main() {
	input, err := getInputArgs()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	decoder, err := decode(input)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var result string
	switch {
	case cArgs.GetSchema != nil:
		result, err = getSchema(decoder)
	case cArgs.ToJson != nil:
		result, err = getJson(decoder)
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(result)
}

func getInputArgs() (input []byte, err error) {
	input, err = fromPipe()
	if len(input) > 0 {
		return input, err
	}
	return fromArgs()
}

func fromPipe() (input []byte, err error) {
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		cmd := os.Args[len(os.Args)-1]
		if cmd == "tojson" {
			cArgs.ToJson = &CommandType{}
		} else {
			cArgs.GetSchema = &CommandType{}
		}

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			input = append(input, scanner.Bytes()...)
		}
		if err = scanner.Err(); err != nil {
			return input, fmt.Errorf("not able to read from stdin: %v", err)
		}
		return input, err
	}
	return input, err
}

func fromArgs() (input []byte, err error) {
	arg.MustParse(&cArgs)
	avroPath := cArgs.Avro
	if _, err = os.Stat(avroPath); errors.Is(err, os.ErrNotExist) {
		return input, fmt.Errorf("avro file not found: %v", avroPath)
	}
	input, err = os.ReadFile(avroPath)
	if err != nil {
		return input, fmt.Errorf("could not read avro: %v", err)
	}
	return input, err
}

func getSchema(decoder *ocf.Decoder) (out string, err error) {
	schemaBytes := decoder.Metadata()["avro.schema"]
	var buff bytes.Buffer
	err = json.Indent(&buff, schemaBytes, "", "  ")
	if err != nil {
		return out, fmt.Errorf("could not parse schema to json: %v", err)
	}
	return buff.String(), err
}

func getJson(decoder *ocf.Decoder) (out string, err error) {
	var avroMap []*map[string]interface{}
	avroMap, err = parse(decoder)
	if err != nil {
		return out, err
	}

	b, err := json.MarshalIndent(avroMap, "", "  ")
	if err != nil {
		return out, fmt.Errorf("could not parse to content to json: %v", err)
	}
	return string(b), nil
}

func decode(avro []byte) (dec *ocf.Decoder, err error) {
	// it reads with the schema from the metadata
	dec, err = ocf.NewDecoder(bytes.NewReader(avro))
	if err != nil {
		return dec, fmt.Errorf("could not decode avro: %v", err)
	}
	return dec, err
}

func parse(d *ocf.Decoder) (avroMap []*map[string]interface{}, err error) {
	var piece map[string]interface{}
	var avros []*map[string]interface{}

	if !d.HasNext() {
		return avroMap, fmt.Errorf("avro is empty")
	}

	for d.HasNext() {
		err = d.Decode(&piece)
		if err != nil {
			return avroMap, fmt.Errorf("could not parse avro: %v", err)
		}
		avros = append(avros, &piece)
	}
	return avros, err
}

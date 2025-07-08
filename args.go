package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/alexflint/go-arg"
	"os"
)

type CommandType struct {
	AvroAbsolutePath string `arg:"positional, required" placeholder:"avro_absolute_path" help:"absolute path to avro file"`
}

type CliArgs struct {
	ToJson    *CommandType `arg:"subcommand:tojson, required" help:"Extracts JSON from provided AVRO"`
	GetSchema *CommandType `arg:"subcommand:getschema, required" help:"Extracts Schema from provided AVRO"`
}

var cArgs CliArgs

func GetInputArgs() (input []byte, err error) {
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
	avroPath := cArgs.ToJson.AvroAbsolutePath
	if len(avroPath) == 0 {
		avroPath = cArgs.GetSchema.AvroAbsolutePath
	}

	if _, err = os.Stat(avroPath); errors.Is(err, os.ErrNotExist) {
		return input, fmt.Errorf("avro file not found: %v", avroPath)
	}
	input, err = os.ReadFile(avroPath)
	if err != nil {
		return input, fmt.Errorf("could not read avro: %v", err)
	}
	return input, err
}

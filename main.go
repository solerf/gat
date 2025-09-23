package main

import (
	"os"

	"github.com/alecthomas/kong"
)

type ToJsonCmd struct{}

func (cmd *ToJsonCmd) Run(avroPath string) error {
	data, err := ReadJson(avroPath)
	if err != nil {
		return err
	}

	_, err = os.Stdout.Write(data)
	if err != nil {
		return err
	}

	return nil
}

type GetSchemaCmd struct{}

func (cmd *GetSchemaCmd) Run(avroPath string) error {
	data, err := ReadSchema(avroPath)
	if err != nil {
		return err
	}

	_, err = os.Stdout.Write(data)
	if err != nil {
		return err
	}

	return nil
}

var cli struct {
	ToJson    ToJsonCmd    `cmd:"" name:"tojson" help:"prints avro to json format"`
	GetSchema GetSchemaCmd `cmd:"" name:"getschema" help:"prints avro schema to json format"`
	AvroPath  string       `required:"" short:"a" name:"avro-path" placeholder:"<PATH>" help:"absolute path to json file"`
}

func main() {
	ctx := kong.Parse(&cli)
	ctx.FatalIfErrorf(ctx.Run(cli.AvroPath))
}

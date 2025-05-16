package main

import (
	"fmt"
	"os"
)

func main() {
	input, err := GetInputArgs()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var result []byte
	switch {
	case cArgs.GetSchema != nil:
		result, err = ReadSchema(input)
	case cArgs.ToJson != nil:
		result, err = ReadJson(input)
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(string(result))
}

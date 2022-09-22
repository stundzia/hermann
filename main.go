package main

import (
	"fmt"

	"github.com/stundzia/hermann/kafka"
)

func main() {
	h, err := kafka.NewHandler()
	if err != nil {
		panic(err)
	}
	fmt.Println("doing")
	h.FindMessageContaining("topic", [][]byte{[]byte("4460861"), []byte("something")}, kafka.ContainTypeAll)
}

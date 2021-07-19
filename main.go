package main

import (
	"github.com/stundzia/hermann/kafka"
)

func main() {
	cons := kafka.NewConsumer()
	cons.PrintMessageForEveryTopic()
}

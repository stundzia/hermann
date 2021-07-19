package main

import (
	"github.com/stundzia/hermann/kafka"
)

func main() {
	cons := kafka.NewConsumer()
	cons.PrintMessageForEveryTopic()
	cons.GetTopicMetadata("customer-traffic", false)
	//cons.CreateTopic("tavo-mamos-bernai", 3, 3)

	//cons.FindMessageContaining([]byte("over1t25"))
}

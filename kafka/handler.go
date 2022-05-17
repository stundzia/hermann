package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/stundzia/hermann/config"
	"log"
	"strings"
	"sync"
)

// Handler handles communication with Kafka.
type Handler struct {
	conn        *kafka.Conn
	genericConn *kafka.Conn
	topic       string
	topics      []string
	address     string
}

// NewHandler - creates and returns Handler pointer.
func NewHandler() *Handler {
	address := config.GetConfig().Kafka.Address
	topic := config.GetConfig().Kafka.Topic
	topics := config.GetConfig().Kafka.Topics

	genConn, _ := kafka.Dial("tcp", address)

	c := &Handler{
		conn:        nil,
		genericConn: genConn,
		topic:       topic,
		topics:      topics,
		address:     address,
	}
	c.conn = c.getTopicConn(c.topic)
	fmt.Println("topics: ", topics)
	fmt.Println("topic: ", topic)
	return c
}

func NewHandlerWithParamsAndReader(address string, topic string) *Handler {
	genConn, _ := kafka.Dial("tcp", address)

	c := &Handler{
		conn:        nil,
		genericConn: genConn,
		topic:       topic,
		address:     address,
	}
	c.conn = c.getTopicConn(c.topic)
	fmt.Println("topic: ", topic)
	return c
}

func GetKafkaReader(topic, groupID string) *kafka.Reader {
	address := config.GetConfig().Kafka.Address
	brokers := strings.Split(address, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func (c *Handler) setConn() {
	c.conn = c.getTopicConn(c.topic)
}

func (c *Handler) getTopicConn(topic string) *kafka.Conn {
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", c.address, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	return conn
}

// GetControllerConn returns controller connection (controller connection is necessary for topic creation).
func (c *Handler) GetControllerConn() *kafka.Conn {
	broker, err := c.conn.Controller()
	if err != nil {
		fmt.Println("Failed to get controller: ", err)
	}
	conn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", broker.Host, broker.Port))
	if err != nil {
		fmt.Printf("Failed to dial controller at %s:%d : %s\n", broker.Host, broker.Port, err.Error())
	}
	return conn
}

// CreateTopic - creates a topic in the configured cluster.
func (c *Handler) CreateTopic(topic string, replicationFactor int, partitionCount int) {

	cc := c.GetControllerConn()
	if cc == nil {
		return
	}
	err := cc.CreateTopics(kafka.TopicConfig{
		Topic:              topic,
		NumPartitions:      partitionCount,
		ReplicationFactor:  replicationFactor,
		ReplicaAssignments: nil,
		ConfigEntries:      nil,
	})
	if err != nil {
		fmt.Printf("Unable to create topic `%s` due to: %s\n", topic, err.Error())
	}
}

// FetchMessage fetches one message from the default consumer topic and returns it's stringified value.
func (c *Handler) FetchMessage() (string, error) {
	msg, err := c.conn.ReadMessage(5000000)
	if err != nil {
		return "", err
	}
	return string(msg.Value), nil
}

// WriteMessage writes a message to provided topic
func (c *Handler) WriteMessage(topic string, value []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Value: value,
	}
	n, err := c.conn.WriteMessages(msg)
	fmt.Println(n, err)
	return err
}

func (c *Handler) printMessageFromTopic(topic string, wg *sync.WaitGroup) {
	conn := c.getTopicConn(topic)
	msg, err := conn.ReadMessage(5000000)
	if err != nil {
		fmt.Println("Message read error: ", err)
	}
	conn.Close()
	// NOTE: does not work for customer-overusage topic as it's messages are not JSON
	valInt := map[string]interface{}{}
	err = json.Unmarshal(msg.Value, &valInt)
	prettyVal, _ := json.MarshalIndent(valInt, "", "\t")
	if len(valInt) != 0 {
		fmt.Printf("------\nTopic: `%s`\n%s\n", topic, prettyVal)
	} else {
		fmt.Printf("------\nTopic: `%s`\n%s\n", topic, string(msg.Value))
	}
	wg.Done()
}

func (c *Handler) PrintMessageForEveryTopic() {
	wg := &sync.WaitGroup{}
	for _, topic := range c.topics {
		wg.Add(1)
		go c.printMessageFromTopic(topic, wg)
	}
	wg.Wait()
}

func (c *Handler) GetTopicMetadata(topic string, partitionDetails bool) *TopicMetadata {
	partitions, err := c.conn.ReadPartitions(topic)
	if err != nil {
		fmt.Println("Partition fetch error: ", err)
		return nil
	}
	var replicationFactor int

	replicationFactor = len(partitions[0].Replicas)

	tm := &TopicMetadata{
		Topic:             topic,
		Partitions:        len(partitions),
		ReplicationFactor: replicationFactor,
	}

	//if partitionDetails {
	//	for _, partition := range partitions {
	//		fmt.Println(partition.Replicas)
	//		fmt.Println(partition.Isr)
	//		fmt.Println(partition.Topic)
	//		fmt.Println(partition.ID)
	//	}
	//}
	//fmt.Printf("Topic %s has %d partitions\n", topic, len(partitions))
	//fmt.Printf("Topic %s has replication factor of %d\n", topic, replicationFactor)
	return tm
}

func printIfContains(msg kafka.Message, containing []byte) {
	if bytes.Contains(msg.Value, containing) {
		valInt := map[string]interface{}{}
		err := json.Unmarshal(msg.Value, &valInt)
		if err != nil {
			fmt.Println("Error: ", err)
			return
		}
		prettyVal, _ := json.MarshalIndent(valInt, "", "\t")
		if len(valInt) != 0 {
			fmt.Printf("------\n%s\n", prettyVal)
		} else {
			fmt.Printf("------\n%s\n", string(msg.Value))
		}
	}
}

func (c *Handler) FindMessageContaining(containing []byte) {
	consumedCount := 0
	for {
		batch := c.conn.ReadBatch(900000, 9000000)
		for {
			msg, err := batch.ReadMessage()
			consumedCount++
			if batch.Err() != nil || err != nil || msg.Value == nil {
				_ = batch.Close()
				break
			}
			go printIfContains(msg, containing)
			if consumedCount > 0 && consumedCount%10000 == 0 {
				fmt.Printf("Checked %d messages\n", consumedCount)
			}
		}
	}
}

func GetTopicMetaAndMessage(address, topic string) (*TopicMetadata, kafka.Message, error) {
	c := &Handler{
		conn:        nil,
		genericConn: nil,
		topic:       topic,
		address:     address,
	}
	c.conn = c.getTopicConn(c.topic)
	defer c.conn.Close()

	tm := c.GetTopicMetadata(topic, true)

	msg, err := c.conn.ReadMessage(10e5)
	if err != nil {
		return nil, kafka.Message{}, err
	}

	return tm, msg, nil
}
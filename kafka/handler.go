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
	topicConns  map[string]*kafka.Conn
	address     string
}

// NewHandler - creates and returns Handler pointer.
func NewHandler() (*Handler, error) {
	address := config.GetConfig().Kafka.Address
	topic := config.GetConfig().Kafka.Topic
	topics := config.GetConfig().Kafka.Topics

	genConn, _ := kafka.Dial("tcp", address)

	c := &Handler{
		conn:        nil,
		genericConn: genConn,
		topic:       topic,
		topics:      topics,
		topicConns:  map[string]*kafka.Conn{},
		address:     address,
	}
	err := c.setConn()
	if err != nil {
		return nil, err
	}

	return c, nil
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

func (h *Handler) setConn() error {
	conn, err := kafka.Dial("tcp", h.address)
	h.conn = conn
	return err
}

func (h *Handler) getTopicConn(topic string) *kafka.Conn {
	if conn, exists := h.topicConns[topic]; exists {
		return conn
	}
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", h.address, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	h.topicConns[topic] = conn
	return conn
}

// GetControllerConn returns controller connection (controller connection is necessary for topic creation).
func (h *Handler) GetControllerConn() *kafka.Conn {
	broker, err := h.conn.Controller()
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
func (h *Handler) CreateTopic(topic string, replicationFactor int, partitionCount int) {

	cc := h.GetControllerConn()
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
func (h *Handler) FetchMessage() (string, error) {
	msg, err := h.conn.ReadMessage(5000000)
	if err != nil {
		return "", err
	}
	return string(msg.Value), nil
}

// WriteMessage writes a message to provided topic
func (h *Handler) WriteMessage(topic string, value []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Value: value,
	}
	n, err := h.conn.WriteMessages(msg)
	fmt.Println(n, err)
	return err
}

func (h *Handler) printMessageFromTopic(topic string, wg *sync.WaitGroup) {
	conn := h.getTopicConn(topic)
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

func (h *Handler) PrintMessageForEveryTopic() {
	wg := &sync.WaitGroup{}
	for _, topic := range h.topics {
		wg.Add(1)
		go h.printMessageFromTopic(topic, wg)
	}
	wg.Wait()
}

func (h *Handler) GetTopicMetadata(topic string, partitionDetails bool) *TopicMetadata {
	partitions, err := h.conn.ReadPartitions(topic)
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

func (h *Handler) FindMessageContaining(containing []byte) {
	consumedCount := 0
	for {
		batch := h.conn.ReadBatch(900000, 9000000)
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

func (h *Handler) GetTopicMetaAndMessage(topic string) (*TopicMetadata, kafka.Message, error) {
	conn := h.getTopicConn(topic)

	tm := h.GetTopicMetadata(topic, true)

	msg, err := conn.ReadMessage(10e5)
	if err != nil {
		return nil, kafka.Message{}, err
	}

	return tm, msg, nil
}

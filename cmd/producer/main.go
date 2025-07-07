package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	PublishMessage("Hello Kafka", "test-topic", producer, []byte("key1"), deliveryChan)

	event := <-deliveryChan
	msg := event.(*kafka.Message)
	if msg.TopicPartition.Error != nil {
		fmt.Printf("Failed to deliver message: %v\n", msg.TopicPartition.Error)
	} else {
		fmt.Printf("Message delivered to %s [%d] at offset %d\n",
			*msg.TopicPartition.Topic,
			msg.TopicPartition.Partition,
			msg.TopicPartition.Offset)
	}
	close(deliveryChan)
}

func NewKafkaProducer() *kafka.Producer{
	configMap := & kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		}
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println("Failed to create producer:", err.Error())
		return nil
	}
	return producer
}

func PublishMessage(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error{
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}
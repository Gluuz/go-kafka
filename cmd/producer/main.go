package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	producer := NewKafkaProducer()
	PublishMessage("Hello Kafka", "test-topic", producer, []byte("key1"))
	producer.Flush(1000) // Wait for messages to be delivered
	producer.Close()
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

func PublishMessage(msg string, topic string, producer *kafka.Producer, key []byte) error{
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}
	err := producer.Produce(message, nil)
	if err != nil {
		return err
	}
	return nil
}
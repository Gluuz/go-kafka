package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	PublishMessage("Hello Kafka", "test-topic", producer, []byte("key1"), deliveryChan)

	go DeliveryReport(deliveryChan)

	producer.Flush(1000) // Wait for messages to be delivered
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

func DeliveryReport(deliveryChan chan kafka.Event) {
	for event := range deliveryChan {
		switch e := event.(type) {
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				log.Printf("Failed to deliver message: %v\n", e.TopicPartition.Error)
			} else {
				log.Printf("Message delivered to %s [%d] at offset %d\n",
					*e.TopicPartition.Topic,
					e.TopicPartition.Partition,
					e.TopicPartition.Offset)
			}
		default:
			log.Printf("Ignored event: %v\n", e)
		}
	}
}
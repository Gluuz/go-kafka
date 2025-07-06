package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	fmt.Println("Hello, Producer!")
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
package main

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	kafkaBrokers := []string{"kafka:9092"}
	kafkaPublisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   kafkaBrokers,
			Marshaler: kafka.DefaultMarshaler{},
		},
		watermill.NewStdLogger(false, false),
	)
	log.Println("PDF Start")
	if err != nil {
		log.Fatalf("Error creating Kafka publisher: %v", err)
	}
	defer kafkaPublisher.Close()

	pdfPaths := []string{"LETTER_HEAD.pdf", "Ashish_Resume.pdf"}
	go consumer()
	for _, path := range pdfPaths {
		pdfData, err := ioutil.ReadFile(path)
		log.Println("PDF Mid", path)
		if err != nil {
			log.Printf("Error reading PDF file %s: %v", path, err)
			continue
		}
		log.Println("PDF Mid 2")
		// Create Watermill message
		msg := message.NewMessage(watermill.NewUUID(), pdfData)

		// Publish PDF message
		if err := kafkaPublisher.Publish("pdf-topic", msg); err != nil {
			log.Printf("Failed to publish message for PDF %s: %v", path, err)
			continue
		}
		log.Println("PDF End")
		log.Printf("Message published successfully for PDF %s", path)

	}
	time.Sleep(10 * time.Minute)
}

func consumer() {
	// Create a Watermill logger
	log.Println("Start PDF")
	logger := watermill.NewStdLogger(false, false)

	// Create a Kafka subscriber
	kafkaSubscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers: []string{"kafka:9092"},
			// Update this line with the correct field for consumer group ID
			// GroupID: "your_consumer_group_id",
			Unmarshaler: kafka.DefaultMarshaler{},
		},
		logger,
	)
	if err != nil {
		log.Fatalf("Error creating Kafka subscriber: %s", err)
	}

	// Subscribe to the "pdf-topic" topic
	messages, err := kafkaSubscriber.Subscribe(context.Background(), "pdf-topic")
	if err != nil {
		log.Fatalf("Error subscribing to Kafka topic: %s", err)
	}

	// Handle signals for graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Consume messages
	log.Println("Middle PDF")
	for {
		select {
		case msg := <-messages:
			log.Printf("Received message: %s\n", msg.Payload)
			log.Println("Middle PDF 1")
		case <-signals:
			log.Println("Interrupt signal received, shutting down...")
			log.Println("Middle PDF 2")
			return
		}
	}
}

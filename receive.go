package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
)

var messagesProcessed = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "messages_processed_total",
		Help: "Total number of messages processed by the consumer",
	},
)

var consumerStatus = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "consumer_status",
		Help: "Consumer connection status (1=connected, 0=disconnected)",
	},
)

func init() {
	prometheus.MustRegister(messagesProcessed)
	prometheus.MustRegister(consumerStatus)
}

func main() {
	log.Println("Starting RabbitMQ Consumer with Prometheus metrics...")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Failed to create channel:", err)
	}
	defer ch.Close()
	
	err = ch.Qos(
		1,     // prefetch count - max unacknowledged messages
		0,     // prefetch size - no limit on message size
		false, // global - apply to current channel only
	)
	if err != nil {
		log.Fatal("Failed to set QoS:", err)
	}

	q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
	if err != nil {
		log.Fatal("Failed to declare queue:", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue name
		"",     // consumer tag (empty = auto-generated)
		false,  // auto-ack (false = manual acknowledgment for reliability)
		false,  // exclusive (false = allow multiple consumers)
		false,  // no-local (false = receive messages published by this connection)
		false,  // no-wait (false = wait for server confirmation)
		nil,    // arguments (none)
	)
	if err != nil {
		log.Fatal("Failed to start consuming messages:", err)
	}

	consumerStatus.Set(1)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server started at: http://localhost:8082/metrics")
		
		// Start HTTP server on port 8082
		if err := http.ListenAndServe(":8082", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Consumer is ready. Waiting for messages...")
	log.Println("Press Ctrl+C to stop")

	go func() {
		
		for delivery := range msgs {
			startTime := time.Now()
			log.Printf("Received message: %s", delivery.Body)

			time.Sleep(500 * time.Millisecond)

			processingDuration := time.Since(startTime)
			err := delivery.Ack(false) 
			if err != nil {
				log.Printf("Failed to acknowledge message: %v", err)
				continue
			}
			messagesProcessed.Inc()

			log.Printf("Processed message successfully (took %v)", processingDuration)
		}
	}()

	<-sigChan
	
	consumerStatus.Set(0)
	
	log.Println("Shutdown signal received. Stopping consumer...")
	log.Println("Consumer stopped gracefully")
}

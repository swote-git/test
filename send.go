package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
)

var messagesSent = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "messages_sent_total",
		Help: "Total number of messages sent to RabbitMQ",
	},
)

func init() {
	prometheus.MustRegister(messagesSent)
}

func main() {
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
	
	q, err := ch.QueueDeclare("hello", false, false, false, false, nil)
	if err != nil {
		log.Fatal("Failed to declare queue:", err)
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics server started at: http://localhost:8081/metrics")
		
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	log.Println("Producer started. Sending messages every second...")
	log.Println("Press Ctrl+C to stop")

	for i := 1; ; i++ {
		message := fmt.Sprintf("Message #%d - %s", i, time.Now().Format("15:04:05"))

		// Publish message to RabbitMQ
		err = ch.Publish(
			"",     // exchange (empty string uses default exchange)
			q.Name, // routing key (queue name for default exchange)
			false,  // mandatory (false = don't return undeliverable messages)
			false,  // immediate (false = queue messages normally)
			amqp.Publishing{
				ContentType: "text/plain",           // MIME type of the message body
				Body:        []byte(message),        // Message content as byte array
				Timestamp:   time.Now(),             // Message timestamp
				MessageId:   fmt.Sprintf("msg_%d", i), // Unique message identifier
			})
		if err != nil {
			log.Printf("Failed to send message: %v", err)
		} else {
			// Log successful message send
			log.Printf("Sent: %s", message)
			messagesSent.Inc()
		}
		time.Sleep(1 * time.Second)
	}
}

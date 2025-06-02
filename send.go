package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streadway/amqp"
)

var (
	messagesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "producer_messages_sent_total",
			Help: "Total number of messages sent",
		},
		[]string{"queue", "status"},
	)
	
	connectionErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "producer_connection_errors_total",
			Help: "Total number of connection errors",
		},
	)
	
	lastMessageTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "producer_last_message_timestamp",
			Help: "Timestamp of last sent message",
		},
	)
	
	currentConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "producer_active_connections",
			Help: "Number of active RabbitMQ connections",
		},
	)
)

func init() {
	prometheus.MustRegister(messagesSent)
	prometheus.MustRegister(connectionErrors)
	prometheus.MustRegister(lastMessageTime)
	prometheus.MustRegister(currentConnections)
}

func getConfig() (string, string, int, int) {
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		vmIP := os.Getenv("VM_IP")
		if vmIP == "" {
			vmIP = "192.168.214.128"
		}
		rabbitmqURL = fmt.Sprintf("amqp://admin:admin123@%s:5672/", vmIP)
	}
	
	queueName := os.Getenv("QUEUE_NAME")
	if queueName == "" {
		queueName = "task_queue"
	}
	
	intervalStr := os.Getenv("SEND_INTERVAL")
	interval := 2
	if intervalStr != "" {
		if i, err := strconv.Atoi(intervalStr); err == nil {
			interval = i
		}
	}
	
	metricsPortStr := os.Getenv("METRICS_PORT")
	metricsPort := 8081
	if metricsPortStr != "" {
		if p, err := strconv.Atoi(metricsPortStr); err == nil {
			metricsPort = p
		}
	}
	
	return rabbitmqURL, queueName, interval, metricsPort
}

func connectRabbitMQ(url string) (*amqp.Connection, *amqp.Channel, error) {
	log.Printf("RabbitMQ connecting: %s", url)
	
	conn, err := amqp.Dial(url)
	if err != nil {
		connectionErrors.Inc()
		return nil, nil, fmt.Errorf("RabbitMQ connection failed: %v", err)
	}
	
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		connectionErrors.Inc()
		return nil, nil, fmt.Errorf("Channel creation failed: %v", err)
	}
	
	currentConnections.Set(1)
	log.Println("RabbitMQ connected successfully")
	return conn, ch, nil
}

func declareQueue(ch *amqp.Channel, queueName string) error {
	_, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("Queue declaration failed: %v", err)
	}
	log.Printf("Queue declared: %s", queueName)
	return nil
}

func sendMessage(ch *amqp.Channel, queueName string, message string) error {
	err := ch.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(message),
			Timestamp:    time.Now(),
		})
	
	if err != nil {
		messagesSent.WithLabelValues(queueName, "failed").Inc()
		return fmt.Errorf("Message publish failed: %v", err)
	}
	
	messagesSent.WithLabelValues(queueName, "success").Inc()
	lastMessageTime.SetToCurrentTime()
	return nil
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Producer OK"))
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ready"))
}

func startMetricsServer(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", healthCheckHandler)
	mux.HandleFunc("/ready", readyHandler)
	
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	
	log.Printf("Metrics server starting: http://0.0.0.0:%d", port)
	log.Printf("Metrics: http://0.0.0.0:%d/metrics", port)
	log.Printf("Health check: http://0.0.0.0:%d/health", port)
	
	if err := server.ListenAndServe(); err != nil {
		log.Printf("Metrics server error: %v", err)
	}
}

func main() {
	log.Println("Producer starting")
	
	rabbitmqURL, queueName, interval, metricsPort := getConfig()
	
	log.Printf("Configuration:")
	log.Printf("  RabbitMQ URL: %s", rabbitmqURL)
	log.Printf("  Queue: %s", queueName)
	log.Printf("  Interval: %d seconds", interval)
	log.Printf("  Metrics Port: %d", metricsPort)
	
	go startMetricsServer(metricsPort)
	
	var conn *amqp.Connection
	var ch *amqp.Channel
	var err error
	
	messageCount := 0
	for {
		if conn == nil || conn.IsClosed() {
			currentConnections.Set(0)
			log.Println("RabbitMQ reconnecting...")
			
			conn, ch, err = connectRabbitMQ(rabbitmqURL)
			if err != nil {
				log.Printf("Connection failed: %v", err)
				log.Println("Retrying in 5 seconds...")
				time.Sleep(5 * time.Second)
				continue
			}
			
			if err := declareQueue(ch, queueName); err != nil {
				log.Printf("Queue declaration failed: %v", err)
				ch.Close()
				conn.Close()
				conn = nil
				continue
			}
		}
		
		messageCount++
		message := fmt.Sprintf("Message #%d from Producer at %s", 
			messageCount, time.Now().Format("2006-01-02 15:04:05"))
		
		log.Printf("Sending message: %s", message)
		
		if err := sendMessage(ch, queueName, message); err != nil {
			log.Printf("Send failed: %v", err)
			ch.Close()
			conn.Close()
			conn = nil
		} else {
			log.Printf("Message sent successfully (#%d)", messageCount)
		}
		
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

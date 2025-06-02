package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streadway/amqp"
)

var (
	messagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consumer_messages_received_total",
			Help: "Total number of messages received",
		},
		[]string{"queue", "status"},
	)
	
	messagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consumer_messages_processed_total",
			Help: "Total number of messages processed",
		},
		[]string{"queue", "result"},
	)
	
	processingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "consumer_processing_duration_seconds",
			Help:    "Time spent processing messages",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"queue"},
	)
	
	connectionErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "consumer_connection_errors_total",
			Help: "Total number of connection errors",
		},
	)
	
	lastProcessedTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "consumer_last_processed_timestamp",
			Help: "Timestamp of last processed message",
		},
	)
	
	currentConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "consumer_active_connections",
			Help: "Number of active RabbitMQ connections",
		},
	)
	
	queueLength = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "consumer_queue_length",
			Help: "Current queue length",
		},
		[]string{"queue"},
	)
)

func init() {
	prometheus.MustRegister(messagesReceived)
	prometheus.MustRegister(messagesProcessed)
	prometheus.MustRegister(processingDuration)
	prometheus.MustRegister(connectionErrors)
	prometheus.MustRegister(lastProcessedTime)
	prometheus.MustRegister(currentConnections)
	prometheus.MustRegister(queueLength)
}

func getConfig() (string, string, int, int, int) {
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
	
	workersStr := os.Getenv("WORKER_COUNT")
	workers := 2
	if workersStr != "" {
		if w, err := strconv.Atoi(workersStr); err == nil && w > 0 {
			workers = w
		}
	}
	
	processingTimeStr := os.Getenv("MAX_PROCESSING_TIME")
	maxProcessingTime := 3
	if processingTimeStr != "" {
		if p, err := strconv.Atoi(processingTimeStr); err == nil && p > 0 {
			maxProcessingTime = p
		}
	}
	
	metricsPortStr := os.Getenv("METRICS_PORT")
	metricsPort := 8082
	if metricsPortStr != "" {
		if p, err := strconv.Atoi(metricsPortStr); err == nil {
			metricsPort = p
		}
	}
	
	return rabbitmqURL, queueName, workers, maxProcessingTime, metricsPort
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
	
	err = ch.Qos(1, 0, false)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("QoS setup failed: %v", err)
	}
	
	currentConnections.Set(1)
	log.Println("RabbitMQ connected successfully")
	return conn, ch, nil
}

func declareQueue(ch *amqp.Channel, queueName string) error {
	q, err := ch.QueueDeclare(
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
	
	queueLength.WithLabelValues(queueName).Set(float64(q.Messages))
	
	log.Printf("Queue declared: %s (messages: %d)", queueName, q.Messages)
	return nil
}

func processMessage(body string, maxProcessingTime int, queueName string) error {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		processingDuration.WithLabelValues(queueName).Observe(duration)
	}()
	
	processingTime := time.Duration(rand.Intn(maxProcessingTime)+1) * time.Second
	
	log.Printf("Processing message: %s (processing time: %v)", body, processingTime)
	
	time.Sleep(processingTime)
	
	if rand.Float32() < 0.9 {
		messagesProcessed.WithLabelValues(queueName, "success").Inc()
		lastProcessedTime.SetToCurrentTime()
		log.Printf("Message processed successfully: %s", body)
		return nil
	} else {
		messagesProcessed.WithLabelValues(queueName, "failed").Inc()
		return fmt.Errorf("processing failed (simulation)")
	}
}

func worker(id int, ch *amqp.Channel, queueName string, maxProcessingTime int) {
	log.Printf("Worker %d starting", id)
	
	msgs, err := ch.Consume(
		queueName,
		fmt.Sprintf("worker-%d", id),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("Worker %d: consume failed: %v", id, err)
		return
	}
	
	for msg := range msgs {
		messagesReceived.WithLabelValues(queueName, "received").Inc()
		
		log.Printf("Worker %d: message received: %s", id, string(msg.Body))
		
		if err := processMessage(string(msg.Body), maxProcessingTime, queueName); err != nil {
			log.Printf("Worker %d: processing failed: %v", id, err)
			messagesReceived.WithLabelValues(queueName, "failed").Inc()
			
			msg.Reject(true)
		} else {
			log.Printf("Worker %d: processing successful", id)
			messagesReceived.WithLabelValues(queueName, "success").Inc()
			
			msg.Ack(false)
		}
	}
	
	log.Printf("Worker %d stopping", id)
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Consumer OK"))
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ready"))
}

func queueStatusHandler(queueName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"queue": "%s", "status": "consuming"}`, queueName)))
	}
}

func startMetricsServer(port int, queueName string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", healthCheckHandler)
	mux.HandleFunc("/ready", readyHandler)
	mux.HandleFunc("/queue-status", queueStatusHandler(queueName))
	
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	
	log.Printf("Metrics server starting: http://0.0.0.0:%d", port)
	log.Printf("Metrics: http://0.0.0.0:%d/metrics", port)
	log.Printf("Health check: http://0.0.0.0:%d/health", port)
	log.Printf("Queue status: http://0.0.0.0:%d/queue-status", port)
	
	if err := server.ListenAndServe(); err != nil {
		log.Printf("Metrics server error: %v", err)
	}
}

func main() {
	log.Println("Consumer starting")
	
	rand.Seed(time.Now().UnixNano())
	
	rabbitmqURL, queueName, workers, maxProcessingTime, metricsPort := getConfig()
	
	log.Printf("Configuration:")
	log.Printf("  RabbitMQ URL: %s", rabbitmqURL)
	log.Printf("  Queue: %s", queueName)
	log.Printf("  Workers: %d", workers)
	log.Printf("  Max Processing Time: %d seconds", maxProcessingTime)
	log.Printf("  Metrics Port: %d", metricsPort)
	
	go startMetricsServer(metricsPort, queueName)
	
	for {
		conn, ch, err := connectRabbitMQ(rabbitmqURL)
		if err != nil {
			log.Printf("Connection failed: %v", err)
			log.Println("Retrying in 5 seconds...")
			currentConnections.Set(0)
			time.Sleep(5 * time.Second)
			continue
		}
		
		if err := declareQueue(ch, queueName); err != nil {
			log.Printf("Queue declaration failed: %v", err)
			ch.Close()
			conn.Close()
			continue
		}
		
		log.Printf("Starting %d workers...", workers)
		for i := 1; i <= workers; i++ {
			go worker(i, ch, queueName, maxProcessingTime)
		}
		
		closeCh := make(chan *amqp.Error)
		conn.NotifyClose(closeCh)
		
		log.Println("Consumer running... (Ctrl+C to exit)")
		
		select {
		case err := <-closeCh:
			if err != nil {
				log.Printf("RabbitMQ connection lost: %v", err)
			}
			currentConnections.Set(0)
		}
		
		log.Println("Preparing to reconnect...")
		ch.Close()
		conn.Close()
		time.Sleep(2 * time.Second)
	}
}

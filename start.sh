#!/bin/bash

echo "1. Cleaning up existing containers..."
docker stop rabbitmq prometheus 2>/dev/null
docker rm rabbitmq prometheus 2>/dev/null

# Start RabbitMQ server
echo "2. Starting RabbitMQ server..."
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  -p 15692:15692 \
  rabbitmq:3-management

# Wait for RabbitMQ initialization
echo "3. Waiting for RabbitMQ to initialize... (15 seconds)"
sleep 15

# Enable Prometheus plugin
echo "4. Enabling Prometheus plugin in RabbitMQ..."
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_prometheus

# Create Prometheus configuration
echo "5. Creating Prometheus configuration..."
cat > prometheus.yml << 'EOF'
global:
  scrape_interval: 10s

scrape_configs:
  - job_name: 'rabbitmq'
    static_configs:
      - targets: ['host.docker.internal:15692']
    scrape_interval: 10s

  - job_name: 'producer'
    static_configs:
      - targets: ['host.docker.internal:8081']
    scrape_interval: 5s

  - job_name: 'consumer'
    static_configs:
      - targets: ['host.docker.internal:8082']
    scrape_interval: 5s
EOF

# Start Prometheus server
echo "6. Starting Prometheus server..."
docker run -d --name prometheus \
  -p 9090:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus

echo "Infrastructure setup completed!"

package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("connection create error:", err)
	}
	defer conn.Close()

	// channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("channel create error:", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello",
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		log.Fatal("queue create error:", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatal("consume failed:", err)
	}

	// 메시지 처리 대기
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("consume: %s", d.Body)
		}
	}()

	log.Printf("pending... quit to CTRL+C")
	<-forever
}

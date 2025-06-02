package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("connection creation error:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("channel creation error:", err)
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
		log.Fatal("queue creation error:", err)
	}

	body := "hello smape queue"
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key 
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil {
		log.Fatal("send fail:", err)
	}

	log.Printf("send success: %s", body)
}

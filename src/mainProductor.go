package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

const (
	brokerAddress  = "localhost:9092"
	topicEnvio     = "envio"
	topicRespuesta = "respuesta"
)

func main() {
	var message string
	// Configurar el productor y el consumidor
	producer := newKafkaProducer(brokerAddress, topicEnvio)

	consumer := newKafkaConsumer(brokerAddress, topicRespuesta)

	// gorutina para recibir mensajes
	go recibiRespuesta(consumer)

	for {
		fmt.Scanf("%s\n", &message)
		enviarMensage(producer, message)
		//respuesta
	}

}

func newKafkaProducer(brokerAddress, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:  kafka.TCP(brokerAddress),
		Topic: topic,
	}
}

func newKafkaConsumer(brokerAddress, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})
}

func recibiRespuesta(consumer *kafka.Reader) {
	for {
		msg, err := consumer.ReadMessage(context.Background())
		fmt.Println("recibe")
		if err != nil {
			log.Fatal("Error reading message:", err)
			return
		}

		fmt.Println(string(msg.Value))
	}
}
func enviarMensage(w *kafka.Writer, message string) {
	err := w.WriteMessages(
		context.Background(),
		kafka.Message{
			Value: []byte(message),
		},
	)

	if err != nil {
		log.Println("Error sending message:", err)
	}

}

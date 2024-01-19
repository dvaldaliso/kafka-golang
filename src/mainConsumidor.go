package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	const (
		brokerAddress  = "localhost:9092"
		topicEnvio     = "envio"
		topicRespuesta = "respuesta"
	)

	// Configurar el productor
	producer := newKafkaProducerServidor(brokerAddress, topicRespuesta)
	// Configurar el consumidor
	consumer := newKafkaConsumerServidor(brokerAddress, topicEnvio)

	fmt.Println(" espero mensajes ... ")
	// Configura el canal de mensajes
	messageChannel := make(chan string)

	// Canal para manejar señales de interrupción
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	// Canal para cerrar limpiamente la aplicación
	done := make(chan bool)

	// Gorrutina Esperar mensajes del consumidor y enviarlos a los clientes
	go func() {
		for {
			select {
			case <-sigChan:
				log.Println("Received interrupt signal. Shutting down...")
				// Cerrar limpiamente el consumidor
				fmt.Println(" FIN ")
				consumer.Close()
				producer.Close()
				os.Exit(0)
				done <- true
				return
			case msg := <-messageChannel:
				//  lógica para enviar el mensaje a los clientes
				responder(producer, msg)
				fmt.Println("Sending message to clients:", msg)
			}
		}
	}()

	readMessage(consumer, messageChannel)

	// Esperar señales de interrupción
	select {
	case <-done:
		// Cerrar limpiamente el consumidor
		fmt.Println(" FIN ")
		consumer.Close()
		producer.Close()
		os.Exit(0)
	}

} // ()

func newKafkaProducerServidor(brokerAddress, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:  kafka.TCP(brokerAddress),
		Topic: topic,
	}
}

func newKafkaConsumerServidor(brokerAddress, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})
}

func readMessage(lector *kafka.Reader, messageChannel chan string) {
	for {

		msg, err := lector.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(" *** ERROR *** ")
			fmt.Println(err)
		}

		fmt.Println(msg.Topic)
		fmt.Println(string(msg.Value))

		messageChannel <- string(msg.Value)

	}
}

func responder(w *kafka.Writer, msg string) {
	fmt.Println("envio")
	err := w.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte("asd"),
			Value: []byte(msg),
		},
	)

	if err != nil {
		log.Println("Error sending message:", err)
	}
}

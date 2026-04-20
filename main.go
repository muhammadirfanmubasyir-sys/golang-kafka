package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const topicName = "bismillah-topic"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	switch os.Args[1] {
	case "producer":
		writer := &kafka.Writer{
			Addr:         kafka.TCP("localhost:9092"),
			Async:        true,
			RequiredAcks: kafka.RequireOne,
			BatchSize:    100,
			BatchTimeout: 1 * time.Second,
			Compression:  kafka.Gzip,
		}
		defer writer.Close()
		for {
			fmt.Print("> ")
			reader := bufio.NewReader(os.Stdin)

			response, err := reader.ReadString('\n')
			if err != nil {
				panic(err)
			}

			response = strings.TrimSuffix(response, "\n")
			if response == "" {
				break
			}

			if err := writer.WriteMessages(ctx, kafka.Message{
				Topic: topicName,
				Value: []byte(response),
				Key:   nil,
			}); err != nil {
				if errors.Is(err, context.Canceled) {
					break
				}
				panic(err)
			}
		}

	case "consumer":
		fmt.Println("1")
		groupId := os.Args[2]
		fmt.Println("2")
		consumer := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   topicName,
			GroupID: groupId,
		})
		fmt.Println("3")
		for {
			m, err := consumer.FetchMessage(ctx)
			fmt.Println("4, message: " + string(m.Value))
			if errors.Is(err, context.Canceled) {
				fmt.Println("5")
				break
			}

			if err != nil {
				fmt.Println("6")
				panic(err)
			}

			fmt.Println("RECEIVED: " + string(m.Value))
			err = consumer.CommitMessages(ctx, m)
			if err != nil {
				fmt.Println("7")
				panic(err)
			}
		}
	}
}

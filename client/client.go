package main

import (
	"bufio"
	"crypto/sha256"
	"flag"
	"fmt"
	"os"

	proto "github.com/michaelhenkel/broadcast/server/proto"

	"encoding/hex"
	"log"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	broadcastClient proto.BroadcastClient
	port            = flag.Int("port", 10000, "The server port")
	wait            *sync.WaitGroup
)

func init() {
	wait = &sync.WaitGroup{}
}

func connect(client *proto.Client) error {
	var streamerror error

	stream, err := broadcastClient.CreateStream(context.Background(), &proto.Connect{
		Client: client,
		Active: true,
	})

	if err != nil {
		return fmt.Errorf("connection failed: %v", err)
	}

	wait.Add(1)
	go func(str proto.Broadcast_CreateStreamClient) {
		defer wait.Done()

		for {
			msg, err := str.Recv()
			if err != nil {
				streamerror = fmt.Errorf("Error reading message: %v", err)
				break
			}

			fmt.Printf("%v : %s\n", msg.Id, msg.Content)

		}
	}(stream)

	return streamerror
}

func main() {
	flag.Parse()
	timestamp := time.Now()
	done := make(chan int)

	name := flag.String("N", "Anon", "The name of the user")
	flag.Parse()

	id := sha256.Sum256([]byte(timestamp.String() + *name))

	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", *port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Couldnt connect to service: %v", err)
	}

	broadcastClient = proto.NewBroadcastClient(conn)
	client := &proto.Client{
		Id:   hex.EncodeToString(id[:]),
		Name: *name,
	}

	connect(client)

	wait.Add(1)
	go func() {
		defer wait.Done()

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			msg := &proto.Message{
				Id:        client.Id,
				Content:   scanner.Text(),
				Timestamp: timestamp.String(),
			}

			_, err := broadcastClient.BroadcastMessage(context.Background(), msg)
			if err != nil {
				fmt.Printf("Error Sending Message: %v", err)
				break
			}
		}

	}()

	go func() {
		wait.Wait()
		close(done)
	}()

	<-done
}

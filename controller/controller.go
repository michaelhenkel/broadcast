package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"math/rand"

	"github.com/michaelhenkel/broadcast/nbc"
	proto "github.com/michaelhenkel/broadcast/server/proto"
	"github.com/michaelhenkel/broadcast/utils"

	"encoding/hex"
	"log"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var (
	errChan         = make(chan error)
	p               peer.Peer
	serverAddrs     addrsValue
	broadcastClient proto.BroadcastClient
	apiClient       proto.ApiClient
	wait            *sync.WaitGroup
	nbChan          *nbc.NonBlockingChan
	kindFlags       arrayFlags
	messageChan     = make(chan *proto.Message)
	kindChan        = make(map[string]*nbc.NonBlockingChan)
	controllerFuncs = map[string]func(*proto.Message) error{
		"KindA": func(msg *proto.Message) error {
			return reconcileKindA(msg)
		},
		"KindB": func(msg *proto.Message) error {
			return reconcileKindB(msg)
		},
	}
)

type addrsValue []string
type arrayFlags []string

func (as *addrsValue) String() string {
	return "localhost:10000"
}

func (as *addrsValue) Set(addr string) error {
	*as = append(*as, addr)
	return nil
}

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func init() {
	wait = &sync.WaitGroup{}
}

func connect(controller *proto.Controller) error {
	var streamerror error

	stream, err := broadcastClient.CreateStream(context.Background(), &proto.Connect{
		Controller: controller,
		Active:     true,
	})

	if err != nil {
		return fmt.Errorf("connection failed: %v", err)
	}
	//wait.Add(1)
	go func(str proto.Broadcast_CreateStreamClient) {
		//defer wait.Done()

		for {
			select {
			case <-stream.Context().Done():
				fmt.Println("stream closed by server")
				streamerror = fmt.Errorf("stream closed by server")
				errChan <- streamerror
			default:
				msg, err := str.Recv()
				if err == io.EOF {
					log.Println("exit")
					return
				}
				if err != nil {
					log.Printf("receive error %v", err)
					continue
				}
				fmt.Println("received", msg)

				oldMsg, err := apiClient.ReadMessage(context.Background(), msg, grpc.FailFast(false), grpc.Peer(&p))
				if err != nil {
					if status.Code(err) != codes.NotFound {
						streamerror = fmt.Errorf("Error reading message: %v", err)
						break
					}
				} else {
					ts := msg.Timestamp
					msg = oldMsg
					msg.Timestamp = ts
				}
				msg.Version = msg.Version + 1
				status := &proto.Status{
					Msg: "INPROGRESS",
				}
				msg.Status = status
				_, err = apiClient.WriteMessage(context.Background(), msg, grpc.FailFast(false), grpc.Peer(&p))
				if err != nil {
					streamerror = fmt.Errorf("cannot write file: %v", err)
					break
				}
				ack := &proto.Ack{
					Status:     0,
					Msg:        "ACK",
					Timestamp:  msg.Timestamp,
					Controller: controller.Name,
				}
				fmt.Println("sending ack")
				if err != nil {
					ack.Status = 1
				}
				_, err = broadcastClient.SendAck(context.Background(), ack)
				if err != nil {
					streamerror = fmt.Errorf("Error sending ack: %v", err)
					break
				}
				fmt.Println("sending msg to channel")
				kindChan[msg.Kind].Send <- msg
			}
		}
	}(stream)
	fmt.Println("returned")
	return streamerror
}

func processMessage(kind string) {
	for {
		select {
		case rcv := <-kindChan[kind].Recv:
			msg := rcv.(*proto.Message)
			log.Printf("processsing message %s/%s", msg.Kind, msg.Name)
			err := controllerFuncs[msg.Kind](msg)
			if err != nil {
				log.Println(err)
			}
		default:
		}
	}
}

func reconcileKindA(msg *proto.Message) error {
	fmt.Printf("%v : %d\n", msg.Id, msg.Version)
	r := rand.Intn(10)
	fmt.Printf("Waiting %d sec\n", r)
	time.Sleep(time.Duration(r) * time.Second)
	status := &proto.Status{}
	r = rand.Intn(5)
	if r <= 3 {
		log.Println("success")
		status.Msg = "SUCCESS"
	}
	if r >= 4 {
		log.Println("fail")
		status.Msg = "FAIL"
	}
	msg.Status = status
	newMsg, err := apiClient.ReadMessage(context.Background(), msg, grpc.FailFast(false), grpc.Peer(&p))
	if err != nil {
		return err
	}
	if msg.Timestamp != newMsg.Timestamp {
		log.Printf("msg was updated")
		return nil
	}
	if _, err := apiClient.WriteMessage(context.Background(), msg, grpc.FailFast(false), grpc.Peer(&p)); err != nil {
		return err
	}
	if len(msg.Messages) > 0 {
		for _, subMsg := range msg.Messages {
			_, err := apiClient.SendMessage(context.Background(), subMsg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func reconcileKindB(msg *proto.Message) error {
	fmt.Printf("%v : %d\n", msg.Id, msg.Version)
	r := rand.Intn(10)
	fmt.Printf("Waiting %d sec\n", r)
	time.Sleep(time.Duration(r) * time.Second)
	status := &proto.Status{}
	r = rand.Intn(5)
	if r <= 3 {
		log.Println("success")
		status.Msg = "SUCCESS"
	}
	if r >= 4 {
		log.Println("fail")
		status.Msg = "FAIL"
	}
	msg.Status = status
	newMsg, err := apiClient.ReadMessage(context.Background(), msg, grpc.FailFast(false), grpc.Peer(&p))
	if err != nil {
		return err
	}
	if msg.Timestamp != newMsg.Timestamp {
		log.Printf("msg was updated")
		return nil
	}
	if _, err := apiClient.WriteMessage(context.Background(), msg, grpc.FailFast(false), grpc.Peer(&p)); err != nil {
		return err
	}
	return nil
}

func main() {
	timestamp := time.Now()
	nbChan = nbc.New()

	flag.Var(&kindFlags, "kind", "kinds")
	port := flag.Int("port", 10000, "The server port")
	flag.Var(&serverAddrs, "server", "Server hostports")
	name := flag.String("c", "controller", "The name of the controller")
	flag.Parse()

	id := sha256.Sum256([]byte(timestamp.String() + *name))
	utils.Retry(time.Second, func() error {
		apiConn, err := grpc.Dial("dummy", grpc.WithInsecure(), grpc.WithBalancer(grpc.RoundRobin(utils.NewPseudoResolver(serverAddrs))))
		if err != nil {
			log.Fatalf("Couldnt connect to api: %v", err)
		}
		apiClient = proto.NewApiClient(apiConn)

		streamConn, err := grpc.Dial(fmt.Sprintf("localhost:%d", *port), grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Couldnt connect to stream: %v", err)
		}
		broadcastClient = proto.NewBroadcastClient(streamConn)
		var kindList []string
		for kind := range controllerFuncs {
			kindList = append(kindList, kind)
			kindChan[kind] = nbc.New()
			go processMessage(kind)
		}
		controller := &proto.Controller{
			Id:    hex.EncodeToString(id[:]),
			Name:  *name,
			Kinds: kindList,
		}
		if err := connect(controller); err != nil {
			fmt.Println("cannot connect to stream")
			return err
		}
		fmt.Println("connected to stream")

		err = <-errChan
		if err != nil {
			return err
		}
		return nil
	})

}

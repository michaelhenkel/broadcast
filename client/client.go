package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	proto "github.com/michaelhenkel/broadcast/server/proto"
	"github.com/michaelhenkel/broadcast/utils"
	"google.golang.org/grpc"
	glog "google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/peer"
	"gopkg.in/yaml.v2"
)

var (
	apiClient   proto.ApiClient
	serverAddrs addrsValue
	port        = flag.Int("port", 10000, "The server port")
	kind        = flag.String("kind", "kind1", "message kind")
	name        = flag.String("name", "name", "message name")
	file        = flag.String("file", "input.yaml", "file name")

	grpcLog glog.LoggerV2
)

type addrsValue []string

func (as *addrsValue) String() string {
	return "localhost:10000"
}

func (as *addrsValue) Set(addr string) error {
	*as = append(*as, addr)
	return nil
}

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
}

func main() {
	flag.Var(&serverAddrs, "server", "Server hostports")
	flag.Parse()

	inputYaml, err := ioutil.ReadFile(*file)
	if err != nil {
		log.Fatalln(err)
	}
	msg := &proto.Message{}
	if err := yaml.Unmarshal(inputYaml, msg); err != nil {
		log.Fatal(err)
	}

	conn, err := grpc.Dial("dummy", grpc.WithInsecure(), grpc.WithBalancer(grpc.RoundRobin(utils.NewPseudoResolver(serverAddrs))))
	if err != nil {
		log.Fatalf("Couldnt connect to api: %v", err)
	}

	apiClient = proto.NewApiClient(conn)
	msg.Timestamp = time.Now().String()
	msg.Id = "client1"
	var p peer.Peer
	ack, err := apiClient.SendMessage(context.Background(), msg, grpc.FailFast(false), grpc.Peer(&p))
	if err != nil {
		fmt.Println(conn.GetState().String())
		log.Fatalf("Couldnt send msg to service: %v", err)
	}
	grpcLog.Info("ack: ", ack)

}

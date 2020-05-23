//go:generate protoc -I proto -I ../ --go_out=plugins=grpc:${GOPATH}/src proto/server.proto

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"

	db "github.com/michaelhenkel/broadcast/server/db"
	proto "github.com/michaelhenkel/broadcast/server/proto"
	glog "google.golang.org/grpc/grpclog"
)

var (
	port    = flag.Int("port", 10000, "The server port")
	grpcLog glog.LoggerV2
	ackChan = make(map[string]map[string]chan *proto.Ack)
)

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
}

type Connection struct {
	stream  proto.Broadcast_CreateStreamServer
	id      string
	active  bool
	name    string
	kinds   []string
	filters map[string]string
	error   chan error
}

type Server struct {
	Connection     map[string]*Connection
	KindController map[string]*Connection
}

func (s *Server) SendMessage(ctx context.Context, msg *proto.Message) (*proto.Ack, error) {
	grpcLog.Info("Received msg: ", msg)
	_, err := s.BroadcastMessage(context.Background(), msg)
	if err != nil {
		fmt.Println(err)
	}
	return &proto.Ack{}, nil
}

func (s *Server) WriteMessage(ctx context.Context, msg *proto.Message) (*proto.Result, error) {
	grpcLog.Info("Writing msg: ", msg)
	if err := db.Write(msg); err != nil {
		fmt.Println(err)
		return &proto.Result{}, err
	}
	return &proto.Result{}, nil
}

func (s *Server) ReadMessage(ctx context.Context, msg *proto.Message) (*proto.Message, error) {
	grpcLog.Info("Reading msg: ", msg)
	msg, err := db.Read(msg)
	if err != nil {
		fmt.Println(err)
		return msg, err
	}
	return msg, nil
}

func (s *Server) AddFilter(ctx context.Context, filter *proto.Filter) (*proto.Result, error) {
	grpcLog.Info("Received add filter: ", filter)
	for _, conn := range s.Connection {
		if conn.name == filter.Controller.Name {
			conn.filters[filter.Name] = ""
			break
		}
	}
	return &proto.Result{}, nil
}

func (s *Server) DelFilter(ctx context.Context, filter *proto.Filter) (*proto.Result, error) {
	grpcLog.Info("Received del filter: ", filter)
	for _, conn := range s.Connection {
		if conn.name == filter.Controller.Name {
			if _, ok := conn.filters[filter.Name]; ok {
				delete(conn.filters, filter.Name)
				break
			}
		}
	}
	return &proto.Result{}, nil
}

func (s *Server) SendAck(ctx context.Context, ack *proto.Ack) (*proto.Result, error) {
	grpcLog.Info("Received Ack: ", ack)
	ackChan[ack.Timestamp][ack.Controller] <- ack
	return &proto.Result{}, nil
}

func (s *Server) CreateStream(pconn *proto.Connect, stream proto.Broadcast_CreateStreamServer) error {
	conn := &Connection{
		stream: stream,
		id:     pconn.Controller.Id,
		active: true,
		error:  make(chan error),
		name:   pconn.Controller.Name,
		kinds:  pconn.Controller.Kinds,
	}
	/*
		if len(s.Connection) == 0 {
			s.Connection = make(map[string]*Connection)
		}
	*/
	for _, kind := range conn.kinds {
		s.KindController[kind] = conn
	}
	s.Connection[pconn.Controller.Name] = conn
	fmt.Printf("registered controller %s with kinds %s\n", conn.name, conn.kinds)
loop:
	for {
		select {
		case <-conn.stream.Context().Done():
			fmt.Println("context done")
			delete(s.Connection, conn.name)
			break loop
		default:
		}
	}

	return <-conn.error
}

func (s *Server) BroadcastMessage(ctx context.Context, msg *proto.Message) (*proto.Ack, error) {
	wait := sync.WaitGroup{}
	done := make(chan int)
	ackChan[msg.Timestamp] = make(map[string]chan *proto.Ack)
	/*
		for _, conn := range s.Connection {
			wait.Add(1)

			go func(msg *proto.Message, conn *Connection) {
				defer wait.Done()
				if conn.active {
					for _, kind := range conn.kinds {
						if msg.Kind == kind {
							process := true
							if len(conn.filters) > 0 {
								if _, ok := conn.filters[msg.Name]; !ok {
									process = false
								}
							}
							if process {
								err := conn.stream.Send(msg)
								grpcLog.Info("Sending message to: ", conn.stream)
								if err != nil {
									grpcLog.Errorf("Error with Stream: %v - Error: %v", conn.stream, err)
									conn.active = false
									conn.error <- err
								}
								c := make(chan *proto.Ack)
								ackChan[msg.Timestamp][conn.name] = c
								for {
									select {
									case <-ackChan[msg.Timestamp][conn.name]:
										fmt.Println("received ack")
										delete(ackChan[msg.Timestamp], conn.name)
										break
									case <-conn.stream.Context().Done():
										fmt.Println("context done")
										delete(s.Connection, conn.name)
										delete(ackChan[msg.Timestamp], conn.name)
										break
									}
									break
								}
							}
						}
					}
				}
			}(msg, conn)
		}
	*/
	if conn, ok := s.KindController[msg.Kind]; ok {
		wait.Add(1)
		go func(msg *proto.Message, conn *Connection) {
			defer wait.Done()
			err := conn.stream.Send(msg)
			grpcLog.Info("Sending message to: ", conn.stream)
			if err != nil {
				grpcLog.Errorf("Error with Stream: %v - Error: %v", conn.stream, err)
				conn.active = false
				conn.error <- err
			}
			c := make(chan *proto.Ack)
			fmt.Println(ackChan)
			ackChan[msg.Timestamp][conn.name] = c
			for {
				select {
				case <-ackChan[msg.Timestamp][conn.name]:
					fmt.Println("received ack")
					delete(ackChan[msg.Timestamp], conn.name)
					break
				case <-conn.stream.Context().Done():
					fmt.Println("context done")
					delete(s.Connection, conn.name)
					delete(ackChan[msg.Timestamp], conn.name)
					break
				}
				break
			}
		}(msg, conn)
	} else {
		fmt.Printf("no controller for kind %s connected\n", msg.Kind)
		return &proto.Ack{}, nil
	}

	go func() {
		wait.Wait()
		close(done)
	}()
	<-done
	delete(ackChan, msg.Timestamp)
	return &proto.Ack{}, nil
}

func main() {
	flag.Parse()
	var connections = make(map[string]*Connection)
	var kindController = make(map[string]*Connection)

	server := &Server{
		Connection:     connections,
		KindController: kindController,
	}

	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("error creating the server %v", err)
	}

	grpcLog.Info("Starting server at port ", *port)

	proto.RegisterBroadcastServer(grpcServer, server)
	proto.RegisterApiServer(grpcServer, server)
	grpcServer.Serve(listener)
}

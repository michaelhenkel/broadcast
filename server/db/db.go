package db

import (
	"fmt"
	"io/ioutil"
	"os"

	proto "github.com/michaelhenkel/broadcast/server/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v2"
)

//Write message to DB
func Write(message *proto.Message) error {
	filename := message.Kind + ".yaml"
	messages := &proto.Messages{}
	var msgIdx *int
	if fileExists(filename) {
		dbFile, err := ioutil.ReadFile(filename)
		if err != nil {
			return fmt.Errorf("cannot read db for resource %s", message.Kind)
		}
		if err := yaml.Unmarshal(dbFile, messages); err != nil {
			return fmt.Errorf("cannot unmarshal db for resource %s", message.Kind)
		}
		for idx, msg := range messages.Messages {
			if message.Kind == msg.Kind && message.Name == msg.Name {
				msgIdx = &idx
				break
			}
		}
	}
	if msgIdx != nil {
		messages.Messages[*msgIdx] = message
	} else {
		messages.Messages = append(messages.Messages, message)
	}
	d, err := yaml.Marshal(&messages)
	if err != nil {
		return fmt.Errorf("cannot marshal  resource %s", message.Kind)
	}
	if err := ioutil.WriteFile(filename, d, 0644); err != nil {
		return fmt.Errorf("cannot write resource %s to db", message.Kind)
	}
	return nil
}

// Read reads a messate3 from the db
func Read(message *proto.Message) (*proto.Message, error) {
	filename := message.Kind + ".yaml"
	if !fileExists(filename) {
		return nil, status.Error(codes.NotFound, "message not found")
	}
	dbFile, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, status.Error(codes.NotFound, "message not found")
	}
	messages := &proto.Messages{}
	if err := yaml.Unmarshal(dbFile, messages); err != nil {
		return nil, fmt.Errorf("cannot unmarshal db for message %s", message.Kind)
	}
	for _, msg := range messages.Messages {
		if msg.GetName() == message.GetName() && msg.GetKind() == message.GetKind() {
			return msg, nil
		}
	}
	return nil, status.Error(codes.NotFound, "message not found")
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

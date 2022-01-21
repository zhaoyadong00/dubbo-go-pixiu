package zookeeper

import (
	"github.com/apache/dubbo-go-pixiu/pkg/model"
	"testing"
)

func TestZkEventListener_ListenRootEventV0(t *testing.T) {

	config := &model.RemoteConfig{
		Protocol: "zookeeper",
		Timeout:  "10000000s",
		Address:  "127.0.0.1:2181",
	}
	client, err := NewZkClient(config)
	if err != nil {
		t.Errorf("error create client %v", err)
	}

	dataListener := new(PiDataListener)
	listener := NewZkEventListener(client)

	// pi watch application
	listener.ListenRootEventV0(config, "/services", dataListener)
	// pi watch service

	// pi watch service instance

	select {}

}

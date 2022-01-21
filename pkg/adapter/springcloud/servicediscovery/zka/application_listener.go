/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zookeeper

import (
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/servicediscovery"
	"strings"
	"sync"
	"time"
)

import (
	_ "github.com/apache/dubbo-go/cluster/cluster_impl"
	_ "github.com/apache/dubbo-go/cluster/loadbalance"
	_ "github.com/apache/dubbo-go/common/proxy/proxy_factory"
	_ "github.com/apache/dubbo-go/filter/filter_impl"
	_ "github.com/apache/dubbo-go/metadata/service/inmemory"
	_ "github.com/apache/dubbo-go/metadata/service/remote"
	_ "github.com/apache/dubbo-go/registry/protocol"
	_ "github.com/apache/dubbo-go/registry/zookeeper"

	"github.com/dubbogo/go-zookeeper/zk"
)

import (
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/common"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/registry"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/remoting/zookeeper"
	"github.com/apache/dubbo-go-pixiu/pkg/common/constant"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
)

const (
	defaultServicesPath = "/services"
	methodsRootPath     = "/dubbo/metadata"
)

var _ registry.Listener = new(zkAppListener)

type zkAppListener struct {
	servicesPath string
	exit         chan struct{}
	client       *ZooKeeperClient
	//reg             *ZKRegistry
	svcListeners    *SvcListeners
	wg              sync.WaitGroup
	adapterListener common.RegistryEventListener

	ds *zookeeperDiscovery
}

// newZkAppListener returns a new newZkAppListener with pre-defined servicesPath according to the registered type.
func newZkAppListener(client *ZooKeeperClient, ds *zookeeperDiscovery, adapterListener common.RegistryEventListener) registry.Listener {
	p := defaultServicesPath
	return &zkAppListener{
		servicesPath:    p,
		exit:            make(chan struct{}),
		client:          client,
		adapterListener: adapterListener,
		svcListeners:    &SvcListeners{listeners: make(map[string]registry.Listener), listenerLock: sync.Mutex{}},
		ds:              ds,
	}
}

func (z *zkAppListener) Close() {
	close(z.exit)
	z.wg.Wait()
}

func (z *zkAppListener) WatchAndHandle() {
	z.wg.Add(1)
	go z.watch()
}

func (z *zkAppListener) watch() {
	defer z.wg.Done()

	var (
		failTimes  int64 = 0
		delayTimer       = time.NewTimer(ConnDelay * time.Duration(failTimes))
	)
	defer delayTimer.Stop()
	for {
		// pi servicesPath : "/services"
		children, e, err := z.client.GetChildrenW(z.servicesPath) // pi children : [sc1, sc2, sc3]
		// error handling
		if err != nil {
			failTimes++
			logger.Infof("watching (path{%s}) = error{%v}", z.servicesPath, err)
			// Exit the watch if root node is in error
			if err == zookeeper.ErrNilNode {
				logger.Errorf("watching (path{%s}) got errNilNode,so exit listen", z.servicesPath)
				return
			}
			if failTimes > MaxFailTimes {
				logger.Errorf("Error happens on (path{%s}) exceed max fail times: %s,so exit listen",
					z.servicesPath, MaxFailTimes)
				return
			}
			delayTimer.Reset(ConnDelay * time.Duration(failTimes))
			<-delayTimer.C
			continue
		}
		failTimes = 0
		if continueLoop := z.waitEventAndHandlePeriod(children, e); !continueLoop {
			return
		}
	}
}

func (z *zkAppListener) waitEventAndHandlePeriod(children []string, e <-chan zk.Event) bool {
	tickerTTL := defaultTTL
	ticker := time.NewTicker(tickerTTL)
	defer ticker.Stop()
	z.handleEvent(children) // pi children : [sc1, sc2, sc3]
	for {
		select {
		case <-ticker.C:
			z.handleEvent(children)
		case zkEvent := <-e:
			logger.Warnf("get a zookeeper e{type:%s, server:%s, path:%s, state:%d-%s, err:%s}",
				zkEvent.Type.String(), zkEvent.Server, zkEvent.Path, zkEvent.State, zookeeper.StateToString(zkEvent.State), zkEvent.Err)
			if zkEvent.Type != zk.EventNodeChildrenChanged {
				return true
			}
			z.handleEvent(children)
			return true
		case <-z.exit:
			logger.Warnf("listen(path{%s}) goroutine exit now...", z.servicesPath)
			return false
		}
	}
}

func (z *zkAppListener) handleEvent(children []string) {
	fetchChildren, err := z.client.GetChildren(z.servicesPath) // pi fetchChildren : [sc1, sc2, sc3]
	if err != nil {
		logger.Warnf("Error when retrieving newChildren in path: %s, Error:%s", z.servicesPath, err.Error())
	}

	discovery := z.ds

	go func() {
		keys := Keys(discovery.serviceMap)
		diff := Diff(keys, fetchChildren)
		// pi del
		if diff != nil {
			logger.Debugf("Del the service %s", diff)
			for _, sn := range diff {
				for _, instance := range discovery.serviceMap[sn] {
					// del
					discovery.listener.OnDeleteServiceInstance(&instance)
				}
			}
		}
	}()

	for _, path := range fetchChildren { // pi fetchChildren : [sc1, sc2, sc3]
		serviceName := strings.Join([]string{z.servicesPath, path}, constant.PathSlash) // pi serviceName : /services/sc1
		if z.svcListeners.GetListener(serviceName) != nil {
			continue
		}
		l := newApplicationServiceListener(serviceName, z.client, discovery, z.adapterListener) // pi serviceName : /services/sc1
		l.wg.Add(1)
		go l.WatchAndHandle()
		z.svcListeners.SetListener(serviceName, l)
	}
}

func Keys(m map[string][]servicediscovery.ServiceInstance) []string {
	j := 0
	keys := make([]string, len(m))
	for k := range m {
		keys[j] = k
		j++
	}
	return keys
}

func Diff(a, b []string) (diff []string) {
	m := make(map[string]bool)

	for _, item := range b {
		m[item] = true
	}

	for _, item := range a {
		if _, ok := m[item]; !ok {
			diff = append(diff, item)
		}
	}

	return diff
}

type SvcListeners struct {
	// listeners use url.ServiceKey as the index.
	listeners    map[string]registry.Listener
	listenerLock sync.Mutex
}

// GetListener returns existing listener or nil
func (s *SvcListeners) GetListener(id string) registry.Listener {
	s.listenerLock.Lock()
	defer s.listenerLock.Unlock()
	listener, ok := s.listeners[id]
	if !ok {
		return nil
	}
	return listener
}

// SetListener set the listener to the registry
func (s *SvcListeners) SetListener(id string, listener registry.Listener) {
	s.listenerLock.Lock()
	defer s.listenerLock.Unlock()
	s.listeners[id] = listener
}

// RemoveListener removes the listener of the registry
func (s *SvcListeners) RemoveListener(id string) {
	s.listenerLock.Lock()
	defer s.listenerLock.Unlock()
	delete(s.listeners, id)
}

func (s *SvcListeners) GetAllListener() map[string]registry.Listener {
	s.listenerLock.Lock()
	defer s.listenerLock.Unlock()
	return s.listeners
}

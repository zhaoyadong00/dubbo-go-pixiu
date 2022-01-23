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
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/registry"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/remoting/zookeeper"
	"github.com/apache/dubbo-go-pixiu/pkg/common/constant"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
)

const (
	defaultServicesPath = "/services"
)

var _ registry.Listener = new(zkAppListener)

type zkAppListener struct {
	servicesPath string
	exit         chan struct{}
	svcListeners    *SvcListeners
	wg              sync.WaitGroup

	ds *zookeeperDiscovery
}

// newZkAppListener returns a new newZkAppListener with pre-defined servicesPath according to the registered type.
func newZkAppListener(ds *zookeeperDiscovery) registry.Listener {
	p := defaultServicesPath
	return &zkAppListener{
		servicesPath:    p,
		exit:            make(chan struct{}),
		svcListeners:    &SvcListeners{listeners: make(map[string]registry.Listener), listenerLock: sync.Mutex{}},
		ds:              ds,
	}
}

func (z *zkAppListener) Close() {
	for k, listener := range z.svcListeners.GetAllListener() {
		z.svcListeners.RemoveListener(k)
		listener.Close()
	}
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
		//children, e, err := z.client.GetChildrenW(z.servicesPath) // pi children : [sc1, sc2, sc3]
		children, e, err := z.ds.getClient().GetChildrenW(z.servicesPath) // pi children : [sc1, sc2, sc3]
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
			//z.handleEvent(children)
		case zkEvent := <-e:
			logger.Debugf("get a zookeeper e{type:%s, server:%s, path:%s, state:%d-%s, err:%s}",
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
	fetchChildren, err := z.ds.getClient().GetChildren(z.servicesPath)
	//fetchChildren, err := z.client.GetChildren(z.servicesPath) // pi fetchChildren : [sc1, sc2, sc3]
	if err != nil {
		logger.Warnf("Error when retrieving newChildren in path: %s, Error:%s", z.servicesPath, err.Error())
	}

	discovery := z.ds

	del := func() {
		keys := Keys(discovery.getServiceMap())
		diff := Diff(keys, fetchChildren)
		if diff != nil {
			logger.Debugf("Del the service %s", diff)
			for _, sn := range diff {
				for _, instance := range discovery.getServiceMap()[sn] {
					// pi del
					discovery.delServiceInstance(instance)
				}
			}
		}
	};del()

	for _, serviceName := range fetchChildren { // pi fetchChildren : [sc1, sc2, sc3]
		serviceNodePath := strings.Join([]string{z.servicesPath, serviceName}, constant.PathSlash) // pi serviceName : /services/sc1
		if z.svcListeners.GetListener(serviceNodePath) != nil {
			continue
		}
		l := newApplicationServiceListener(serviceNodePath, serviceName, discovery) // pi serviceName : /services/sc1
		l.wg.Add(1)
		go l.WatchAndHandle()
		z.svcListeners.SetListener(serviceNodePath, l)
	}
}

func Keys(m map[string][]*servicediscovery.ServiceInstance) []string {
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

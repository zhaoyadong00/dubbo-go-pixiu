package zookeeper

import (
	"encoding/json"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/registry"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/common"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/servicediscovery"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
	"github.com/apache/dubbo-go-pixiu/pkg/model"
	"github.com/dubbogo/go-zookeeper/zk"
	"path"
	"sync"
	"time"
)

const (
	MaxFailTimes = 2
	ConnDelay    = 3 * time.Second
	//defaultTTL       = 10 * time.Minute
	defaultTTL = 30 * time.Second
)

type zookeeperDiscovery struct {
	client   *ZooKeeperClient
	basePath string

	targetService []string
	listener      servicediscovery.ServiceEventListener

	serviceMap  map[string][]servicediscovery.ServiceInstance
	instanceMap map[string]*servicediscovery.ServiceInstance
	watchMap    map[string]string

	watchAppLock      sync.Mutex
	watchInstanceLock sync.Mutex

	wmanager *WatchManager

	exit chan struct{}
	wg   sync.WaitGroup

	registryListener map[string]registry.Listener
}

func NewZKServiceDiscovery(targetService []string, config *model.RemoteConfig, listener servicediscovery.ServiceEventListener) (servicediscovery.ServiceDiscovery, error) {

	var err error

	if len(config.Timeout) == 0 {
		config.Timeout = "3s"
	}
	client, err := NewZookeeperClient(
		&model.Registry{
			Protocol: config.Protocol,
			Address:  config.Address,
			Timeout:  config.Timeout,
			Username: config.Username,
			Password: config.Password,
		})

	if err != nil {
		return nil, err
	}

	return &zookeeperDiscovery{
		client:           client,
		basePath:         "/services",
		listener:         listener,
		targetService:    targetService,
		serviceMap:       make(map[string][]servicediscovery.ServiceInstance),
		instanceMap:      make(map[string]*servicediscovery.ServiceInstance),
		watchMap:         make(map[string]string),
		registryListener: map[string]registry.Listener{},
		// watch manager
		wmanager: &WatchManager{
			basePath: "/services",
			client:   client,
			wlock:    sync.Mutex{},
			watchMap: make(map[WatchPath]*PiWatchEventHandler),
		},
	}, err
}

func (sd *zookeeperDiscovery) QueryAllServices() ([]servicediscovery.ServiceInstance, error) {
	serviceNames, err := sd.queryForNames()
	logger.Debugf("%s get all services by root path %s, services %v", common.ZKLogDiscovery, sd.basePath, serviceNames)
	if err != nil {
		return nil, err
	}
	return sd.QueryServicesByName(serviceNames)
}

func (sd *zookeeperDiscovery) QueryServicesByName(serviceNames []string) ([]servicediscovery.ServiceInstance, error) {

	var instancesAll []servicediscovery.ServiceInstance
	for _, s := range serviceNames {

		var instances []servicediscovery.ServiceInstance

		ids, err := sd.client.GetChildren(sd.pathForName(s))
		logger.Debugf("%s get services %s, services instanceIds %s", common.ZKLogDiscovery, s, ids)
		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			var instance *servicediscovery.ServiceInstance
			instance, err = sd.queryForInstance(s, id)
			if err != nil {
				return nil, err
			}
			instances = append(instances, *instance)
			instancesAll = append(instancesAll, *instance)
		}
		sd.serviceMap[s] = instances
	}

	// pi instance init
	for _, instance := range instancesAll {
		if sd.instanceMap[instance.ID] == nil {
			sd.instanceMap[instance.ID] = &instance
		}
	}

	return instancesAll, nil
}

// Register not now
func (sd *zookeeperDiscovery) Register() error {
	logger.Warnf("%s Register implement me!!", common.ZKLogDiscovery)
	return nil
}

// UnRegister not now
func (sd *zookeeperDiscovery) UnRegister() error {
	logger.Warnf("%s UnRegister implement me!!", common.ZKLogDiscovery)
	return nil
}

func (sd *zookeeperDiscovery) Subscribe() error {

	appListener := newZkAppListener(sd.client, sd, nil)
	sd.registryListener["app"] = appListener
	appListener.WatchAndHandle()

	//sd.watch()
	return nil
}

func (sd *zookeeperDiscovery) Unsubscribe() error {
	logger.Warnf("%s Unsubscribe implement me!!", common.ZKLogDiscovery)
	return nil
}

func (sd *zookeeperDiscovery) queryForInstance(name string, id string) (*servicediscovery.ServiceInstance, error) {
	path := sd.pathForInstance(name, id)
	data, err := sd.client.GetContent(path)
	if err != nil {
		return nil, err
	}
	sczk := &SpringCloudZKInstance{}
	instance := &servicediscovery.ServiceInstance{}
	err = json.Unmarshal(data, sczk)
	if err != nil {
		return nil, err
	}
	instance.Port = sczk.Port
	instance.ServiceName = sczk.Name
	instance.Host = sczk.Address
	instance.ID = sczk.ID
	instance.CLusterName = sczk.Name
	instance.Healthy = sczk.Payload.Metadata.InstanceStatus == "UP"
	return instance, nil
}

func (sd *zookeeperDiscovery) queryByServiceName() ([]string, error) {
	return sd.client.GetChildren(sd.basePath)
}

func (sd *zookeeperDiscovery) queryForNames() ([]string, error) {
	return sd.client.GetChildren(sd.basePath)
}

func (sd *zookeeperDiscovery) pathForInstance(name, id string) string {
	return path.Join(sd.basePath, name, id)
}

func (sd *zookeeperDiscovery) pathForName(name string) string {
	return path.Join(sd.basePath, name)
}

type SpringCloudZKInstance struct {
	Name    string      `json:"name"`
	ID      string      `json:"id"`
	Address string      `json:"address"`
	Port    int         `json:"port"`
	SslPort interface{} `json:"sslPort"`
	Payload struct {
		Class    string `json:"@class"`
		ID       string `json:"id"`
		Name     string `json:"name"`
		Metadata struct {
			InstanceStatus string `json:"instance_status"`
		} `json:"metadata"`
	} `json:"payload"`
	RegistrationTimeUTC int64  `json:"registrationTimeUTC"`
	ServiceType         string `json:"serviceType"`
	URISpec             struct {
		Parts []struct {
			Value    string `json:"value"`
			Variable bool   `json:"variable"`
		} `json:"parts"`
	} `json:"uriSpec"`
}

type WatchEventHandle interface {
	Handle(event <-chan zk.Event)
}
type PiWatchEventHandler struct{}

func (p *PiWatchEventHandler) Handle(event <-chan zk.Event) {

	zkEvent := <-event

	switch zkEvent.Type {
	case zk.EventNodeDataChanged:
		p.EventNodeDataChanged(zkEvent)
	case zk.EventNodeDeleted:
		p.EventNodeDeleted(zkEvent)
	case zk.EventNodeCreated:
		p.EventNodeCreated(zkEvent)
	case zk.EventNodeChildrenChanged:
		p.EventNodeChildrenChanged(zkEvent)
	case zk.EventSession:
		p.EventSession(zkEvent)
	case zk.EventNotWatching:
		p.EventNotWatching(zkEvent)
	default:
		logger.Debugf(common.ZKLogDiscovery, " none handler on event %s ", zkEvent.Type.String())
	}
}
func (p *PiWatchEventHandler) EventNodeDataChanged(event zk.Event) {

}
func (p *PiWatchEventHandler) EventNodeDeleted(event zk.Event) {

}
func (p *PiWatchEventHandler) EventNodeCreated(event zk.Event) {

}
func (p *PiWatchEventHandler) EventNodeChildrenChanged(event zk.Event) {

}
func (p *PiWatchEventHandler) EventNotWatching(event zk.Event) {

}
func (p *PiWatchEventHandler) EventSession(event zk.Event) {

}

// pi Application watch, e.p.: watch path "/services" childEvent
type AppWatchHandler struct {
	PiWatchEventHandler
	sd *zookeeperDiscovery
}

// Application add or delete
func (p *AppWatchHandler) EventNodeChildrenChanged(event zk.Event) {
	// pi watch service add or delete
}

// pi Application watch, e.p.: watch path "/services/sc1" childEvent
type ServiceWatchHandler struct {
	PiWatchEventHandler
	sd *zookeeperDiscovery
}

// Service instance add or delete
func (p *ServiceWatchHandler) EventNodeChildrenChanged(event zk.Event) {
	// pi watch service instance add or delete
}

// pi Application watch, e.p.: watch path "/services/sc1/10c59770-c3b3-496b-9845-3ee95fe8e62c" dataEvent
type ServiceInstanceWatchHandler struct {
	PiWatchEventHandler
	sd *zookeeperDiscovery
}

// Service instance update
func (p *ServiceInstanceWatchHandler) EventNodeDataChanged(event zk.Event) {
	// pi watch service instance update
}

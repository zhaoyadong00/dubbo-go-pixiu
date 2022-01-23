package zookeeper

import (
	"encoding/json"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/registry"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/common"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/servicediscovery"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
	"github.com/apache/dubbo-go-pixiu/pkg/model"
	"github.com/apache/dubbo-go-pixiu/pkg/remote/zookeeper"
	gxzookeeper "github.com/dubbogo/gost/database/kv/zk"
	"path"
	"sync"
	"time"
)

const (
	ZKRootPath = "/services"
	MaxFailTimes = 2
	ConnDelay    = 3 * time.Second
	//defaultTTL       = 10 * time.Minute
	defaultTTL = 30 * time.Second
)

type zookeeperDiscovery struct {
	//client   *gxzookeeper.ZookeeperClient
	basePath string

	targetService []string
	listener      servicediscovery.ServiceEventListener

	instanceMapLock      sync.Mutex
	instanceMap map[string]*servicediscovery.ServiceInstance

	//watchAppLock      sync.Mutex
	//watchInstanceLock sync.Mutex
	//
	//exit chan struct{}
	//wg   sync.WaitGroup

	registryListener map[string]registry.Listener
	clientFacade *BaseZkClientFacade
}

func NewZKServiceDiscovery(targetService []string, config *model.RemoteConfig, listener servicediscovery.ServiceEventListener) (servicediscovery.ServiceDiscovery, error) {

	var err error

	if len(config.Timeout) == 0 {
		config.Timeout = "3s"
	}

	client, err := zookeeper.NewZkClient(config)

	if err != nil {
		return nil, err
	}

	z := &zookeeperDiscovery{
		basePath:         ZKRootPath,
		listener:         listener,
		targetService:    targetService,
		instanceMap:      make(map[string]*servicediscovery.ServiceInstance),
		registryListener: map[string]registry.Listener{},
		clientFacade: &BaseZkClientFacade{
			client:  client,
			conf:       config,
			clientLock: sync.Mutex{},
			wg:         sync.WaitGroup{},
			done:       make(chan struct{}),
		},
	}
	go zookeeper.HandleClientRestart(z.clientFacade)
	return z, err
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

		ids, err := sd.getClient().GetChildren(sd.pathForName(s))
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
	}

	for _, instance := range instancesAll {
		if sd.instanceMap[instance.ID] == nil {
			sd.instanceMap[instance.ID] = &instance
		}
	}

	return instancesAll, nil
}

// Register not now
func (sd *zookeeperDiscovery) Register() error {
	logger.Debugf("%s Register implement me!!", common.ZKLogDiscovery)
	return nil
}

// UnRegister not now
func (sd *zookeeperDiscovery) UnRegister() error {
	logger.Debugf("%s UnRegister implement me!!", common.ZKLogDiscovery)
	return nil
}

func (sd *zookeeperDiscovery) getClient() *gxzookeeper.ZookeeperClient {
	if err := zookeeper.ValidateZookeeperClient(sd.clientFacade, "zka3"); err != nil {
		logger.Errorf("ValidateZookeeperClient error %s", err)
	}
	return sd.clientFacade.ZkClient()
}

func (sd *zookeeperDiscovery) Subscribe() error {

	sd.registryListener[sd.basePath] = newZkAppListener(sd)
	sd.registryListener[sd.basePath].WatchAndHandle()

	return nil
}

func (sd *zookeeperDiscovery) Unsubscribe() error {
	logger.Debugf("%s Unsubscribe me!", common.ZKLogDiscovery)

	for k, listener := range sd.registryListener {
		logger.Infof("Unsubscribe listener %s", k)
		listener.Close()
	}

	return nil
}

func (sd *zookeeperDiscovery) queryForInstance(name string, id string) (*servicediscovery.ServiceInstance, error) {
	path := sd.pathForInstance(name, id)
	data, _, err := sd.getClient().GetContent(path)
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

func (sd *zookeeperDiscovery) getServiceMap() map[string][]*servicediscovery.ServiceInstance {

	m := make(map[string][]*servicediscovery.ServiceInstance)

	for _, instance := range sd.instanceMap {

		if instances := m[instance.ServiceName]; instances == nil {
			m[instance.ServiceName] = []*servicediscovery.ServiceInstance{}
		}

		m[instance.ServiceName] = append(m[instance.ServiceName], instance)
	}

	return m
}

func (sd *zookeeperDiscovery) delServiceInstance(instance *servicediscovery.ServiceInstance) (bool, error) {

	if instance == nil {
		return true, nil
	}

	defer sd.instanceMapLock.Unlock()
	sd.instanceMapLock.Lock()
	if sd.instanceMap[instance.ID] != nil {
		sd.listener.OnDeleteServiceInstance(instance)
		delete(sd.instanceMap, instance.ID)
	}

	return true, nil
}

func (sd *zookeeperDiscovery) updateServiceInstance(instance *servicediscovery.ServiceInstance) (bool, error) {
	if instance == nil {
		return true, nil
	}

	defer sd.instanceMapLock.Unlock()
	sd.instanceMapLock.Lock()
	if sd.instanceMap[instance.ID] != nil {
		sd.listener.OnUpdateServiceInstance(instance)
		sd.instanceMap[instance.ID] = instance
	}

	return true, nil
}

func (sd *zookeeperDiscovery) addServiceInstance(instance *servicediscovery.ServiceInstance) (bool, error) {
	if instance == nil {
		return true, nil
	}

	defer sd.instanceMapLock.Unlock()
	sd.instanceMapLock.Lock()
	if sd.instanceMap[instance.ID] == nil {
		sd.listener.OnAddServiceInstance(instance)
		sd.instanceMap[instance.ID] = instance
	}

	return true, nil
}

func (sd *zookeeperDiscovery) queryByServiceName() ([]string, error) {
	return sd.getClient().GetChildren(sd.basePath)
}

func (sd *zookeeperDiscovery) queryForNames() ([]string, error) {
	return sd.getClient().GetChildren(sd.basePath)
}

func (sd *zookeeperDiscovery) pathForInstance(name, id string) string {
	return path.Join(sd.basePath, name, id)
}

func (sd *zookeeperDiscovery) pathForName(name string) string {
	return path.Join(sd.basePath, name)
}

type ZkServiceInstance struct {
	servicediscovery.ServiceInstance
}

func (i *ZkServiceInstance) GetUniqKey() string {
	return i.ID
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

type BaseZkClientFacade struct {
	client     *gxzookeeper.ZookeeperClient
	clientLock sync.Mutex
	wg         sync.WaitGroup
	done       chan struct{}
	conf       *model.RemoteConfig
}

func (b *BaseZkClientFacade) ZkClient() *gxzookeeper.ZookeeperClient {
	return b.client
}

func (b *BaseZkClientFacade) SetZkClient(client *gxzookeeper.ZookeeperClient) {
	b.client = client
}

func (b *BaseZkClientFacade) ZkClientLock() *sync.Mutex {
	return &b.clientLock
}

func (b *BaseZkClientFacade) WaitGroup() *sync.WaitGroup {
	return &b.wg
}

func (b *BaseZkClientFacade) Done() chan struct{} {
	return b.done
}

func (b *BaseZkClientFacade) RestartCallBack() bool {
	//
	return true
}

func (b *BaseZkClientFacade) GetConfig() *model.RemoteConfig {
	return b.conf
}
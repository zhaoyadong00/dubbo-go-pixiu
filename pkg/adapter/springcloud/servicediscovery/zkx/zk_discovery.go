package zookeeper

import (
	"encoding/json"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/common"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/servicediscovery"
	"github.com/apache/dubbo-go-pixiu/pkg/common/constant"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
	"github.com/apache/dubbo-go-pixiu/pkg/model"
	"github.com/apache/dubbo-go-pixiu/pkg/remote/zookeeper"
	"github.com/dubbogo/go-zookeeper/zk"
	gxset "github.com/dubbogo/gost/container/set"
	gxzookeeper "github.com/dubbogo/gost/database/kv/zk"
	"github.com/pkg/errors"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	MaxFailTimes = 2
	ConnDelay    = 3 * time.Second
	//defaultTTL       = 10 * time.Minute
	defaultTTL = 30 * time.Second
)

var (
	ErrNilNode = errors.Errorf("node does not exist")
)

type zookeeperDiscovery struct {
	client   *gxzookeeper.ZookeeperClient
	basePath string

	targetService []string
	listener      servicediscovery.ServiceEventListener

	serviceMap  map[string][]servicediscovery.ServiceInstance
	instanceMap map[string]servicediscovery.ServiceInstance
	watchMap    map[string]string

	watchAppLock      sync.Mutex
	watchInstanceLock sync.Mutex

	exit chan struct{}

	config *model.RemoteConfig

	wg                  sync.WaitGroup
	cltLock             sync.Mutex
	listenLock          sync.Mutex
	done                chan struct{}
	rootPath            string
	listenNames         []string
	instanceListenerMap map[string]*gxset.HashSet

	zklistener *zookeeper.ZkEventListener
}

func (sd *zookeeperDiscovery) ZkClient() *gxzookeeper.ZookeeperClient {
	return sd.client
}

func (sd *zookeeperDiscovery) SetZkClient(client *gxzookeeper.ZookeeperClient) {
	sd.client = client
}

func (sd *zookeeperDiscovery) ZkClientLock() *sync.Mutex {
	return &sd.cltLock
}

func (sd *zookeeperDiscovery) WaitGroup() *sync.WaitGroup {
	return &sd.wg
}

func (sd *zookeeperDiscovery) Done() chan struct{} {
	return sd.done
}

func (sd *zookeeperDiscovery) RestartCallBack() bool {
	//sd.csd.ReRegisterServices()
	sd.listenLock.Lock()
	defer sd.listenLock.Unlock()
	//for _, name := range zksd.listenNames {
	//	sd.csd.ListenServiceEvent(name, zksd)
	//}
	return true
}

func (sd *zookeeperDiscovery) GetConfig() *model.RemoteConfig {
	return sd.config
}

func (sd *zookeeperDiscovery) validate() bool {
	err := zookeeper.ValidateZookeeperClient(sd, "zookeeper")
	if err != nil {
		logger.Errorf("ValidateZookeeperClient fail!") // pi fail
		return false
	}
	return true
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

	return &zookeeperDiscovery{
		client:        client,
		basePath:      "/services",
		listener:      listener,
		targetService: targetService,
		serviceMap:    make(map[string][]servicediscovery.ServiceInstance),
		instanceMap:   make(map[string]servicediscovery.ServiceInstance),
		watchMap:      make(map[string]string),

		config:     config,
		zklistener: zookeeper.NewZkEventListener(client),
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

	//sd.watch()
	sd.watchx()

	return nil
}

func (sd *zookeeperDiscovery) watchx() {

	zklistener := sd.zklistener

	//go zklistener.ListenServiceEventV2(sd.config,  sd.basePath,nil)

	go zklistener.ListenRootEventV0(sd.config, sd.basePath, nil)

	//children, _ := sd.ZkClient().GetChildren(sd.basePath)
	//
	//for _, child := range children {
	//	servicePath := path.Join(sd.basePath, child)
	//	//go zklistener.ListenServiceEventV2(sd.config,  path.Join(sd.basePath, child),nil)
	//
	//	getChildren, _ := sd.ZkClient().GetChildren(servicePath)
	//
	//	for _, getChild := range getChildren {
	//		go zklistener.ListenServiceNodeEvent(path.Join(servicePath, getChild),nil)
	//	}
	//}
}

// pi watch for application change events
// https://aimuke.github.io/zookeeper/2019/06/18/zookeeper-watch/
func (sd *zookeeperDiscovery) watch() {

	zkPath := sd.basePath

	appWatch := func() {
		var (
			failTimes  int64 = 0
			delayTimer       = time.NewTimer(ConnDelay * time.Duration(failTimes))
		)
		defer delayTimer.Stop()
		for {
			sd.watchAppLock.Lock()
			watch := sd.watchMap[zkPath]
			if len(watch) > 0 {
				sd.watchAppLock.Unlock()
				return
			}
			logger.Debugf("%s Watch App path %s Create...", common.ZKLogDiscovery, zkPath)
			children, event, err := sd.client.GetChildrenW(zkPath)

			sd.watchMap[zkPath] = zkPath
			sd.watchAppLock.Unlock()
			if err != nil {
				failTimes++
				logger.Infof("Watching path : `%s` fail : ", zkPath, err)
				// Exit the watch if root node is in error
				if err == ErrNilNode {
					logger.Errorf("Watching path : `%s` got errNilNode, so exit listen", zkPath)
					//return
				}
				if failTimes > MaxFailTimes {
					logger.Errorf("Error happens on (path{%s}) exceed max fail times: %s,so exit listen", zkPath, MaxFailTimes)
					//return
				}
				delayTimer.Reset(ConnDelay * time.Duration(failTimes))
				<-delayTimer.C
				continue
			}
			failTimes = 0

			sd.AppEventNodeCallback(zkPath, children, event)
		}
	}

	keys := func(m map[string][]servicediscovery.ServiceInstance) []string {
		j := 0
		keys := make([]string, len(m))
		for k := range m {
			keys[j] = k
			j++
		}
		return keys
	}

	instanceWatch := func() {

		serviceNames := keys(sd.serviceMap)

		for _, child := range serviceNames {
			go func() {
				var (
					failTimes  int64 = 0
					delayTimer       = time.NewTimer(ConnDelay * time.Duration(failTimes))
				)
				defer delayTimer.Stop()

				for {

					childPath := strings.Join([]string{zkPath, child}, constant.PathSlash)

					sd.watchInstanceLock.Lock()
					watch := sd.watchMap[childPath]
					if len(watch) > 0 {
						sd.watchInstanceLock.Unlock()
						return
					}

					logger.Debugf("%s Watch Instance path %s create...", common.ZKLogDiscovery, child)

					children, event, err := sd.client.GetChildrenW(childPath)

					sd.watchMap[childPath] = childPath
					sd.watchInstanceLock.Unlock()
					if err != nil {
						failTimes++
						logger.Debugf("Watching path : `%s` fail : ", zkPath, err)
						if err == ErrNilNode {
							logger.Errorf("Watching path : `%s` got errNilNode, so exit listen", zkPath)
							//return
						}
						if failTimes > MaxFailTimes {
							logger.Errorf("Error happens on (path{%s}) exceed max fail times: %s,so exit listen", zkPath, MaxFailTimes)
							//return
						}
						delayTimer.Reset(ConnDelay * time.Duration(failTimes))
						<-delayTimer.C
						continue
					}
					failTimes = 0

					sd.ServiceEventNodeCallback(childPath, children, event)
				}
			}()
		}
	}

	instanceWatchPeriod := func() {

		ticker := time.NewTicker(ConnDelay)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				instanceWatch()
			case <-sd.exit:
				logger.Warnf("listen(path{%s}) goroutine exit now...", sd.basePath)
			}
		}
	}

	go appWatch()
	go instanceWatchPeriod()

	// todo watch data change
}

func (sd *zookeeperDiscovery) ServiceEventNodeCallback(pZkPath string, children []string, event <-chan zk.Event) {
	logger.Debugf("%s Watching path %s nodes event of child...", common.ZKLogDiscovery, pZkPath)

	zkEvent := <-event
	cZkPath := zkEvent.Path
	logger.Debugf("%s Event %v ", common.ZKLogDiscovery, zkEvent.Type.String())
	serviceInstancePath := cZkPath
	switch zkEvent.Type {
	case zk.EventNodeDataChanged:
		break
	case zk.EventNodeDeleted:
		break
	case zk.EventNodeCreated:
		break
	case zk.EventNodeChildrenChanged:
		exists, _, err := sd.ZkClient().Conn.Exists(serviceInstancePath) // pi todo remove
		if err != nil {
			logger.Errorf("%s Callback Exists err: %s", common.ZKLogDiscovery, err.Error())
			break
		}
		split := strings.Split(cZkPath, constant.PathSlash)
		sn := split[2]
		if !exists {
			for _, instance := range sd.serviceMap[sn] {
				sd.instanceHandle(cZkPath, &instance, "del")
			}
		} else {
			ssi, err := sd.QueryServicesByName([]string{sn})
			if err != nil {
				logger.Errorf("%s Callback QueryServicesByName err: %s", common.ZKLogDiscovery, err.Error())
				break
			}
			if ssi == nil {
				for _, instance := range sd.serviceMap[sn] {
					sd.instanceHandle(cZkPath, &instance, "del")
				}
				break
			}
			for _, instance := range ssi {
				if len(sd.instanceMap[instance.ID].ID) > 0 {
					sd.instanceHandle(sn, &instance, "update")
				} else {
					sd.instanceHandle(sn, &instance, "add")
				}
			}
		}

	default:
		logger.Info(common.ZKLogDiscovery, " none handler on event %s ", zkEvent.Type.String())
	}
	delete(sd.watchMap, serviceInstancePath)
	return
}

func (sd *zookeeperDiscovery) AppEventNodeCallback(pZkPath string, children []string, event <-chan zk.Event) {
	logger.Debugf("%s Watching path %s nodes event of child...", common.ZKLogDiscovery, pZkPath)
	zkEvent := <-event
	cZkPath := zkEvent.Path
	logger.Debugf("%s Event %v callback", common.ZKLogDiscovery, zkEvent.Type.String())
	switch zkEvent.Type {
	case zk.EventNodeDataChanged:
		break
	case zk.EventNodeDeleted:
		break
	case zk.EventNodeCreated:
		break
	case zk.EventNodeChildrenChanged:

		serviceNames, err := sd.client.GetChildren(cZkPath)

		if err != nil {
			logger.Errorf("%s Callback queryForInstance err: %s", common.ZKLogDiscovery, err.Error())
			break
		}

		sns := sort.StringSlice(serviceNames)
		sns.Sort()

		for sn, instances := range sd.serviceMap {
			pos := sort.SearchStrings(sns, sn)
			if pos != len(sns) {
				if sn == sns[pos] {
					// nope
				} else {
					for _, instance := range instances {
						sd.instanceHandle(cZkPath, &instance, "del") // del
					}
				}
			} else {
				for _, instance := range instances {
					sd.instanceHandle(cZkPath, &instance, "del") // del
				}
			}
		}

		for _, name := range serviceNames {
			if sd.serviceMap[name] == nil {
				instances, err := sd.QueryServicesByName([]string{name})
				if err != nil {
					logger.Errorf("%s Callback queryForInstance err: %s", common.ZKLogDiscovery, err.Error())
					break
				}

				for _, instance := range instances {
					sd.instanceHandle(cZkPath, &instance, "add") // add
				}
			}
		}
	default:
		logger.Debugf(common.ZKLogDiscovery, " none handler on event %s ", zkEvent.Type.String())
	}
	delete(sd.watchMap, pZkPath)
	return
}

func (sd *zookeeperDiscovery) Unsubscribe() error {
	logger.Warnf("%s Unsubscribe implement me!!", common.ZKLogDiscovery)
	return nil
}

func (sd *zookeeperDiscovery) queryForInstanceByPath(path string) (*servicediscovery.ServiceInstance, error) {
	//data, err := sd.ZkClient().GetContent(path)
	data, _, err := sd.ZkClient().GetContent(path)
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

func (sd *zookeeperDiscovery) queryForInstance(name string, id string) (*servicediscovery.ServiceInstance, error) {
	path := sd.pathForInstance(name, id)
	data, _, err := sd.ZkClient().GetContent(path)
	//data, err := sd.ZkClient().GetContent(path)
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
	sd.validate()
	return sd.ZkClient().GetChildren(sd.basePath)
}

func (sd *zookeeperDiscovery) queryForNames() ([]string, error) {
	sd.validate()
	return sd.ZkClient().GetChildren(sd.basePath)
}

func (sd *zookeeperDiscovery) pathForInstance(name, id string) string {
	return path.Join(sd.basePath, name, id)
}

func (sd *zookeeperDiscovery) pathForName(name string) string {
	return path.Join(sd.basePath, name)
}

func (sd *zookeeperDiscovery) instanceHandle(serviceName string, instance *servicediscovery.ServiceInstance, operation string) {
	logger.Debugf(" %s Handle instance %v operation %s", common.ZKLogDiscovery, instance, operation)
	switch strings.ToLower(operation) {
	case "update":
		sd.listener.OnUpdateServiceInstance(instance)
		sd.serviceMap[instance.ServiceName] = append(sd.serviceMap[instance.ServiceName], *instance)
		sd.instanceMap[instance.ID] = *instance
	case "del":
		sd.listener.OnDeleteServiceInstance(instance)
		delete(sd.serviceMap, serviceName)
		delete(sd.instanceMap, instance.ID)
	case "add":
		sd.listener.OnAddServiceInstance(instance)
		sd.serviceMap[instance.ServiceName] = append(sd.serviceMap[instance.ServiceName], *instance)
		sd.instanceMap[instance.ID] = *instance
	default:
		logger.Info(common.ZKLogDiscovery, " default none match operation: ", operation)
	}
	return
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

type TreeCacheListener interface {
	childEvent(client *gxzookeeper.ZookeeperClient, event <-chan zk.Event)
}

type TreeNode struct {
}
type ChildData struct {
	path string
	stat zk.State
}

type WatchEventHandle interface {
	handle(event <-chan zk.Event, callback func())
}

type PiWatchEventHandler struct{}

func (p *PiWatchEventHandler) handle(event <-chan zk.Event, callback func()) {

	zkEvent := <-event

	switch zkEvent.Type {
	case zk.EventNodeDataChanged:
		break
	case zk.EventNodeDeleted:
		break
	case zk.EventNodeCreated:
		break
	case zk.EventNodeChildrenChanged:
		p.EventNodeChildrenChanged(event)
		break
	case zk.EventSession:
		break
	case zk.EventNotWatching:
		break
	default:
		logger.Debugf(common.ZKLogDiscovery, " none handler on event %s ", zkEvent.Type.String())
	}
}

func (p *PiWatchEventHandler) EventNodeChildrenChanged(event <-chan zk.Event) {
	logger.Debugf("%s PiWatchEventHandler handle EventNodeChildrenChanged")
}

type PiWatch struct {
	*PiWatchEventHandler

	path string

	client *gxzookeeper.ZookeeperClient
}

// todo
func (pw *PiWatch) EventNodeChildrenChanged(event <-chan zk.Event) {
	logger.Debugf("PiWatchEventHandler EventNodeChildrenChanged")
}

type PiDataListener struct {
}

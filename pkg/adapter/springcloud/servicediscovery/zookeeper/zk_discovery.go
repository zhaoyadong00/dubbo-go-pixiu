package zookeeper

import (
	"encoding/json"
	zk "github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/remoting/zookeeper"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/common"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/servicediscovery"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
	"github.com/apache/dubbo-go-pixiu/pkg/model"
	zk2 "github.com/dubbogo/go-zookeeper/zk"
	"path"
)

type zookeeperDiscovery struct {
	
	client *zk.ZooKeeperClient
	basePath string

	targetService []string
	listener    servicediscovery.ServiceEventListener
	instanceMap map[string]servicediscovery.ServiceInstance
}

func (sd *zookeeperDiscovery) QueryAllServices() ([]servicediscovery.ServiceInstance, error) {
	serviceNames, err := sd.queryForNames()
	logger.Debugf("%s services %v", common.ZKLogDiscovery, serviceNames)
	if err != nil {
		return nil, err
	}
	return sd.QueryServicesByName(serviceNames)
}

func (sd *zookeeperDiscovery) QueryServicesByName(serviceNames []string) ([]servicediscovery.ServiceInstance, error) {

	var instances []servicediscovery.ServiceInstance

	for _, s := range serviceNames {
		ids, err := sd.client.GetChildren(sd.pathForName(s))
		logger.Debugf("%s service name %s, id %s", common.ZKLogDiscovery, s, ids)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			var instance  *servicediscovery.ServiceInstance
			instance, err = sd.queryForInstance(s, id)
			if err != nil {
				return nil, err
			}
			instances = append(instances, *instance)
		}
	}
	return instances, nil
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
	names := sd.listener.GetServiceNames()
	logger.Debugf("%s names %s" , common.ZKLogDiscovery , names)
	for _ , v := range names {
		p := sd.pathForName(v)
		ids , event , err := sd.client.GetChildrenW(p)
		if err != nil {
			logger.Warnf("%s Subscribe GetChildrenW err: %s" , common.ZKLogDiscovery , err.Error())
		}
		instance , err := sd.queryForInstance(v , ids[0])
		if err != nil {
			logger.Warnf("%s Callback queryForInstance err: %s" , common.ZKLogDiscovery , err.Error())
			return err
		}
		go sd.Callback(instance , event)
	}
	return nil
}

func (sd *zookeeperDiscovery) Unsubscribe() error {
	//panic("implement me")
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


func GetServiceDiscovery(targetService []string, config *model.RemoteConfig, listener servicediscovery.ServiceEventListener) (servicediscovery.ServiceDiscovery, error) {

	var err error

	if len(config.Timeout) == 0 {
		config.Timeout = "3s"
	}
	client, err := zk.NewZookeeperClient(
		&model.Registry{
			Protocol: config.Protocol,
			Address: config.Address,
			Timeout: config.Timeout,
			Username: config.Username,
			Password: config.Password,
	})

	if err != nil {
		return nil, err
	}

	return &zookeeperDiscovery{
		client: client,
		basePath: "/services",
		listener: listener,
		targetService: targetService,
	}, err
}


func (sd *zookeeperDiscovery)Callback(instance *servicediscovery.ServiceInstance , event <-chan zk2.Event) {
	logger.Info(common.ZKLogDiscovery , " sd.client" , sd.client , "targetService:" , sd.targetService , "instanceMap:" , sd.instanceMap , "listener:" , sd.listener)
	res := <- event
	switch int(res.State) {
	case (int)(zk2.EventNodeDataChanged):
		sd.listener.OnUpdateServiceInstance(instance)
	case (int)(zk2.EventNodeDeleted):
		sd.listener.OnDeleteServiceInstance(instance)
	case (int)(zk2.EventNodeCreated):
		sd.listener.OnAddServiceInstance(instance)
	case (int)(zk2.EventNodeChildrenChanged):
		logger.Info(common.ZKLogDiscovery , " Callback EventNodeChildrenChanged")
	default :
		logger.Info(common.ZKLogDiscovery , " default")
	}
	logger.Info(common.ZKLogDiscovery , " Callback:" , res)
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
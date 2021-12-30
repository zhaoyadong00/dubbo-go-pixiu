package zookeeper

import (
	"encoding/json"
	zk "github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/remoting/zookeeper"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/common"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/springcloud/servicediscovery"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
	"github.com/apache/dubbo-go-pixiu/pkg/model"
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
	//panic("implement me")
	serviceNames, err := sd.queryForNames()
	logger.Infof("%s find services %v", common.LogDiscovery, serviceNames)
	if err != nil {
		return nil, err
	}
	return sd.QueryServicesByName(serviceNames)
}

func (sd *zookeeperDiscovery) QueryServicesByName(serviceNames []string) ([]servicediscovery.ServiceInstance, error) {

	var instances []servicediscovery.ServiceInstance

	for _, s := range serviceNames {
		ids, err := sd.client.GetChildren(sd.pathForName(s))
		logger.Infof("%s find service vid %s of service name %s", common.LogDiscovery, ids, s)
		if err != nil {
			return nil, err
		}
		logger.Infof("%s find service instance %v ", common.LogDiscovery, ids)
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

func (sd *zookeeperDiscovery) Register() error {
	//panic("implement me")
	logger.Warnf("%s Register implement me!!", common.LogDiscovery)
	return nil
}

func (sd *zookeeperDiscovery) UnRegister() error {
	//panic("implement me")
	logger.Warnf("%s UnRegister implement me!!", common.LogDiscovery)
	return nil
}

func (sd *zookeeperDiscovery) Subscribe() error {
	//panic("implement me")
	logger.Warnf("%s Subscribe implement me!!", common.LogDiscovery)
	return nil
}

func (sd *zookeeperDiscovery) Unsubscribe() error {
	//panic("implement me")
	logger.Warnf("%s Unsubscribe implement me!!", common.LogDiscovery)
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
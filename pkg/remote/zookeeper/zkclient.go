package zookeeper

import (
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
	"github.com/apache/dubbo-go-pixiu/pkg/model"
	gxzookeeper "github.com/dubbogo/gost/database/kv/zk"
	perrors "github.com/pkg/errors"
	"strings"
	"time"
)

const (
	// ConnDelay connection delay interval
	ConnDelay = 3
	// MaxFailTimes max fail times
	MaxFailTimes = 3
)

// ValidateZookeeperClient validates client and sets options
func ValidateZookeeperClient(container ZkClientFacade, zkName string) error {
	lock := container.ZkClientLock()
	config := container.GetConfig()

	lock.Lock()
	defer lock.Unlock()

	if container.ZkClient() == nil {
		// in dubbo, every registry only connect one node, so this is []string{r.Address}
		//timeout := url.GetParamDuration(constant.ConfigTimeoutKey, constant.DefaultRegTimeout)

		//timeout, _ := time.ParseDuration(config.Timeout)
		//zkAddresses := strings.Split(config.Address, ",")

		//logger.Infof("[Zookeeper Client] New zookeeper client with name = %s, zkAddress = %s, timeout = %d", zkName, config.Address, timeout.String())
		//newClient, cltErr := gxzookeeper.NewZookeeperClient(zkName, zkAddresses, true, gxzookeeper.WithZkTimeOut(timeout))
		newClient, cltErr := NewZkClient(config)
		if cltErr != nil {
			//logger.Warnf("newZookeeperClient(name{%s}, zk address{%v}, timeout{%d}) = error{%v}", zkName, config.Address, timeout.String(), cltErr)
			return perrors.WithMessagef(cltErr, "newZookeeperClient(address:%+v)", config.Address)
		}
		container.SetZkClient(newClient)
	}
	return nil
}

func NewZkClient(config *model.RemoteConfig) (*gxzookeeper.ZookeeperClient, error) {
	var (
		zkName      = "zk_name"
		zkAddresses = strings.Split(config.Address, ",")
	)
	timeout, _ := time.ParseDuration(config.Timeout)

	newClient, cltErr := gxzookeeper.NewZookeeperClient(zkName, zkAddresses, true, gxzookeeper.WithZkTimeOut(timeout))
	if cltErr != nil {
		logger.Warnf("newZookeeperClient(name{%s}, zk address{%v}, timeout{%d}) = error{%v}",
			zkName, config.Address, timeout.String(), cltErr)
		return nil, perrors.WithMessagef(cltErr, "newZookeeperClient(address:%+v)", config.Address)
	}
	return newClient, nil
}

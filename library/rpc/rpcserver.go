/**
 *描述：rpc服务端
 *      注册远程rpc engpoint对象，并启动rpc服务
 *作者：江洪
 *时间：2019-5-20
 */

package rpc

import (
	"errors"
	"fmt"
	"github.com/smallnest/rpcx/server"
	"library_Test/common"
	"library_Test/library/config"
	"library_Test/library/log"
	"library_Test/library/servicediscovery"
	"library_Test/library/utils"
	"os"
	"strconv"
)

type RpcServer struct {
	Id     string
	Config *RpcServerConfig
}

type Protocol string

const (
	Tcp  Protocol = "tcp"
	Http Protocol = "http"
	Unix Protocol = "unix"
	Quic Protocol = "quic"
	Kcp  Protocol = "kcp"
)

type RpcServerConfig struct {
	ServiceName string
	Port        string
	Info        map[string]interface{}
	Protocol    Protocol
	Discovery   servicediscovery.ServiceDiscovery
}

func (p Protocol) String() string {
	return string(p)
}

//创建rpc服务实例
func NewRpcServer(config *RpcServerConfig) *RpcServer {
	return &RpcServer{Config: config}
}

//启动
func (server *RpcServer) Start() error {
	id, err := RegisterAndStartServer(server.Config)
	if err != nil {
		return err
	}
	server.Id = id
	return nil
}

/**
 * 注册rpc endpoint
 * serverInfo:
 *		key: servicePath 服务路径
 *      value: common pointer
 */
func RegisterAndStartServer(config *RpcServerConfig) (id string, err error) {
	if config.Port == "" {
		return "", errors.New("rpc service port is not specified")
	}
	if config.Protocol == "" {
		config.Protocol = Tcp
	}
	s := server.NewServer()
	if log.TracerIsEnabled() {
		//p := serverplugin.OpenTracingPlugin{}
		//s.Plugins.Add(p)
	}
	for k, _ := range config.Info {
		err = s.RegisterName(k, config.Info[k], "")
		if err != nil {
			return
		}
	}
	/*向consul注册当前服务
	id, err = RegisterRpcServer(config)
	if err != nil {
		log.Logger.Error(err)
		return "", err
	}
	log.Logger.Info("register rpc server success")*/
	go func() {
		log.Logger.Info("Rpc server listening on: ", config.Port)
		err = s.Serve(config.Protocol.String(), ":"+config.Port)
		if err != nil {
			log.Logger.Error(err)
			//_ = DeregisterRpcServer(config, id)
			return
		}
	}()
	return
}

//注册rpc服务
func RegisterRpcServer(conf *RpcServerConfig) (string, error) {
	if conf.Discovery == nil {
		return "", nil
	}
	serviceName := conf.ServiceName
	if serviceName == "" {
		return "", errors.New("registered RPC service to consul failure, " +
			"because the ServiceName is empty, please set by DomainOptions.SetServiceName(...)")
	}
	port, err := strconv.ParseInt(conf.Port, 10, 32)
	if err != nil {
		return "", err
	}
	ip := os.Getenv("PODIP")
	if ip == "" {
		ip = "localhost"
	}
	ip = utils.MaybeIpv6(ip)
	s := servicediscovery.Service{
		ID:      serviceName + "_" + ip,
		Name:    serviceName,
		Tags:    []string{config.GetEnvServiceTag(), common.ConsulRpcServiceTag, serviceName},
		Port:    int(port),
		Address: ip,
		Check: servicediscovery.HealthCheck{
			ID:                             serviceName + "_check_" + ip,
			Name:                           serviceName + "_check",
			Interval:                       "10s",
			Timeout:                        "1s",
			TCP:                            fmt.Sprintf("%s:%d", ip, port),
			DeregisterCriticalServiceAfter: "10m",
		},
	}
	err = conf.Discovery.RegisterService(s)
	return s.ID, err
}

func DeregisterRpcServer(conf *RpcServerConfig, id string) error {
	if conf.Discovery == nil {
		return nil
	}
	err := conf.Discovery.DeregisterService(servicediscovery.Service{ID: id})
	if err != nil {
		log.Logger.Error("DeregisterRpcServer failed, " + err.Error())
	}
	log.Logger.Info("DeregisterRpcServer success")
	return err
}

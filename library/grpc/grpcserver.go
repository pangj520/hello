/**
 *@Author: pangj
 *@Description:
 *@File: grpcserver
 *@Version:
 *@Date: 2022/07/11/11:16
 */

package grpc

import (
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"library_Test/library/log"
	"net"
)

type GrpcServer struct {
	Id     string
	Config *GrpcServerConfig
}

type GrpcServerConfig struct {
	ServiceName string
	Port        string
	//Info        map[string]interface{}
	Info     map[*grpc.ServiceDesc]interface{}
	Protocol Protocol
}

type Protocol string

const (
	Tcp  Protocol = "tcp"
	Http Protocol = "http"
	Unix Protocol = "unix"
	Quic Protocol = "quic"
)

//创建rpc服务实例
func NewGrpcServer(config *GrpcServerConfig) *GrpcServer {
	return &GrpcServer{Config: config}
}

//启动
func (server *GrpcServer) Start() error {
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
func RegisterAndStartServer(config *GrpcServerConfig) (id string, err error) {
	if config.Port == "" {
		return "", errors.New("grpc service port is not specified")
	}
	if config.Protocol == "" {
		config.Protocol = Tcp
	}
	var grpcSvc *grpc.Server
	if log.TracerIsEnabled() {
		//TODO
		grpcSvc = grpc.NewServer()
	} else {
		grpcSvc = grpc.NewServer()
	}
	for k, v := range config.Info {
		grpcSvc.RegisterService(k, v)
	}

	go func() {
		listen, err := net.Listen(config.Protocol.String(), ":"+config.Port)
		if err != nil {
			log.Logger.Error(err)
			return
		}
		log.Logger.Info("Rpc server listening on: ", config.Port)
		//TODO 怎样注册
		//reflection.Register(grpcSvc)
		err = grpcSvc.Serve(listen)
		if err != nil {
			log.Logger.Error(err)
			return
		}
	}()
	return
}

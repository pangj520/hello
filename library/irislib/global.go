package irislib

import (
	"errors"
	"fmt"
	"github.com/kataras/iris/v12"
	"library_Test/common"
	"library_Test/common/retcode"
	"library_Test/common/svcinit/domain"
	"library_Test/library/config"
	"library_Test/library/log"
	"library_Test/library/servicediscovery"
	"library_Test/library/utils"
	"os"
	"runtime/debug"
)

//iris app 全局处理函数
type GlobalHandlers func(app *iris.Application)

type HttpService struct {
	Id        string
	Name      string
	Port      int
	Discovery servicediscovery.ServiceDiscovery
}

var (

	//服务异常统一处理
	ErrorCodeGlobalHandler = func(app *iris.Application) {
		app.OnErrorCode(iris.StatusInternalServerError, func(ctx iris.Context) {
			ErrorResponse(ctx, iris.StatusInternalServerError, retcode.SYSTEM_EXTL_ERROR, common.MsgServerError)
			ctx.Next()
		})
	}

	//捕获异常
	ExceptionGlobalHandler = func(app *iris.Application) {
		app.UseGlobal(func(ctx iris.Context) {
			defer func() {
				err := recover()
				if err != nil {
					log.Logger.Error(err, "\n", string(debug.Stack()))
					ctx.StatusCode(iris.StatusInternalServerError)
				}
			}()
			ctx.Next()
		})
	}

	//启用参数验证器
	ValidatorGlobalHandler = func(app *iris.Application) {
		InitValidator()
	}

	//将内部返回码转为外部返回码
	TransformInternalCodeGlobalHandler = func(app *iris.Application) {
		app.UseGlobal(func(ctx iris.Context) {
			ctx.Values().Set(common.IsTransformInternalCode, true)
			ctx.Next()
		})
	}

	//链路追踪器
	JaegerTracerGlobalHandler = func(app *iris.Application) {
		app.UseGlobal(func(ctx iris.Context) {
			operate := ctx.Request().RequestURI
			s, err := log.StartSpanWithParentFromHeader(operate, ctx.Request().Header)
			if err != nil {
				log.Logger.Error(err)
				ctx.StatusCode(500)
				return
			}
			if s != nil {
				ctx.Values().Set(common.KeyJaegerSpan, s)
				defer log.FinishJaegerSpan(s)
			}
			ctx.Next()
		})
	}
)

//iris app 全局处理函数
func SetAppGlobalHandler(app *iris.Application, handlers ...GlobalHandlers) {
	for i := range handlers {
		handlers[i](app)
	}
}

//使用所有全局处理函数
func UseAllGlobalHandler(app *iris.Application) {
	SetAppGlobalHandler(app, JaegerTracerGlobalHandler, ErrorCodeGlobalHandler,
		ExceptionGlobalHandler, ValidatorGlobalHandler, TransformInternalCodeGlobalHandler)
}

//服务健康检查
func ServiceHealthCheck(app *iris.Application) {
	app.Get("/health", func(ctx iris.Context) {
		_, _ = ctx.WriteString("ok")
	})
}

func (s *HttpService) Register(app *iris.Application) error {
	if s.Discovery == nil {
		return nil
	}
	ip := os.Getenv("PODIP")
	if ip == "" {
		ip = "localhost"
	}
	ip = utils.MaybeIpv6(ip)
	serviceName := s.Name
	s.Id = serviceName + "_" + ip + common.ConsulHttpServiceIdSuffix
	ds := servicediscovery.Service{
		ID:      s.Id,
		Name:    serviceName,
		Tags:    []string{config.GetEnvServiceTag(), common.ConsulHttpServiceTag, serviceName},
		Port:    s.Port,
		Address: ip,
		Check: servicediscovery.HealthCheck{
			ID:                             serviceName + "_http_check_" + ip,
			Name:                           serviceName + "_http_check",
			Interval:                       "10s",
			Timeout:                        "1s",
			HTTP:                           fmt.Sprintf("http://%s:%d/health", ip, s.Port),
			DeregisterCriticalServiceAfter: "10m",
		},
	}
	ServiceHealthCheck(app)
	return s.Discovery.RegisterService(ds)
}

func (s *HttpService) Close() error {
	log.Logger.Info("DeregisterHttpService success,id:", s.Id)
	return s.Discovery.DeregisterService(servicediscovery.Service{ID: s.Id})
}

//服务注册
func RegisterHttpService(port int, app *iris.Application) error {
	if domain.DCtx.Options.ServiceName == "" {
		return errors.New("registered http service to consul failure, " +
			"because the ServiceName is empty, please set by DomainOptions.SetServiceName(...)")
	}
	service := &HttpService{
		Name:      domain.DCtx.Options.ServiceName,
		Port:      port,
		Discovery: domain.DCtx.Discovery,
	}
	err := service.Register(app)
	if err != nil {
		return err
	}
	log.Logger.Info("RegisterHttpService success, id:", service.Id)
	domain.DCtx.Closer = append(domain.DCtx.Closer, service)
	return nil
}

package irislib

import (
	"context"
	"github.com/kataras/iris/v12"
	"gotest.tools/assert"
	"strconv"
	"library_Test/common/svcinit/domain"
	"library_Test/library/rpc"
	"testing"
	"time"
)

var (
	consulAddr = ""
)

type TestEndpoint struct {
}

func (*TestEndpoint) Do(ctx context.Context, args *string, resp *string) error {
	return nil
}

func TestRegisterHttpService(t *testing.T) {
	defer domain.Close()
	opts := domain.CreateDomainOptions().SetServiceName("test").WithConfig(
		domain.ConfigOptionCenterType("consul"),
		domain.ConfigOptionConsulAddr(consulAddr)).
		ConfigRpcServer(&rpc.RpcServerConfig{
			Port: "5112",
			Info: map[string]interface{}{
				"/test": &TestEndpoint{},
			},
		})
	_, err := domain.Init(opts)
	assert.NilError(t, err)
	port := 5111
	app := iris.New()
	err = RegisterHttpService(port, app)
	assert.NilError(t, err)
	go func() {
		_ = app.Run(iris.Addr(":" + strconv.FormatInt(int64(port), 10)))
	}()
	time.Sleep(time.Minute)
}

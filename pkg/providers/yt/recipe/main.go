package recipe

import (
	"context"
	"os"
	"testing"

	testcontainers_go "github.com/testcontainers/testcontainers-go"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
)

func Main(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	container, _ := RunContainer(ctx, testcontainers_go.WithImage("ytsaurus/local:stable"))
	proxy, _ := container.ConnectionHost(ctx)
	_ = os.Setenv("YT_PROXY", proxy)
	provider_yt.InitExe()
	res := m.Run()
	_ = container.Terminate(ctx)
	cancel()
	os.Exit(res)
}

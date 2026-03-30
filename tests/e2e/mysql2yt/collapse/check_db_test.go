package mysqltoytcollapse

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	mysql_driver2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
)

const tableName = "test"

var (
	source        = *helpers.WithMysqlInclude(helpers.RecipeMysqlSource(), []string{tableName})
	targetCluster = os.Getenv("YT_PROXY")
)

func init() {
	source.WithDefaults()
}

func makeConnConfig() *mysql_driver2.Config {
	cfg := mysql_driver2.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"
	return cfg
}

func makeTarget() provider_yt.YtDestinationModel {
	target := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Path:          "//home/cdc/test/mysql2yt/collapse",
		Cluster:       targetCluster,
		CellBundle:    "default",
		PrimaryMedium: "default",
	})
	target.WithDefaults()
	return target
}

func TestCollapse(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(targetCluster)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	ytDestination := makeTarget()
	transfer := model.Transfer{
		ID:  "collapse_test",
		Src: &source,
		Dst: ytDestination,
	}

	fakeClient := coordinator.NewStatefulFakeClient()
	err = provider_mysql.SyncBinlogPosition(&source, transfer.ID, fakeClient)
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(fakeClient, &transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	defer localWorker.Stop() //nolint

	conn, err := mysql_driver2.NewConnector(makeConnConfig())
	require.NoError(t, err)

	requests := []string{
		"insert into test (id, value) values(1, 'aaa');",
		"delete from test where id = 1;",
		"insert into test (id, value) values(1, 'bbb');",
	}

	db := sql.OpenDB(conn)
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	for _, request := range requests {
		_, err := tx.Query(request)
		require.NoError(t, err)
	}
	err = tx.Commit()
	require.NoError(t, err)

	require.NoError(t, helpers.WaitEqualRowsCount(t, source.Database, tableName, helpers.GetSampleableStorageByModel(t, source), helpers.GetSampleableStorageByModel(t, ytDestination.LegacyModel()), 60*time.Second))
	require.NoError(t, helpers.CompareStorages(t, source, ytDestination.LegacyModel(), helpers.NewCompareStorageParams()))
}

// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databricksreceiver

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/signalfx/splunk-otel-collector/internal/receiver/databricksreceiver/internal/spark"
)

func xTestSparkService_Integration(t *testing.T) {
	logger := zap.NewNop()
	dbcl := newDatabricksClient(
		"https://adb-4429673989716691.11.azuredatabricks.net",
		"dapi29cab1150cc2f1eb365120abbb95d8da",
		http.DefaultClient,
		logger,
	)
	dbsvc := newDatabricksService(dbcl, 25)
	ssvc := newSparkService(
		logger,
		dbsvc,
		http.DefaultClient,
		"https://westus.azuredatabricks.net",
		40001,
		"4429673989716691",
		"dapi29cab1150cc2f1eb365120abbb95d8da",
		newSparkClient,
	)
	clusters, err := dbsvc.runningClusters()
	require.NoError(t, err)
	clusterMetrics, err := ssvc.getSparkMetricsForClusters(clusters)
	require.NoError(t, err)
	for clusterMetric, _ := range clusterMetrics {
		execInfo, err := ssvc.getSparkExecutorInfoSliceByApp(clusterMetric.ClusterId)
		require.NoError(t, err)
		fmt.Printf("execInfo: ->%v<-\n", execInfo)
	}
}

func newTestSparkService() sparkRestService {
	return sparkRestService{
		logger: zap.New(zapcore.NewNopCore()),
		dbsvc:  newTestDatabricksService(),
		sparkClientFactory: func(*zap.Logger, *http.Client, string, string, int, string, string) spark.Client {
			return spark.Client{
				RawClient: testdataSparkClusterClient{},
			}
		},
	}
}

type testdataSparkClusterClient struct{}

func (c testdataSparkClusterClient) Metrics() ([]byte, error) {
	return os.ReadFile(filepath.Join("testdata", "spark", "metrics.json"))
}

func (c testdataSparkClusterClient) Applications() ([]byte, error) {
	return os.ReadFile(filepath.Join("testdata", "spark", "applications.json"))
}

func (c testdataSparkClusterClient) AppExecutors(string) ([]byte, error) {
	return os.ReadFile(filepath.Join("testdata", "spark", "executors.json"))
}

func (c testdataSparkClusterClient) AppJobs(s string) ([]byte, error) {
	return os.ReadFile(filepath.Join("testdata", "spark", "jobs.json"))
}

func (c testdataSparkClusterClient) AppStages(s string) ([]byte, error) {
	return os.ReadFile(filepath.Join("testdata", "spark", "stages.json"))
}

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

	"go.uber.org/zap"

	"github.com/signalfx/splunk-otel-collector/internal/receiver/databricksreceiver/internal/httpauth"
)

const (
	jobsListPath         = "/api/2.1/jobs/list?expand_tasks=true&limit=%d&offset=%d"
	activeJobRunsPath    = "/api/2.1/jobs/runs/list?active_only=true&limit=%d&offset=%d"
	completedJobRunsPath = "/api/2.1/jobs/runs/list?completed_only=true&expand_tasks=true&job_id=%d&limit=%d&offset=%d"
	clustersListPath     = "/api/2.0/clusters/list"
	pipelinesPath        = "/api/2.0/pipelines"
	pipelinePath         = "/api/2.0/pipelines/%s"
)

// databricksRawClientIntf is extracted from databricksRawClient so that it can be swapped for
// testing.
type databricksRawClientIntf interface {
	jobsList(limit int, offset int) ([]byte, error)
	activeJobRuns(limit int, offset int) ([]byte, error)
	completedJobRuns(id int, limit int, offset int) ([]byte, error)
	clustersList() ([]byte, error)
	pipelines() ([]byte, error)
	pipeline(string) ([]byte, error)
}

// databricksRawClient wraps an authClient, encapsulates calls to the databricks API, and
// implements databricksRawClientIntf. Its methods return byte arrays to be unmarshalled
// by the caller.
type databricksRawClient struct {
	logger     *zap.Logger
	authClient httpauth.ClientIntf
}

func newDatabricksClient(endpoint string, tok string, httpClient *http.Client, logger *zap.Logger) databricksRawClientIntf {
	return &databricksRawClient{
		authClient: httpauth.NewClient(httpClient, endpoint, tok),
		logger:     logger,
	}
}

func (c databricksRawClient) jobsList(limit int, offset int) (out []byte, err error) {
	path := fmt.Sprintf(jobsListPath, limit, offset)
	c.logger.Debug("databricksRawClient.jobsList", zap.String("path", path))
	return c.authClient.Get(path)
}

func (c databricksRawClient) activeJobRuns(limit int, offset int) ([]byte, error) {
	path := fmt.Sprintf(activeJobRunsPath, limit, offset)
	c.logger.Debug("databricksRawClient.activeJobRuns", zap.String("path", path))
	return c.authClient.Get(path)
}

func (c databricksRawClient) completedJobRuns(jobID int, limit int, offset int) ([]byte, error) {
	path := fmt.Sprintf(completedJobRunsPath, jobID, limit, offset)
	c.logger.Debug("databricksRawClient.completedJobRuns", zap.String("path", path))
	return c.authClient.Get(path)
}

func (c databricksRawClient) clustersList() ([]byte, error) {
	c.logger.Debug("databricksRawClient.clustersList", zap.String("path", clustersListPath))
	return c.authClient.Get(clustersListPath)
}

func (c databricksRawClient) pipelines() ([]byte, error) {
	c.logger.Debug("databricksRawClient.pipelines", zap.String("path", pipelinesPath))
	return c.authClient.Get(pipelinesPath)
}

func (c databricksRawClient) pipeline(s string) ([]byte, error) {
	path := fmt.Sprintf(pipelinePath, s)
	c.logger.Debug("databricksRawClient.pipeline", zap.String("path", path))
	return c.authClient.Get(path)
}

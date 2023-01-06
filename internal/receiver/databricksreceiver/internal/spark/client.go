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

package spark

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/signalfx/splunk-otel-collector/internal/receiver/databricksreceiver/internal/httpauth"
)

type Client struct {
	RawClient RawClientIntf
}

func NewClient(httpClient *http.Client, endpoint string, tok string) Client {
	return Client{
		RawClient: rawClient{
			authClient: httpauth.NewClient(httpClient, endpoint, tok),
		},
	}
}

func (c Client) Metrics() (ClusterMetrics, error) {
	cm := ClusterMetrics{}
	bytes, err := c.RawClient.Metrics()
	if err != nil {
		return cm, fmt.Errorf("failed to get metrics from spark: %w", err)
	}
	err = json.Unmarshal(bytes, &cm)
	if err != nil {
		return cm, fmt.Errorf("failed to unmarshal spark metrics: %w", err)
	}
	return cm, nil
}

func (c Client) Applications() ([]Application, error) {
	var apps []Application
	bytes, err := c.RawClient.Applications()
	if err != nil {
		return apps, fmt.Errorf("failed to get applications from spark: %w", err)
	}
	err = json.Unmarshal(bytes, &apps)
	if err != nil {
		return apps, fmt.Errorf("failed to unmarshal spark applications: %w", err)
	}
	return apps, nil
}

func (c Client) AppExecutors(appID string) ([]ExecutorInfo, error) {
	bytes, err := c.RawClient.AppExecutors(appID)
	if err != nil {
		return nil, fmt.Errorf("failed to get app executors from spark: %w", err)
	}
	var ei []ExecutorInfo
	err = json.Unmarshal(bytes, &ei)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal executor info: %w", err)
	}
	return ei, nil
}

func (c Client) AppJobs(appID string) ([]JobInfo, error) {
	bytes, err := c.RawClient.AppJobs(appID)
	if err != nil {
		return nil, fmt.Errorf("failed to get jobs from spark: %w", err)
	}
	var jobs []JobInfo
	err = json.Unmarshal(bytes, &jobs)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job info: %w", err)
	}
	return jobs, nil
}

func (c Client) AppStages(appID string) ([]StageInfo, error) {
	bytes, err := c.RawClient.AppStages(appID)
	if err != nil {
		return nil, fmt.Errorf("failed to get jobs from spark: %w", err)
	}
	var stages []StageInfo
	err = json.Unmarshal(bytes, &stages)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal job info: %w", err)
	}
	return stages, nil
}

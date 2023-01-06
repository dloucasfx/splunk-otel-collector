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
	"fmt"

	"github.com/signalfx/splunk-otel-collector/internal/receiver/databricksreceiver/internal/httpauth"
)

const (
	metricsPath         = "/metrics/json"
	applicationsPath    = "/api/v1/applications"
	appExecutorsPathFmt = applicationsPath + "/%s/executors"
	appJobsPathFmt      = applicationsPath + "/%s/jobs"
	appStagesPathFmt    = applicationsPath + "/%s/stages"
)

type RawClientIntf interface {
	Metrics() ([]byte, error)
	Applications() ([]byte, error)
	AppExecutors(string) ([]byte, error)
	AppJobs(string) ([]byte, error)
	AppStages(string) ([]byte, error)
}

type rawClient struct {
	authClient httpauth.ClientIntf
}

func (c rawClient) Metrics() ([]byte, error) {
	return c.authClient.Get(metricsPath)
}

func (c rawClient) Applications() ([]byte, error) {
	return c.authClient.Get(applicationsPath)
}

func (c rawClient) AppExecutors(appID string) ([]byte, error) {
	return c.authClient.Get(fmt.Sprintf(appExecutorsPathFmt, appID))
}

func (c rawClient) AppJobs(appID string) ([]byte, error) {
	return c.authClient.Get(fmt.Sprintf(appJobsPathFmt, appID))
}

func (c rawClient) AppStages(appID string) ([]byte, error) {
	return c.authClient.Get(fmt.Sprintf(appStagesPathFmt, appID))
}

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

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/signalfx/splunk-otel-collector/internal/receiver/databricksreceiver/internal/metadata"
)

type sparkExtraMetricsBuilder struct {
	ssvc sparkService
}

func (b sparkExtraMetricsBuilder) buildExecutorMetrics(builder *metadata.MetricsBuilder, now pcommon.Timestamp, clusters []cluster) error {
	for _, clstr := range clusters {
		execInfosByApp, err := b.ssvc.getSparkExecutorInfoSliceByApp(clstr.ClusterId)
		if err != nil {
			return fmt.Errorf("failed to get executor info for cluster: %s: %w", clstr.ClusterId, err)
		}
		for sparkApp, execInfos := range execInfosByApp {
			for _, execInfo := range execInfos {
				builder.RecordDatabricksSparkExecutorMemoryUsedDataPoint(now, int64(execInfo.MemoryUsed), clstr.ClusterId, sparkApp.Id, execInfo.Id)
				builder.RecordDatabricksSparkExecutorDiskUsedDataPoint(now, int64(execInfo.DiskUsed), clstr.ClusterId, sparkApp.Id, execInfo.Id)
				builder.RecordDatabricksSparkExecutorTotalInputBytesDataPoint(now, execInfo.TotalInputBytes, clstr.ClusterId, sparkApp.Id, execInfo.Id)
				builder.RecordDatabricksSparkExecutorTotalShuffleReadDataPoint(now, int64(execInfo.TotalShuffleRead), clstr.ClusterId, sparkApp.Id, execInfo.Id)
				builder.RecordDatabricksSparkExecutorTotalShuffleWriteDataPoint(now, int64(execInfo.TotalShuffleWrite), clstr.ClusterId, sparkApp.Id, execInfo.Id)
				builder.RecordDatabricksSparkExecutorMaxMemoryDataPoint(now, execInfo.MaxMemory, clstr.ClusterId, sparkApp.Id, execInfo.Id)
			}
		}
	}
	return nil
}

func (b sparkExtraMetricsBuilder) buildJobMetrics(builder *metadata.MetricsBuilder, now pcommon.Timestamp, clusters []cluster) error {
	for _, clstr := range clusters {
		jobInfosByApp, err := b.ssvc.getSparkJobInfoSliceByApp(clstr.ClusterId)
		if err != nil {
			return fmt.Errorf("failed to get jobs for cluster: %s: %w", clstr.ClusterId, err)
		}
		for sparkApp, jobInfos := range jobInfosByApp {
			for _, jobInfo := range jobInfos {
				builder.RecordDatabricksSparkJobNumTasksDataPoint(now, int64(jobInfo.NumTasks), clstr.ClusterId, sparkApp.Id, int64(jobInfo.JobId))
				builder.RecordDatabricksSparkJobNumActiveTasksDataPoint(now, int64(jobInfo.NumActiveTasks), clstr.ClusterId, sparkApp.Id, int64(jobInfo.JobId))
				builder.RecordDatabricksSparkJobNumCompletedTasksDataPoint(now, int64(jobInfo.NumCompletedTasks), clstr.ClusterId, sparkApp.Id, int64(jobInfo.JobId))
				builder.RecordDatabricksSparkJobNumSkippedTasksDataPoint(now, int64(jobInfo.NumSkippedTasks), clstr.ClusterId, sparkApp.Id, int64(jobInfo.JobId))
				builder.RecordDatabricksSparkJobNumFailedTasksDataPoint(now, int64(jobInfo.NumFailedTasks), clstr.ClusterId, sparkApp.Id, int64(jobInfo.JobId))
				builder.RecordDatabricksSparkJobNumActiveStagesDataPoint(now, int64(jobInfo.NumActiveStages), clstr.ClusterId, sparkApp.Id, int64(jobInfo.JobId))
				builder.RecordDatabricksSparkJobNumCompletedStagesDataPoint(now, int64(jobInfo.NumCompletedStages), clstr.ClusterId, sparkApp.Id, int64(jobInfo.JobId))
				builder.RecordDatabricksSparkJobNumSkippedStagesDataPoint(now, int64(jobInfo.NumSkippedStages), clstr.ClusterId, sparkApp.Id, int64(jobInfo.JobId))
				builder.RecordDatabricksSparkJobNumFailedStagesDataPoint(now, int64(jobInfo.NumFailedStages), clstr.ClusterId, sparkApp.Id, int64(jobInfo.JobId))
			}
		}
	}
	return nil
}

func (b sparkExtraMetricsBuilder) buildStageMetrics(builder *metadata.MetricsBuilder, now pcommon.Timestamp, clusters []cluster) error {
	for _, clstr := range clusters {
		stageInfosByApp, err := b.ssvc.getSparkStageInfoSliceByApp(clstr.ClusterId)
		if err != nil {
			return fmt.Errorf("failed to get stages for cluster: %s: %w", clstr.ClusterId, err)
		}
		for sparkApp, stageInfos := range stageInfosByApp {
			for _, stageInfo := range stageInfos {
				builder.RecordDatabricksSparkStageExecutorRunTimeDataPoint(now, int64(stageInfo.ExecutorRunTime), clstr.ClusterId, sparkApp.Id, int64(stageInfo.StageId))
				builder.RecordDatabricksSparkStageInputBytesDataPoint(now, int64(stageInfo.InputBytes), clstr.ClusterId, sparkApp.Id, int64(stageInfo.StageId))
				builder.RecordDatabricksSparkStageInputRecordsDataPoint(now, int64(stageInfo.InputRecords), clstr.ClusterId, sparkApp.Id, int64(stageInfo.StageId))
				builder.RecordDatabricksSparkStageOutputBytesDataPoint(now, int64(stageInfo.OutputBytes), clstr.ClusterId, sparkApp.Id, int64(stageInfo.StageId))
				builder.RecordDatabricksSparkStageOutputRecordsDataPoint(now, int64(stageInfo.OutputRecords), clstr.ClusterId, sparkApp.Id, int64(stageInfo.StageId))
				builder.RecordDatabricksSparkStageMemoryBytesSpilledDataPoint(now, int64(stageInfo.MemoryBytesSpilled), clstr.ClusterId, sparkApp.Id, int64(stageInfo.StageId))
				builder.RecordDatabricksSparkStageDiskBytesSpilledDataPoint(now, int64(stageInfo.DiskBytesSpilled), clstr.ClusterId, sparkApp.Id, int64(stageInfo.StageId))
			}
		}
	}
	return nil
}

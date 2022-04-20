// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package metrics

import (
	"testing"
	"time"

	prometheus "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func GetCounterValue(metric *prometheus.CounterVec, labelValues ...string) (float64, error) {
	var m = &dto.Metric{}
	if err := metric.WithLabelValues(labelValues...).Write(m); err != nil {
		return 0.0, err
	}
	return m.Counter.GetValue(), nil
}

func GetHistogramValue(metric *prometheus.HistogramVec, labelValues ...string) (uint64, error) {
	var m = &dto.Metric{}
	if err := metric.WithLabelValues(labelValues...).(prometheus.Histogram).Write(m); err != nil {
		return 0.0, err
	}
	return m.GetHistogram().GetSampleCount(), nil
}

func training(obs TrainingDataObserver, promMetrics PromMetricsHandler, start time.Time, num int, errors int) {
	for i := 0; i < errors; i++ {
		obs.SetError()
	}

	for i := 0; i < num; i++ {
		obs.ServeRow()
	}

	obs.Finish()
}

func serving(obs PromFeatureObserver, promMetrics PromMetricsHandler, start time.Time, num int, errors int) {
	for i := 0; i < errors; i++ {
		obs.SetError()
	}
	for i := 0; i < num; i++ {
		obs.ServeRow()
	}
	obs.Finish()
}

func TestMetrics(t *testing.T) {
	start := time.Now()
	instanceName := "test"
	promMetrics := NewMetrics(instanceName)
	featureName := "example_feature"
	featureVariant := "example_variant"
	trainingDatasetName := "example_dataset"
	trainingDatasetVariant := "example_variant"

	servingNum := 5
	servingErrorNum := 5
	trainingNum := 5
	trainingErrorNum := 5
	latencyServingCount := servingNum + 1
	latencyTrainingCount := trainingNum + 1

	servingObserver := promMetrics.BeginObservingOnlineServe(featureName, featureVariant).(PromFeatureObserver)
	trainingObserver := promMetrics.BeginObservingTrainingServe(trainingDatasetName, trainingDatasetVariant).(TrainingDataObserver)
	serving(servingObserver, promMetrics, start, servingNum, servingErrorNum)
	training(trainingObserver, promMetrics, start, trainingNum, trainingErrorNum)

	servingCounterValue, err := GetCounterValue(servingObserver.Count, instanceName, featureName, featureVariant, string(ONLINE_ROW_SERVE))
	if err != nil {
		t.Fatalf("Could not fetch value: %v", err)
	}
	assert.Equal(t, int(servingCounterValue), servingNum, "5 feature rows should be served")
	servingCounterValueInt, err := servingObserver.GetObservedRowCount()
	if err != nil {
		t.Fatalf("Could not fetch value: %v", err)
	}
	assert.Equal(t, servingCounterValueInt, servingNum, "5 feature rows should be served")
	servingErrorCounterValue, err := GetCounterValue(servingObserver.Count, instanceName, featureName, featureVariant, string(ERROR))
	if err != nil {
		t.Fatalf("Could not fetch value: %v", err)
	}
	assert.Equal(t, int(servingErrorCounterValue), servingErrorNum, "5 feature error rows should be recorded")
	servingErrorCounterValueInt, err := servingObserver.GetObservedErrorCount()
	if err != nil {
		t.Fatalf("Could not fetch value: %v", err)
	}
	assert.Equal(t, servingErrorCounterValueInt, servingNum, "5 feature error rows should be recorded")
	trainingCounterValue, err := GetCounterValue(trainingObserver.Row_Count, instanceName, trainingDatasetName, trainingDatasetVariant, string(TRAINING_ROW_SERVE))
	if err != nil {
		t.Fatalf("Could not fetch value: %v", err)
	}
	assert.Equal(t, int(trainingCounterValue), trainingNum, "5 training data rows should be recorded")
	trainingCounterValueInt, err := trainingObserver.GetObservedRowCount()
	if err != nil {
		t.Fatalf("Could not fetch value: %v", err)
	}
	assert.Equal(t, trainingCounterValueInt, trainingNum, "5 training data rows should be recorded")
	trainingErrorCounterValue, err := GetCounterValue(trainingObserver.Row_Count, instanceName, trainingDatasetName, trainingDatasetVariant, string(ERROR))
	if err != nil {
		t.Fatalf("Could not fetch value: %v", err)
	}
	assert.Equal(t, int(trainingErrorCounterValue), trainingErrorNum, "5 training data errors should be recorded")
	trainingErrorCounterValueInt, err := trainingObserver.GetObservedErrorCount()
	if err != nil {
		t.Fatalf("Could not fetch value: %v", err)
	}
	assert.Equal(t, trainingErrorCounterValueInt, trainingNum, "5 training data errors should be recorded")
	latencyCounterValue, err := GetHistogramValue(promMetrics.Hist, instanceName, featureName, featureVariant, "")
	if err != nil {
		t.Fatalf("Could not fetch value: %v", err)
	}
	assert.Equal(t, int(latencyCounterValue), latencyServingCount, "Feature latency records 6 events")
	latencyTrainingCounterValue, err := GetHistogramValue(promMetrics.Hist, instanceName, trainingDatasetName, trainingDatasetVariant, "")
	if err != nil {
		t.Fatalf("Could not fetch value: %v", err)
	}
	assert.Equal(t, int(latencyTrainingCounterValue), latencyTrainingCount, "Training latency records 6 events")

}

package config

import "github.com/featureform/helpers"

// image paths
const (
	PandasBaseImage = "featureformcom/k8s_runner"
	WorkerImage     = "featureformcom/worker"
)

// script paths
const (
	SparkLocalScriptPath              = "/app/provider/scripts/spark/offline_store_spark_runner.py"
	SparkRemoteScriptPath             = "featureform/scripts/spark/offline_store_spark_runner.py"
	PythonLocalInitPath               = "/app/provider/scripts/spark/python_packages.sh"
	PythonRemoteInitPath              = "featureform/scripts/spark/python_packages.sh"
	MaterializeNoTimestampQueryPath   = "/app/provider/queries/materialize_no_ts.sql"
	MaterializeWithTimestampQueryPath = "/app/provider/queries/materialize_ts.sql"
)

func GetWorkerImage() string {
	return helpers.GetEnv("WORKER_IMAGE", WorkerImage)
}

func GetPandasRunnerImage() string {
	return helpers.GetEnv("PANDAS_RUNNER_IMAGE", PandasBaseImage)
}

func GetSparkLocalScriptPath() string {
	return helpers.GetEnv("SPARK_LOCAL_SCRIPT_PATH", SparkLocalScriptPath)
}

func GetSparkRemoteScriptPath() string {
	return helpers.GetEnv("SPARK_REMOTE_SCRIPT_PATH", SparkRemoteScriptPath)
}

func GetPythonLocalInitPath() string {
	return helpers.GetEnv("PYTHON_LOCAL_INIT_PATH", PythonLocalInitPath)
}

func GetPythonRemoteInitPath() string {
	return helpers.GetEnv("PYTHON_REMOTE_INIT_PATH", PythonRemoteInitPath)
}

func GetMaterializeNoTimestampQueryPath() string {
	return helpers.GetEnv("MATERIALIZE_NO_TIMESTAMP_QUERY_PATH", MaterializeNoTimestampQueryPath)
}

func GetMaterializeWithTimestampQueryPath() string {
	return helpers.GetEnv("MATERIALIZE_WITH_TIMESTAMP_QUERY_PATH", MaterializeWithTimestampQueryPath)
}

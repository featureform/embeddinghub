// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"github.com/featureform/helpers"
	"github.com/featureform/metadata"
	"github.com/featureform/types"
	"github.com/google/uuid"
	"io"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	kubernetes "k8s.io/client-go/kubernetes"
	rest "k8s.io/client-go/rest"
	"math"
	"os"
	"strings"
)

type CronSchedule string

const MaxJobNameLength = 52

// CreateJobName Only the first value in prefixes will be used.
func CreateJobName(id metadata.ResourceID, prefixes ...string) string {
	jobNameBase := fmt.Sprintf("%s-%s-%s", id.Type, id.Name, id.Variant)

	// if jobPrefix is provided, prepend it to jobNameBase
	if len(prefixes) > 0 && prefixes[0] != "" {
		jobNameBase = fmt.Sprintf("%s-%s", prefixes[0], jobNameBase)
	}

	// clean up job name for k8s
	replacer := strings.NewReplacer("_", ".", "/", "", ":", "")
	jobNameBase = replacer.Replace(jobNameBase)

	lowerCased := strings.ToLower(jobNameBase)

	// leave room for a 10 character uuid and a 1 character separator
	if len(lowerCased) > MaxJobNameLength-11 {
		lowerCased = lowerCased[:MaxJobNameLength-11]
	}

	return fmt.Sprintf("%s-%s", lowerCased, uuid.New().String()[:10])
}

type CronRunner interface {
	types.Runner
	ScheduleJob(schedule CronSchedule) error
}

func generateKubernetesEnvVars(envVars map[string]string) []v1.EnvVar {
	kubeEnvVars := make([]v1.EnvVar, len(envVars))
	i := 0
	for key, element := range envVars {
		kubeEnvVars[i] = v1.EnvVar{Name: key, Value: element}
		i++
	}
	return kubeEnvVars
}

func validateJobLimits(specs metadata.KubernetesResourceSpecs) (v1.ResourceRequirements, error) {
	rsrcReq := v1.ResourceRequirements{
		Requests: make(v1.ResourceList),
		Limits:   make(v1.ResourceList),
	}
	var parseErr error
	if specs.CPURequest != "" {
		qty, err := resource.ParseQuantity(specs.CPURequest)
		rsrcReq.Requests[v1.ResourceCPU] = qty
		parseErr = err
	}
	if specs.CPULimit != "" {
		qty, err := resource.ParseQuantity(specs.CPULimit)
		rsrcReq.Limits[v1.ResourceCPU] = qty
		parseErr = err
	}
	if specs.MemoryRequest != "" {
		qty, err := resource.ParseQuantity(specs.MemoryRequest)
		rsrcReq.Requests[v1.ResourceMemory] = qty
		parseErr = err
	}
	if specs.MemoryLimit != "" {
		qty, err := resource.ParseQuantity(specs.MemoryLimit)
		rsrcReq.Limits[v1.ResourceMemory] = qty
		parseErr = err
	}
	if parseErr != nil {
		return rsrcReq, parseErr
	}
	return rsrcReq, nil
}

func newJobSpec(config KubernetesRunnerConfig, rsrcReqs v1.ResourceRequirements) batchv1.JobSpec {
	containerID := uuid.New().String()
	envVars := generateKubernetesEnvVars(config.EnvVars)
	//only indexed completion if copyRunner
	var completionMode batchv1.CompletionMode
	if config.EnvVars["Name"] == "Copy to online" {
		completionMode = batchv1.IndexedCompletion
	} else {
		completionMode = batchv1.NonIndexedCompletion
	}

	backoffLimit := helpers.GetEnvInt32("K8S_JOB_BACKOFF_LIMIT", 0)
	ttlLimitSeconds := helpers.GetEnvInt32("K8S_JOB_TTL_LIMIT_SECONDS", 60)

	var pullPolicy v1.PullPolicy
	if helpers.IsDebugEnv() {
		pullPolicy = v1.PullAlways
	} else {
		pullPolicy = v1.PullIfNotPresent
	}

	return batchv1.JobSpec{
		Completions:             &config.NumTasks,
		Parallelism:             &config.NumTasks,
		CompletionMode:          &completionMode,
		BackoffLimit:            &backoffLimit,
		TTLSecondsAfterFinished: &ttlLimitSeconds,
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:            containerID,
						Image:           config.Image,
						Env:             envVars,
						ImagePullPolicy: pullPolicy,
						Resources:       rsrcReqs,
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
			},
		},
	}

}

type KubernetesRunnerConfig struct {
	JobPrefix string
	EnvVars   map[string]string
	Resource  metadata.ResourceID
	Image     string
	NumTasks  int32
	Specs     metadata.KubernetesResourceSpecs
}

type JobClient interface {
	GetJobName() string
	Get() (*batchv1.Job, error)
	GetCronJob() (*batchv1.CronJob, error)
	UpdateCronJob(cronJob *batchv1.CronJob) (*batchv1.CronJob, error)
	Watch() (watch.Interface, error)
	Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error)
	SetJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error
	GetJobSchedule(jobName string) (CronSchedule, error)
}

type KubernetesRunner struct {
	jobClient JobClient
	jobSpec   *batchv1.JobSpec
}

type KubernetesCompletionWatcher struct {
	jobClient JobClient
}

func (k KubernetesCompletionWatcher) Complete() bool {
	job, err := k.jobClient.Get()
	if err != nil {
		return false
	}
	if job.Status.Active == 0 && job.Status.Failed == 0 {
		return true
	}
	return false
}

func (k KubernetesCompletionWatcher) String() string {
	job, err := k.jobClient.Get()
	if err != nil {
		return "Could not fetch job."
	}
	return fmt.Sprintf("%d jobs succeeded. %d jobs active. %d jobs failed", job.Status.Succeeded, job.Status.Active, job.Status.Failed)
}

func getPodLogs(namespace string, name string) string {
	podLogOpts := corev1.PodLogOptions{}
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Sprintf("error in getting config, %s", err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Sprintf("error in getting access to K8S: %s", err.Error())
	}
	podList, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Sprintf("could not get pod list: %s", err.Error())
	}
	podName := ""
	for _, pod := range podList.Items {
		currentPod := pod.GetName()
		if strings.Contains(currentPod, name) {
			podName = currentPod
		}
	}
	if podName == "" {
		return fmt.Sprintf("pod not found: %s", name)
	}
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &podLogOpts)
	podLogs, err := req.Stream(context.Background())
	if err != nil {
		return fmt.Sprintf("error in opening stream: %s", err.Error())
	}
	defer podLogs.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, podLogs)
	if err != nil {
		return fmt.Sprintf("error in copy information from podLogs to buf: %s", err.Error())
	}
	str := buf.String()

	return str
}

func (k KubernetesCompletionWatcher) Wait() error {
	watcher, err := k.jobClient.Watch()
	if err != nil {
		fmt.Println("error fetching watcher for job:", k.jobClient.GetJobName())
		return err
	}
	watchChannel := watcher.ResultChan()
	for jobEvent := range watchChannel {

		job := jobEvent.Object.(*batchv1.Job)
		if active := job.Status.Active; active == 0 {
			if succeeded := job.Status.Succeeded; succeeded > 0 {
				return nil
			}
			if failed := job.Status.Failed; failed > 0 {
				return fmt.Errorf("job failed while running: container: %s: error: %s",
					job.Name, getPodLogs(job.Namespace, job.GetName()))
			}
		}

	}
	return nil
}

func (k KubernetesCompletionWatcher) Err() error {
	job, err := k.jobClient.Get()
	if err != nil {
		return err
	}
	if job.Status.Failed > 0 {
		return fmt.Errorf("job failed while running: container: %s: %w", job.Name, err)
	}
	return nil
}

func (k KubernetesRunner) Resource() metadata.ResourceID {
	return metadata.ResourceID{}
}

func (k KubernetesRunner) IsUpdateJob() bool {
	return false
}

func (k KubernetesRunner) Run() (types.CompletionWatcher, error) {
	if _, err := k.jobClient.Create(k.jobSpec); err != nil {
		return nil, err
	}
	return KubernetesCompletionWatcher{jobClient: k.jobClient}, nil
}

func (k KubernetesRunner) ScheduleJob(schedule CronSchedule) error {
	if err := k.jobClient.SetJobSchedule(schedule, k.jobSpec); err != nil {
		return err
	}
	return nil
}

func GetCurrentNamespace() (string, error) {
	contents, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

func generateCleanRandomJobName() string {
	cleanUUID := strings.ReplaceAll(uuid.New().String(), "-", "")
	jobName := fmt.Sprintf("job__%s", cleanUUID)
	return jobName[0:int(math.Min(float64(len(jobName)), 63))]
}

func NewKubernetesRunner(config KubernetesRunnerConfig) (CronRunner, error) {
	rsrcReqs, err := validateJobLimits(config.Specs)
	if err != nil {
		return nil, err
	}
	jobSpec := newJobSpec(config, rsrcReqs)
	var jobName string
	if config.Resource.Name != "" {
		jobName = CreateJobName(config.Resource, config.JobPrefix)
	} else {
		jobName = generateCleanRandomJobName()
	}
	namespace, err := GetCurrentNamespace()
	if err != nil {
		return nil, err
	}
	jobClient, err := NewKubernetesJobClient(jobName, namespace)
	if err != nil {
		return nil, err
	}
	return KubernetesRunner{
		jobClient: jobClient,
		jobSpec:   &jobSpec,
	}, nil
}

type KubernetesJobClient struct {
	Clientset *kubernetes.Clientset
	JobName   string
	Namespace string
}

func (k KubernetesJobClient) GetJobName() string {
	return k.JobName
}

func (k KubernetesJobClient) getCronJobName() string {
	return fmt.Sprintf("cron-%s", k.JobName)
}

func (k KubernetesJobClient) Get() (*batchv1.Job, error) {
	return k.Clientset.BatchV1().Jobs(k.Namespace).Get(context.TODO(), k.JobName, metav1.GetOptions{})
}

func (k KubernetesJobClient) GetCronJob() (*batchv1.CronJob, error) {
	return k.Clientset.BatchV1().CronJobs(k.Namespace).Get(context.TODO(), k.getCronJobName(), metav1.GetOptions{})
}

func (k KubernetesJobClient) UpdateCronJob(cronJob *batchv1.CronJob) (*batchv1.CronJob, error) {
	return k.Clientset.BatchV1().CronJobs(k.Namespace).Update(context.TODO(), cronJob, metav1.UpdateOptions{})
}

func (k KubernetesJobClient) Watch() (watch.Interface, error) {
	return k.Clientset.BatchV1().Jobs(k.Namespace).Watch(context.TODO(), metav1.ListOptions{FieldSelector: fmt.Sprintf("metadata.name=%s", k.JobName)})
}

func (k KubernetesJobClient) Create(jobSpec *batchv1.JobSpec) (*batchv1.Job, error) {
	fmt.Println("Creating kubernetes job with name:", k.JobName)
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: k.JobName, Namespace: k.Namespace}, Spec: *jobSpec}
	return k.Clientset.BatchV1().Jobs(k.Namespace).Create(context.TODO(), job, metav1.CreateOptions{})
}

func (k KubernetesJobClient) SetJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error {
	successfulJobsHistoryLimit := helpers.GetEnvInt32("SUCCESSFUL_JOBS_HISTORY_LIMIT", 2)
	failedJobsHistoryLimit := helpers.GetEnvInt32("FAILED_JOBS_HISTORY_LIMIT", 1)
	concurrencyPolicyEnv := helpers.GetEnv("JOBS_CONCURRENCY_POLICY", "Allow")
	concurrencyPolicy := getConcurrencyPolicy(concurrencyPolicyEnv)

	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.getCronJobName(),
			Namespace: k.Namespace},
		Spec: batchv1.CronJobSpec{
			Schedule: string(schedule),
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: *jobSpec,
			},
			SuccessfulJobsHistoryLimit: &successfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     &failedJobsHistoryLimit,
			ConcurrencyPolicy:          concurrencyPolicy,
		},
	}
	if _, err := k.Clientset.BatchV1().CronJobs(k.Namespace).Create(context.TODO(), cronJob, metav1.CreateOptions{}); err != nil {
		return err
	}
	return nil
}

func (k KubernetesJobClient) UpdateJobSchedule(schedule CronSchedule, jobSpec *batchv1.JobSpec) error {
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.JobName,
			Namespace: k.Namespace,
		},
		Spec: batchv1.CronJobSpec{
			Schedule: string(schedule),
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: *jobSpec,
			},
		},
	}
	if _, err := k.Clientset.BatchV1().CronJobs(k.Namespace).Update(context.TODO(), cronJob, metav1.UpdateOptions{}); err != nil {
		return err
	}
	return nil
}

func (k KubernetesJobClient) GetJobSchedule(jobName string) (CronSchedule, error) {
	job, err := k.Clientset.BatchV1().CronJobs(k.Namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return CronSchedule(job.Spec.Schedule), nil
}

func NewKubernetesJobClient(name string, namespace string) (*KubernetesJobClient, error) {
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}
	return &KubernetesJobClient{Clientset: clientset, JobName: name, Namespace: namespace}, nil
}

func getConcurrencyPolicy(policy string) batchv1.ConcurrencyPolicy {
	var concurrencyPolicy batchv1.ConcurrencyPolicy
	switch policy {
	case "Allow":
		concurrencyPolicy = batchv1.AllowConcurrent
	case "Forbid":
		concurrencyPolicy = batchv1.ForbidConcurrent
	case "Replace":
		concurrencyPolicy = batchv1.ReplaceConcurrent
	default:
		fmt.Printf("invalid concurrency policy: %s, defaulting to Allow", policy)
		return batchv1.AllowConcurrent
	}
	return concurrencyPolicy
}

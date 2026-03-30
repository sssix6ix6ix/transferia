package container

import (
	"context"
	"io"

	testify_mock "github.com/stretchr/testify/mock"
	k8s_api "k8s.io/api/core/v1"
	k8s_api_meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type MockKubernetesClient struct {
	testify_mock.Mock
}

func (m *MockKubernetesClient) CreatePod(ctx context.Context, namespace string, pod *k8s_api.Pod) (*k8s_api.Pod, error) {
	args := m.Called(ctx, namespace, pod)
	return args.Get(0).(*k8s_api.Pod), args.Error(1)
}

func (m *MockKubernetesClient) GetPodLogs(ctx context.Context, namespace, podName, containerName string, opts *k8s_api.PodLogOptions) (io.ReadCloser, error) {
	args := m.Called(ctx, namespace, podName, containerName, opts)
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (m *MockKubernetesClient) GetPod(ctx context.Context, namespace, podName string) (*k8s_api.Pod, error) {
	args := m.Called(ctx, namespace, podName)
	return args.Get(0).(*k8s_api.Pod), args.Error(1)
}

func (m *MockKubernetesClient) DeletePod(ctx context.Context, namespace, podName string, opts *k8s_api_meta.DeleteOptions) error {
	args := m.Called(ctx, namespace, podName, opts)
	return args.Error(0)
}

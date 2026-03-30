package container

import (
	"fmt"
	"time"

	k8s_api "k8s.io/api/core/v1"
	k8s_api_meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s_yaml "sigs.k8s.io/yaml"
)

type K8sOpts struct {
	Namespace     string
	PodName       string
	Image         string
	RestartPolicy k8s_api.RestartPolicy
	Command       []string
	Args          []string
	Env           []k8s_api.EnvVar
	Volumes       []k8s_api.Volume
	VolumeMounts  []k8s_api.VolumeMount
	Timeout       time.Duration
}

func (k K8sOpts) String() string {
	pod := &k8s_api.Pod{
		TypeMeta: k8s_api_meta.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: k8s_api_meta.ObjectMeta{
			Name:      k.PodName,
			Namespace: k.Namespace,
		},
		Spec: k8s_api.PodSpec{
			RestartPolicy: k.RestartPolicy,
			Containers: []k8s_api.Container{
				{
					Name:         k.PodName,
					Image:        k.Image,
					Command:      k.Command,
					Args:         k.Args,
					Env:          k.Env,
					VolumeMounts: k.VolumeMounts,
				},
			},
			Volumes: k.Volumes,
		},
	}

	b, err := k8s_yaml.Marshal(pod)
	if err != nil {
		return fmt.Sprintf("error marshalling pod to YAML: %v", err)
	}
	return string(b)
}

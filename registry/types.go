package registry

import (
	"context"
	"io"
)

// Registry 服务注册器，用于服务注册发现
type Registry interface {
	Register(ctx context.Context, si ServiceInstance) error
	Unregister(ctx context.Context, si ServiceInstance) error
	ListServices(ctx context.Context, serviceName string) ([]ServiceInstance, error)
	Subscribe(serviceName string) <-chan struct{}

	io.Closer
}

// ServiceInstance 服务实例
type ServiceInstance struct {
	Name        string // 服务名
	Addr        string // 服务地址
	Group       string // 服务组信息（可选）
	ReadWeight  uint32 // 读权重
	WriteWeight uint32 // 写权重
}

package client

import (
	"fmt"

	"github.com/JrMarcco/easy-kit/xsync"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
)

type Manager[T any] struct {
	grpcClients xsync.Map[string, T]

	rb resolver.Builder
	bb balancer.Builder

	insecure bool

	creator func(conn *grpc.ClientConn) T
}

func (m *Manager[T]) Get(serviceName string) (T, error) {
	grpcClient, ok := m.grpcClients.Load(serviceName)
	if !ok {
		cc, err := m.dial(serviceName)
		if err != nil {
			var zero T
			return zero, fmt.Errorf("[easy-grpc] failed to create grpc client connection for service %s: %v", serviceName, err)
		}

		grpcClient = m.creator(cc)
		m.grpcClients.Store(serviceName, grpcClient)
	}
	return grpcClient, nil
}

func (m *Manager[T]) dial(serviceName string) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithResolvers(m.rb),
		grpc.WithNoProxy(),
	}

	if m.insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	if m.bb != nil {
		opts = append(opts, grpc.WithDefaultServiceConfig(
			fmt.Sprintf(`{"loadBalancingPolicy: %q"}`, m.bb.Name()),
		))
	}

	add := fmt.Sprintf("%s:///%s", m.rb.Scheme(), serviceName)
	return grpc.NewClient(add, opts...)
}

func NewManager[T any](
	rb resolver.Builder, bb balancer.Builder, creator func(conn *grpc.ClientConn) T,
) *Manager[T] {
	return &Manager[T]{
		rb:      rb,
		bb:      bb,
		creator: creator,
	}
}

package client

import (
	"fmt"

	"github.com/JrMarcco/easy-kit/xsync"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
)

type ManagerBuilder[T any] struct {
	rb resolver.Builder
	bb balancer.Builder

	insecure bool

	creator func(conn *grpc.ClientConn) T
}

func (b *ManagerBuilder[T]) ResolverBuilder(rb resolver.Builder) *ManagerBuilder[T] {
	b.rb = rb
	return b
}

func (b *ManagerBuilder[T]) BalancerBuilder(bb balancer.Builder) *ManagerBuilder[T] {
	b.bb = bb
	return b
}

func (b *ManagerBuilder[T]) Insecure() *ManagerBuilder[T] {
	b.insecure = true
	return b
}

func (b *ManagerBuilder[T]) Creator(creator func(conn *grpc.ClientConn) T) *ManagerBuilder[T] {
	b.creator = creator
	return b
}

func (b *ManagerBuilder[T]) Build() *Manager[T] {
	return &Manager[T]{
		rb:       b.rb,
		bb:       b.bb,
		insecure: b.insecure,
		creator:  b.creator,
	}
}

func NewManagerBuilder[T any](rb resolver.Builder, bb balancer.Builder, creator func(conn *grpc.ClientConn) T) *ManagerBuilder[T] {
	return &ManagerBuilder[T]{
		rb:       rb,
		bb:       bb,
		creator:  creator,
		insecure: false,
	}
}

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

	addr := fmt.Sprintf("%s:///%s", m.rb.Scheme(), serviceName)
	return grpc.NewClient(addr, opts...)
}

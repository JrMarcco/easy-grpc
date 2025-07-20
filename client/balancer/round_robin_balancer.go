package balancer

import (
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*RoundRobinBalancerBuilder)(nil)

type RoundRobinBalancerBuilder struct{}

func (b *RoundRobinBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	ccs := make([]balancer.SubConn, 0, len(info.ReadySCs))

	for cc := range info.ReadySCs {
		ccs = append(ccs, cc)
	}

	return &RoundRobinBalancer{
		ccs:    ccs,
		index:  0,
		length: uint64(len(ccs)),
	}
}

var _ balancer.Picker = (*RoundRobinBalancer)(nil)

type RoundRobinBalancer struct {
	ccs []balancer.SubConn

	index  uint64
	length uint64
}

func (p *RoundRobinBalancer) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.ccs) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	index := atomic.AddUint64(&p.index, 1)

	// index - 1 是为了从 0 开始。
	// 这里做不做 -1 没有什么实质性影响，不 -1 也只是第一个节点少参与一次轮询
	cc := p.ccs[(index-1)%p.length]
	return balancer.PickResult{
		SubConn: cc,
		Done:    func(_ balancer.DoneInfo) {},
	}, nil
}

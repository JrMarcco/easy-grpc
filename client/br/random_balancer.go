package br

import (
	"math/rand/v2"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*RandomBalancerBuilder)(nil)

type RandomBalancerBuilder struct{}

func (b *RandomBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	nodes := make([]balancer.SubConn, 0, len(info.ReadySCs))

	for cc := range info.ReadySCs {
		nodes = append(nodes, cc)
	}
	return &RandomBalancer{
		nodes:  nodes,
		length: len(nodes),
	}
}

var _ balancer.Picker = (*RandomBalancer)(nil)

type RandomBalancer struct {
	nodes  []balancer.SubConn
	length int
}

func (p *RandomBalancer) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	if p.length == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	index := rand.IntN(p.length)
	return balancer.PickResult{
		SubConn: p.nodes[index],
		Done:    func(_ balancer.DoneInfo) {},
	}, nil
}

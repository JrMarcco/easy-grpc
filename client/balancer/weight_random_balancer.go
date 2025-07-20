package balancer

import (
	"math/rand"

	"github.com/JrMarcco/easy-grpc/client"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*WeightRandomBalancerBuilder)(nil)

type WeightRandomBalancerBuilder struct{}

func (b *WeightRandomBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	nodes := make([]weightRandomNode, 0, len(info.ReadySCs))

	var totalWeight uint32
	for cc, ccInfo := range info.ReadySCs {
		weight, _ := ccInfo.Address.Attributes.Value(client.AttrNameWeight).(uint32)
		totalWeight += weight

		nodes = append(nodes, weightRandomNode{
			cc:     cc,
			weight: weight,
		})
	}
	return &WeightRandomBalancer{
		nodes:       nodes,
		totalWeight: totalWeight,
	}
}

var _ balancer.Picker = (*WeightRandomBalancer)(nil)

type WeightRandomBalancer struct {
	nodes       []weightRandomNode
	totalWeight uint32
}

func (p *WeightRandomBalancer) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	target := rand.Intn(int(p.totalWeight) + 1)
	for _, node := range p.nodes {
		target -= int(node.weight)
		if target < 0 {
			return balancer.PickResult{
				SubConn: node.cc,
				Done:    func(_ balancer.DoneInfo) {},
			}, nil
		}
	}
	panic("[easy-grpc] unreachable")
}

type weightRandomNode struct {
	cc     balancer.SubConn
	weight uint32
}

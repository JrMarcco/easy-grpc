package balancer

import (
	"context"
	"fmt"
	"strconv"

	"github.com/JrMarcco/easy-grpc/client"
	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*HashBalancerBuilder)(nil)

type HashBalancerBuilder struct{}

func (b *HashBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	nodes := make([]balancer.SubConn, 0, len(info.ReadySCs))

	for cc := range info.ReadySCs {
		nodes = append(nodes, cc)
	}

	return &HashBalancer{
		nodes:  nodes,
		length: len(nodes),
	}
}

var _ balancer.Picker = (*HashBalancer)(nil)

type HashBalancer struct {
	nodes  []balancer.SubConn
	length int
}

func (p *HashBalancer) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if p.length == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	hash, err := p.hashFromPickInfo(info)
	if err != nil {
		return balancer.PickResult{}, err
	}

	index := hash % uint32(p.length)

	return balancer.PickResult{
		SubConn: p.nodes[index],
		Done:    func(_ balancer.DoneInfo) {},
	}, nil
}

func (p *HashBalancer) hashFromPickInfo(info balancer.PickInfo) (uint32, error) {
	var ctx context.Context
	if ctx = info.Ctx; ctx == nil {
		return 0, fmt.Errorf("[easy-grpc] context not found in pick info")
	}

	// 获取 bizId
	bizId, ok := client.BizIdFromContext(ctx)
	if !ok {
		return 0, fmt.Errorf("[easy-grpc] bizId not found in context")
	}

	hash := xxhash.Sum64String(strconv.FormatUint(bizId, 10))
	return uint32(hash), nil
}

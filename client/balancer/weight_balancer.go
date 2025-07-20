package balancer

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/JrMarcco/easy-grpc/client"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*WeightBalancerBuilder)(nil)

type WeightBalancerBuilder struct{}

func (w *WeightBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	nodes := make([]*weightServiceNode, 0, len(info.ReadySCs))

	for cc, ccInfo := range info.ReadySCs {
		weight, _ := ccInfo.Address.Attributes.Value(client.AttrNameWeight).(uint32)
		nodes = append(nodes, &weightServiceNode{
			cc:            cc,
			weight:        weight,
			currentWeight: weight,
		})
	}

	return &WeightBalancer{
		nodes: nodes,
	}
}

var _ balancer.Picker = (*WeightBalancer)(nil)

type WeightBalancer struct {
	nodes []*weightServiceNode
}

func (w *WeightBalancer) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	if len(w.nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var totalWeight uint32
	var selectedNode *weightServiceNode

	for _, node := range w.nodes {
		node.mu.Lock()
		totalWeight += node.efficientWeight
		node.currentWeight += node.efficientWeight

		if selectedNode == nil || selectedNode.currentWeight < node.currentWeight {
			selectedNode = node
		}
		node.mu.Unlock()
	}

	if selectedNode == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	selectedNode.mu.Lock()
	selectedNode.currentWeight -= totalWeight
	selectedNode.mu.Unlock()

	return balancer.PickResult{
		SubConn: selectedNode.cc,
		Done: func(info balancer.DoneInfo) {
			selectedNode.mu.Lock()
			defer selectedNode.mu.Unlock()

			const twice = 2
			if info.Err == nil {
				selectedNode.efficientWeight++
				selectedNode.efficientWeight = max(selectedNode.efficientWeight, selectedNode.weight*twice)
				return
			}

			if errors.Is(info.Err, context.DeadlineExceeded) || errors.Is(info.Err, io.EOF) {
				if selectedNode.efficientWeight > 1 {
					selectedNode.efficientWeight--
				}
			}
		},
	}, nil
}

type weightServiceNode struct {
	mu sync.RWMutex

	cc              balancer.SubConn
	weight          uint32
	currentWeight   uint32
	efficientWeight uint32
}

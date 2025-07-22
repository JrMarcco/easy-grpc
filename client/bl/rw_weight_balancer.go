package bl

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/JrMarcco/easy-grpc/client"
	"github.com/JrMarcco/easy-kit/slice"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*RwWeightBalancerBuilder)(nil)

type RwWeightBalancerBuilder struct {
	mu        sync.RWMutex
	nodeCache map[string]*rwWeightServiceNode
}

func (b *RwWeightBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	nodes := make([]*rwWeightServiceNode, 0, len(info.ReadySCs))
	ccMap := make(map[string]struct{})

	b.mu.Lock()
	defer b.mu.Unlock()

	for cc, ccInfo := range info.ReadySCs {
		readWeight, ok := ccInfo.Address.Attributes.Value(client.AttrNameReadWeight).(uint32)
		if !ok {
			continue
		}
		writeWeight, ok := ccInfo.Address.Attributes.Value(client.AttrNameWriteWeight).(uint32)
		if !ok {
			continue
		}
		groupName, ok := ccInfo.Address.Attributes.Value(client.AttrNameGroup).(string)
		if !ok {
			continue
		}
		nodeName, ok := ccInfo.Address.Attributes.Value(client.AttrNameNode).(string)
		if !ok {
			continue
		}
		ccMap[nodeName] = struct{}{}

		if cacheNode, ok := b.nodeCache[nodeName]; ok {
			// 当前节点存在缓存中，更新连接信息、组信息
			cacheNode.mu.Lock()
			cacheNode.group = groupName
			cacheNode.mu.Unlock()

			if cacheNode.readWeight != readWeight || cacheNode.writeWeight != writeWeight {
				// 权重发生变化，更新权重
				cacheNode = newRwWeightServiceNode(cc, readWeight, writeWeight, groupName)
				b.nodeCache[nodeName] = cacheNode
			}
			nodes = append(nodes, cacheNode)
		} else {
			newNode := newRwWeightServiceNode(cc, readWeight, writeWeight, groupName)
			b.nodeCache[nodeName] = newNode
			nodes = append(nodes, newNode)
		}
	}

	// 从缓存中清除已经不存在的节点
	for key := range b.nodeCache {
		if _, ok := ccMap[key]; !ok {
			delete(b.nodeCache, key)
		}
	}

	return &RwWeightBalancer{nodes: nodes}
}

func NewRwWeightBalancerBuilder() *RwWeightBalancerBuilder {
	return &RwWeightBalancerBuilder{
		nodeCache: make(map[string]*rwWeightServiceNode),
	}
}

var _ balancer.Picker = (*RwWeightBalancer)(nil)

type RwWeightBalancer struct {
	nodes []*rwWeightServiceNode
}

func (p *RwWeightBalancer) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	// 获取候选节点
	ctx := info.Ctx
	candidateNodes := slice.FilterMap(p.nodes, func(_ int, src *rwWeightServiceNode) (*rwWeightServiceNode, bool) {
		src.mu.RLock()
		nodeGroup := src.group
		src.mu.RUnlock()

		group, ok := client.ContextGroup(ctx)
		return src, ok && group == nodeGroup
	})

	if len(candidateNodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	var totalWeight uint32
	var selectedNode *rwWeightServiceNode

	isWriteReq := p.isWriteReq(ctx)

	// 权重计算
	for _, node := range candidateNodes {
		node.mu.Lock()
		if isWriteReq {
			totalWeight += node.efficientWriteWeight
			node.currentWriteWeight += node.efficientWriteWeight
			if selectedNode == nil || selectedNode.currentWriteWeight < node.currentWriteWeight {
				selectedNode = node
			}
			node.mu.Unlock()
			continue
		}

		totalWeight += node.efficientReadWeight
		node.currentReadWeight += node.efficientReadWeight
		if selectedNode == nil || selectedNode.currentReadWeight < node.currentReadWeight {
			selectedNode = node
		}
		node.mu.Unlock()
	}

	if selectedNode == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	selectedNode.mu.Lock()
	if isWriteReq {
		selectedNode.currentWriteWeight -= totalWeight
	} else {
		selectedNode.currentReadWeight -= totalWeight
	}
	selectedNode.mu.Unlock()

	return balancer.PickResult{
		SubConn: selectedNode.cc,
		Done: func(info balancer.DoneInfo) {
			selectedNode.mu.Lock()
			defer selectedNode.mu.Unlock()

			isDecrementErr := info.Err != nil && (errors.Is(info.Err, context.DeadlineExceeded) || errors.Is(info.Err, io.EOF))
			const twice = 2
			if isWriteReq {
				if info.Err == nil {
					selectedNode.efficientWriteWeight++
					selectedNode.currentWriteWeight = max(selectedNode.efficientWriteWeight, selectedNode.writeWeight*twice)
					return
				}
				if isDecrementErr && selectedNode.efficientWriteWeight > 1 {
					selectedNode.efficientWriteWeight--
				}
				return
			}

			if info.Err == nil {
				selectedNode.efficientReadWeight++
				selectedNode.currentReadWeight = max(selectedNode.efficientReadWeight, selectedNode.readWeight*twice)
				return
			}

			if isDecrementErr && selectedNode.efficientReadWeight > 1 {
				selectedNode.efficientReadWeight--
			}
		},
	}, nil
}

func (p *RwWeightBalancer) isWriteReq(ctx context.Context) bool {
	if reqType, ok := client.ContextReqType(ctx); ok {
		return reqType == 1
	}
	return false
}

type rwWeightServiceNode struct {
	mu sync.RWMutex

	cc                  balancer.SubConn
	readWeight          uint32
	currentReadWeight   uint32
	efficientReadWeight uint32

	writeWeight          uint32
	currentWriteWeight   uint32
	efficientWriteWeight uint32

	group string
}

func newRwWeightServiceNode(cc balancer.SubConn, readWeight uint32, writeWeight uint32, group string) *rwWeightServiceNode {
	return &rwWeightServiceNode{
		cc:                   cc,
		readWeight:           readWeight,
		currentReadWeight:    readWeight,
		efficientReadWeight:  readWeight,
		writeWeight:          writeWeight,
		currentWriteWeight:   writeWeight,
		efficientWriteWeight: writeWeight,
		group:                group,
	}
}

package client

import "context"

const (
	AttrNameWeight      = "attr_weight"
	AttrNameReadWeight  = "attr_read_weight"
	AttrNameWriteWeight = "attr_write_weight"
	AttrNameGroup       = "attr_group"
	AttrNameNode        = "attr_node"
)

type contextKeyGroup struct{}

// WithGroup 在 context.Context 内写入 group 信息
func WithGroup(ctx context.Context, group string) context.Context {
	return context.WithValue(ctx, contextKeyGroup{}, group)
}

func GroupFromContext(ctx context.Context) (string, bool) {
	val := ctx.Value(contextKeyGroup{})
	group, ok := val.(string)
	return group, ok
}

type contextKeyReqType struct{}

// WithReqType 在 context.Context 内写入 request type 信息
// read request  = 0
// write request = 1
func WithReqType(ctx context.Context, reqType uint8) context.Context {
	return context.WithValue(ctx, contextKeyReqType{}, reqType)
}

// ReqTypeFromContext 从 context.Context 获取 request type
func ReqTypeFromContext(ctx context.Context) (uint8, bool) {
	val := ctx.Value(contextKeyReqType{})
	reqType, ok := val.(uint8)
	return reqType, ok
}

type contextKeyBizId struct{}

// WithBizId 在 context.Context 内写入 business id
func WithBizId(ctx context.Context, bizId uint64) context.Context {
	return context.WithValue(ctx, contextKeyBizId{}, bizId)
}

// BizIdFromContext 从 context.Context 获取 business id
func BizIdFromContext(ctx context.Context) (uint64, bool) {
	val := ctx.Value(contextKeyBizId{})
	bizId, ok := val.(uint64)
	return bizId, ok
}

type contextKeyBizKey struct{}

// WithBizKey 在 context.Context 写入 business key
func WithBizKey(ctx context.Context, bizKey string) context.Context {
	return context.WithValue(ctx, contextKeyBizKey{}, bizKey)
}

// BizKeyFromContext 从 context.Context 获取 business key
func BizKeyFromContext(ctx context.Context) (string, bool) {
	val := ctx.Value(contextKeyBizKey{})
	bizKey, ok := val.(string)
	return bizKey, ok
}

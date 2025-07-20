package errs

import "errors"

var (
	ErrInvalidEtcdLeaseTTL = errors.New("[easy-grpc] etcd lease TTL must be greater than 0")
)

package pubsub

import (
	"bytes"
)

type Symbol byte

const (
	Equal Symbol = iota
	Greater
	Less
	GreaterOrEqual
	LessOrEqual
)

// BytesCompare 比较两个byte数组，如果a, b满足want指定的结果则返回true，其它返回false
func BytesCompare(a, b []byte, want Symbol) bool {
	switch want {
	case Equal:
		return bytes.Equal(a, b)
	case Greater:
		return bytes.Compare(a, b) > 0
	case GreaterOrEqual:
		return bytes.Compare(a, b) >= 0
	case Less:
		return bytes.Compare(a, b) < 0
	case LessOrEqual:
		return bytes.Compare(a, b) <= 0
	}
	return false
}

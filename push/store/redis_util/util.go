package util

import (
	"bytes"
	"encoding/binary"
)

const (
	SysPrefix        = "bifrost"
	DefaultDelimiter = ":"
)

// EncodeInt64 encode the int64 object to binary
func EncodeInt64(vi int64) []byte {
	var buf bytes.Buffer
	// keep the same pattern of 0.0 and -0.0
	if vi == 0 {
		vi = 0
	}

	vi = ((vi ^ (vi >> 63)) | int64(uint64(^vi)&0x8000000000000000))

	// / Ignore the error returned here, because buf is a memory io.Writer, can should not fail here
	binary.Write(&buf, binary.BigEndian, vi)
	return buf.Bytes()
}

// DecodeFloat64 decode the float64 object from binary
func DecodeInt64(d []byte) int64 {
	vi := int64(binary.BigEndian.Uint64(d))
	if vi == 0 {
		return 0.0
	}
	if vi > 0 {
		vi = ^vi
	} else {
		vi = (vi & 0x7FFFFFFFFFFFFFFF)
	}
	return vi
}

package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesCompare(t *testing.T) {
	cases := []struct {
		name string
		a    []byte
		b    []byte
		want Symbol
		res  bool
	}{
		{
			name: "equal",
			a:    []byte("a"),
			b:    []byte("a"),
			want: Equal,
			res:  true,
		},
		{
			name: "greater",
			a:    []byte("b"),
			b:    []byte("a"),
			want: Greater,
			res:  true,
		},
		{
			name: "greater and equal",
			a:    []byte("b"),
			b:    []byte("a"),
			want: GreaterOrEqual,
			res:  true,
		},
		{
			name: "less",
			a:    []byte("a"),
			b:    []byte("b"),
			want: Less,
			res:  true,
		},
		{
			name: "less and equal",
			a:    []byte("a"),
			b:    []byte("b"),
			want: LessOrEqual,
			res:  true,
		},
		{
			name: "less nil",
			a:    nil,
			b:    []byte("b"),
			want: Less,
			res:  true,
		},
		{
			name: "less or equal nil",
			a:    nil,
			b:    []byte("b"),
			want: LessOrEqual,
			res:  true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, BytesCompare(c.a, c.b, c.want), c.res)
		})
	}
}

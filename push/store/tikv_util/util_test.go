package util

import (
	"fmt"
	"testing"
)

func TestNotFound(t *testing.T) {
	fmt.Println(IsErrNotFound(nil))
}

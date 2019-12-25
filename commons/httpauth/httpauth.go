package httpauth

import (
	"net"
	"net/http"
	"strconv"
	"strings"
)

var AuthRequest = func(req *http.Request) (any, sensitive bool) {
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}
	if strings.HasPrefix(host, "10") || strings.HasPrefix(host, "192.168") || strings.HasPrefix(host, "127") {
		return true, true
	} else if strings.HasPrefix(host, "172") {
		s, err := strconv.Atoi(strings.Split(host, ".")[1])
		if err == nil {
			if s <= 31 && s >= 16 {
				return true, true
			}
		}
	}
	return false, false
}

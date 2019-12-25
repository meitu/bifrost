package poll

import (
	"fmt"
	"net"
	"reflect"
)

// system takes the net Listener and makes it non-blocking.
func SocketFD(ln interface{}) (fd int, err error) {
	lnName := reflect.Indirect(reflect.ValueOf(ln)).Type().String()
	switch lnName {
	case "net.TCPConn":
		tcpConn := reflect.Indirect(reflect.ValueOf(ln)).FieldByName("conn")
		return TCPFD(tcpConn), nil
	case "net.TCPListener":
		return PointerTCPFD(ln), nil
	case "tls.Conn":
		tcpConn := reflect.Indirect(reflect.ValueOf(ln)).FieldByName("conn")
		tcpConn = reflect.Indirect(tcpConn.Elem())
		return TCPFD(tcpConn), nil
	case "tls.listener":
		tln := reflect.Indirect(reflect.ValueOf(ln)).FieldByName("Listener")
		ln, _ := tln.Interface().(*net.TCPListener)
		return PointerTCPFD(ln), nil
	}
	return 0, fmt.Errorf("socket fd parse failed ,the type of fd is : %s", lnName)
}

func PointerTCPFD(ln interface{}) int {
	fdVal := reflect.Indirect(reflect.ValueOf(ln)).FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")
	return int(pfdVal.FieldByName("Sysfd").Int())
}

func TCPFD(ln reflect.Value) int {
	fdVal := ln.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")
	return int(pfdVal.FieldByName("Sysfd").Int())
}

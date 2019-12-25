package poll

import "unsafe"

type EventType int

const (
	EEvent EventType = iota << 1
	REvent
	WEvent
)

type Event struct {
	etype EventType
	fd    int
	data  unsafe.Pointer
}

func NewEvent(fd int) Event {
	return Event{
		fd: fd,
	}
}

func (ev Event) Type() EventType {
	return ev.etype
}

func (ev Event) FD() int {
	return ev.fd
}

func (ev Event) Data() unsafe.Pointer {
	return ev.data
}

func (ev Event) DataByte() *byte {
	return (*byte)(ev.data)
}

func (ev Event) DataInt() int32 {
	return (int32)((uintptr)(ev.data))
}

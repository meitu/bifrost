package poll

import (
	"syscall"
	"unsafe"
)

// Poll ...
type Poll struct {
	fd     int // epoll fd
	wfd    int // wake fd
	closed chan struct{}
}

// OpenPoll ...
func OpenPoll() (*Poll, error) {
	l := new(Poll)
	p, err := syscall.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	l.fd = p
	r0, _, e0 := syscall.Syscall(syscall.SYS_EVENTFD2, 0, 0, 0)
	if e0 != 0 {
		syscall.Close(p)
		return nil, err
	}
	l.wfd = int(r0)
	l.AddRead(l.wfd, nil)
	l.closed = make(chan struct{}, 1)
	return l, nil
}

// Close ...
func (p *Poll) Close() error {
	if _, err := syscall.Write(p.wfd, []byte{0, 0, 0, 0, 0, 0, 0, 1}); err != nil {
		return err
	}
	<-p.closed

	if err := syscall.Close(p.wfd); err != nil {
		return err
	}
	return syscall.Close(p.fd)
}

// Wait ...
func (p *Poll) Wait(iter func(ev Event) error) error {
	events := make([]syscall.EpollEvent, 512)
	defer func() {
		close(p.closed)
	}()

	for {
		n, err := syscall.EpollWait(p.fd, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return err
		}
		for i := 0; i < n; i++ {
			if p.wfd == int(events[i].Fd) {
				return nil
			}
			ev := Event{fd: int(events[i].Fd), etype: p.event(events[i].Events), data: (unsafe.Pointer)((uintptr)(events[i].Pad))}
			if err := iter(ev); err != nil {
				return err
			}
		}
	}
}

// AddRead ...
func (p *Poll) AddRead(fd int, data unsafe.Pointer) error {
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_ADD, fd,
		&syscall.EpollEvent{Fd: int32(fd),
			Events: syscall.EPOLLIN | syscall.EPOLLONESHOT,
			Pad:    (int32)((uintptr)(data)),
		})
}

// AddReadWrite ...
func (p *Poll) AddReadWrite(fd int, data unsafe.Pointer) error {
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_ADD, fd,
		&syscall.EpollEvent{Fd: int32(fd),
			Events: syscall.EPOLLIN | syscall.EPOLLOUT | syscall.EPOLLONESHOT,
			Pad:    (int32)((uintptr)(data)),
		})
}

// ModDetach ...
func (p *Poll) Remove(fd int, data unsafe.Pointer) error {
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_DEL, fd,
		&syscall.EpollEvent{Fd: int32(fd),
			Events: syscall.EPOLLIN | syscall.EPOLLOUT | syscall.EPOLLONESHOT,
			Pad:    (int32)((uintptr)(data)),
		})
}

func (p *Poll) ModRead(ev Event) error {
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_MOD, ev.fd,
		&syscall.EpollEvent{Fd: int32(ev.fd),
			Events: syscall.EPOLLIN | syscall.EPOLLONESHOT,
			Pad:    ev.DataInt(),
		},
	)
}

// ModReadWrite ...
func (p *Poll) ModReadWrite(ev Event) error {
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_MOD, ev.fd,
		&syscall.EpollEvent{Fd: int32(ev.fd),
			Events: syscall.EPOLLIN | syscall.EPOLLOUT | syscall.EPOLLONESHOT,
			Pad:    ev.DataInt(),
		},
	)
}

// event
// if et only contains EPOLLERR or EPOLLHUP event return EEvent otherwise return REvent or WEvent
func (p *Poll) event(et uint32) EventType {
	var etype EventType
	if (et & syscall.EPOLLOUT) != 0 {
		etype |= WEvent
	}

	if (et & syscall.EPOLLIN) != 0 {
		etype |= REvent
	}

	if etype == 0 {
		etype = REvent
	}
	return etype
}

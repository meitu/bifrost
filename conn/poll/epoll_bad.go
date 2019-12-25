// +build darwin dragonfly freebsd netbsd openbsd

package poll

import (
	"syscall"
	"unsafe"
)

// Poll ...
type Poll struct {
	fd int
}

// OpenPoll ...
func OpenPoll() (*Poll, error) {
	l := new(Poll)
	p, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}
	l.fd = p
	return l, nil
}

// Close ...
func (p *Poll) Close() error {
	return syscall.Close(p.fd)
}

// Wait ...
func (p *Poll) Wait(iter func(ev Event) error) error {
	events := make([]syscall.Kevent_t, 128)
	for {
		n, err := syscall.Kevent(p.fd, nil, events, nil)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return err
		}

		for i := 0; i < n; i++ {
			if events[i].Ident != 0 {
				ev := Event{fd: int(events[i].Ident), etype: p.event(events[i].Filter), data: unsafe.Pointer(events[i].Udata)}
				if err := iter(ev); err != nil {
					return err
				}
			}
		}
	}
}

// AddRead ...
func (p *Poll) AddRead(fd int, data unsafe.Pointer) error {
	changes := []syscall.Kevent_t{syscall.Kevent_t{
		Ident: uint64(fd), Flags: syscall.EV_ADD | syscall.EV_ONESHOT, Filter: syscall.EVFILT_READ, Udata: (*byte)(data),
	}}
	_, err := syscall.Kevent(p.fd, changes, nil, nil)
	return err
}

// AddReadWrite ...
func (p *Poll) AddReadWrite(fd int, data unsafe.Pointer) error {
	changes := []syscall.Kevent_t{
		syscall.Kevent_t{
			Ident: uint64(fd), Flags: syscall.EV_ADD | syscall.EV_ONESHOT, Filter: syscall.EVFILT_READ, Udata: (*byte)(data),
		},
		syscall.Kevent_t{
			Ident: uint64(fd), Flags: syscall.EV_ADD | syscall.EV_ONESHOT, Filter: syscall.EVFILT_WRITE, Udata: (*byte)(data),
		},
	}
	_, err := syscall.Kevent(p.fd, changes, nil, nil)
	return err
}

func (p *Poll) Remove(fd int, data unsafe.Pointer) error {
	/*
			changes := []syscall.Kevent_t{
					syscall.Kevent_t{
						Ident: uint64(fd), Flags: syscall.EV_DELETE, Filter: syscall.EVFILT_READ, Udata: (*byte)(data),
					},
						syscall.Kevent_t{
							Ident: uint64(fd), Flags: syscall.EV_DELETE | syscall.EV_ONESHOT, Filter: syscall.EVFILT_WRITE, Udata: (*byte)(data),
						},
				)
		_, err := syscall.Kevent(p.fd, changes, nil, nil)
	*/
	return nil
}

func (p *Poll) ModRead(ev Event) error {
	changes := []syscall.Kevent_t{
		syscall.Kevent_t{
			Ident: uint64(ev.fd), Flags: syscall.EV_ADD | syscall.EV_ONESHOT, Filter: syscall.EVFILT_READ, Udata: ev.DataByte(),
		},
	}
	_, err := syscall.Kevent(p.fd, changes, nil, nil)
	return err
}

// ModReadWrite ...
func (p *Poll) ModReadWrite(ev Event) error {
	changes := []syscall.Kevent_t{
		syscall.Kevent_t{
			Ident: uint64(ev.fd), Flags: syscall.EV_ADD | syscall.EV_ONESHOT, Filter: syscall.EVFILT_READ, Udata: ev.DataByte(),
		},
		syscall.Kevent_t{
			Ident: uint64(ev.fd), Flags: syscall.EV_ADD | syscall.EV_ONESHOT, Filter: syscall.EVFILT_WRITE, Udata: ev.DataByte(),
		},
	}
	_, err := syscall.Kevent(p.fd, changes, nil, nil)
	return err
}

// Event
func (p *Poll) event(filter int16) EventType {
	switch filter {
	case syscall.EVFILT_WRITE:
		return WEvent
	case syscall.EVFILT_READ:
		return REvent
	}
	return REvent
}

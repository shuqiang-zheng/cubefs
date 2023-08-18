package rdma

import "C"
import (
	"sync"
	"syscall"
)

var (
	epollFd = -1
	lock    sync.Mutex
	once    sync.Once
)

type ReadAble func()
type EpollContext struct {
	cond *sync.Cond
}

type EPoll struct {
	epollFd int
	fds     map[int]ReadAble
}

var instance *EPoll

func GetEpoll() *EPoll {
	once.Do(func() {
		instance = &EPoll{}
		instance.init()
	})

	return instance
}

func (e *EPoll) init() {
	var err error
	e.epollFd, err = syscall.EpollCreate(256)
	if err != nil {
		panic(err)
	}
	instance.fds = make(map[int]ReadAble)

	go e.epollLoop()
}

func (e *EPoll) getContext(fd int) ReadAble {
	return e.fds[fd]
}

func (e *EPoll) EpollAdd(fd int, ctx ReadAble) {
	event := syscall.EpollEvent{}
	event.Events = syscall.EPOLLIN
	event.Fd = int32(fd)
	e.fds[fd] = ctx
	syscall.EpollCtl(e.epollFd, syscall.EPOLL_CTL_ADD, fd, &event)
}

func (e *EPoll) epollLoop() error {
	for {
		events := make([]syscall.EpollEvent, 100)
		n, err := syscall.EpollWait(e.epollFd, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return err
		}

		for i := 0; i < n; i++ {
			e.getContext(int(events[i].Fd))()
		}
	}
}

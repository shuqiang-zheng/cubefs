package rdma

import "sync"

type EpollWorker struct {
	ctx     ReadAble
	wg      sync.WaitGroup
	jobs    chan int
	closeCh chan struct{}
}

func (ew *EpollWorker) initEpollWorker(ctx ReadAble, wg sync.WaitGroup) {
	ew.jobs = make(chan int, 100)
	ew.closeCh = make(chan struct{})
	ew.ctx = ctx
	ew.wg = wg
	go func(epollWorker *EpollWorker) {
		for {
			select {
			case <-epollWorker.jobs:
				//println("exec eventFunc")
				epollWorker.ctx()
				//ew.wg.Done()
			case <-epollWorker.closeCh:
				return
			}
		}
	}(ew)

}

func (ew *EpollWorker) epollWorkerAddJob() {
	//println("exec add job")
	ew.jobs <- 1
}

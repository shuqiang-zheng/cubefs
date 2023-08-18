// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rdma

import "C"
import (
	"strings"
	"sync"
	"time"
)

type RdmaServerObject struct {
	server *Server // server conn
	idle   int64
}

type RdmaClientObject struct {
	client *Client // client conn
	idle   int64
}

const (
	RdmaConnectIdleTime       = 30
	defaultRdmaConnectTimeout = 1
)

type RdmaConnectPool struct {
	sync.RWMutex
	serverPools map[string]*ServerPool
	clientPools map[string]*ClientPool
	//connPools       map[string]*Connection
	//clientConnPools map[string]*ClientConnection
	mincap         int
	maxcap         int
	timeout        int64
	connectTimeout int64
	closeCh        chan struct{}
	closeOnce      sync.Once
}

func NewRdmaConnectPool() (rcp *RdmaConnectPool) {
	rcp = &RdmaConnectPool{
		serverPools:    make(map[string]*ServerPool),
		clientPools:    make(map[string]*ClientPool),
		mincap:         5,
		maxcap:         500,
		timeout:        int64(time.Second * RdmaConnectIdleTime),
		connectTimeout: defaultRdmaConnectTimeout,
		closeCh:        make(chan struct{}),
	}
	//go rcp.autoRelease()

	return rcp
}

func NewRdmaConnectPoolWithTimeout(idleConnTimeout time.Duration, connectTimeout int64) (rcp *RdmaConnectPool) {
	rcp = &RdmaConnectPool{
		serverPools:    make(map[string]*ServerPool),
		clientPools:    make(map[string]*ClientPool),
		mincap:         5,
		maxcap:         80,
		timeout:        int64(idleConnTimeout * time.Second),
		connectTimeout: connectTimeout,
		closeCh:        make(chan struct{}),
	}
	//go rcp.autoRelease()

	return rcp
}

func (rcp *RdmaConnectPool) GetRdmaServer(targetAddr string) (server *Server, err error) {
	rcp.RLock()
	pool, ok := rcp.serverPools[targetAddr]
	rcp.RUnlock()
	if !ok {
		newPool := NewServerPool(rcp.mincap, rcp.maxcap, rcp.timeout, rcp.connectTimeout, targetAddr)
		rcp.Lock()
		pool, ok = rcp.serverPools[targetAddr]
		if !ok {
			//pool = NewPool(cp.mincap, cp.maxcap, cp.timeout, cp.connectTimeout, targetAddr)
			pool = newPool
			rcp.serverPools[targetAddr] = pool
		}
		rcp.Unlock()
	}

	return pool.GetServerFromPool()
}

func (rcp *RdmaConnectPool) GetRdmaClient(targetAddr string) (client *Client, err error) {
	rcp.RLock()
	pool, ok := rcp.clientPools[targetAddr]
	rcp.RUnlock()
	if !ok {
		newPool := NewClientPool(rcp.mincap, rcp.maxcap, rcp.timeout, rcp.connectTimeout, targetAddr)
		rcp.Lock()
		pool, ok = rcp.clientPools[targetAddr]
		if !ok {
			//pool = NewPool(cp.mincap, cp.maxcap, cp.timeout, cp.connectTimeout, targetAddr)
			pool = newPool
			rcp.clientPools[targetAddr] = pool
		}
		rcp.Unlock()
	}

	return pool.GetClientFromPool()
}

func (rcp *RdmaConnectPool) PutRdmaServer(server *Server, forceClose bool) {
	if server == nil {
		return
	}
	/*	if forceClose {
			_ = c.Close()
			return
		}
		select {
		case <-cp.closeCh:
			_ = c.Close()
			return
		default:
		}*/
	addr := server.remoteIp + ":" + server.remotePort
	rcp.RLock()
	pool, ok := rcp.serverPools[addr]
	rcp.RUnlock()
	if !ok {
		//c.Close()
		return
	}
	object := &RdmaServerObject{server: server, idle: time.Now().UnixNano()}
	pool.PutRdmaServerObjectToPool(object)
}

func (rcp *RdmaConnectPool) PutRdmaClient(client *Client, forceClose bool) {
	if client == nil {
		return
	}
	/*	if forceClose {
			_ = c.Close()
			return
		}
		select {
		case <-cp.closeCh:
			_ = c.Close()
			return
		default:
		}*/
	addr := client.remoteIp + ":" + client.remotePort
	rcp.RLock()
	pool, ok := rcp.clientPools[addr]
	rcp.RUnlock()
	if !ok {
		//c.Close()
		return
	}
	object := &RdmaClientObject{client: client, idle: time.Now().UnixNano()}
	pool.PutRdmaClientObjectToPool(object)
}

type ServerPool struct {
	objects        chan *RdmaServerObject
	mincap         int
	maxcap         int
	target         string
	timeout        int64
	connectTimeout int64
}

type ClientPool struct {
	objects        chan *RdmaClientObject
	mincap         int
	maxcap         int
	target         string
	timeout        int64
	connectTimeout int64
}

func NewServerPool(min, max int, timeout, connectTimeout int64, target string) (p *ServerPool) {
	p = new(ServerPool)
	p.mincap = min
	p.maxcap = max
	p.target = target
	p.objects = make(chan *RdmaServerObject, max)
	p.timeout = timeout
	p.connectTimeout = connectTimeout
	p.initAllConnect()
	return p
}

func NewClientPool(min, max int, timeout, connectTimeout int64, target string) (p *ClientPool) {
	p = new(ClientPool)
	p.mincap = min
	p.maxcap = max
	p.target = target
	p.objects = make(chan *RdmaClientObject, max)
	p.timeout = timeout
	p.connectTimeout = connectTimeout
	p.initAllConnect()
	return p
}

func (serverPool *ServerPool) initAllConnect() {
	str := strings.Split(serverPool.target, ":")
	targetIp := str[0]
	targetPort := str[1]
	for i := 0; i < serverPool.mincap; i++ {
		server, err := NewRdmaServer(targetIp, targetPort)
		if err == nil {
			o := &RdmaServerObject{server: server, idle: time.Now().UnixNano()}
			serverPool.PutRdmaServerObjectToPool(o)
		}
	}
}

func (clientPool *ClientPool) initAllConnect() {
	str := strings.Split(clientPool.target, ":")
	targetIp := str[0]
	targetPort := str[1]
	for i := 0; i < clientPool.mincap; i++ {
		client, err := NewRdmaClient(targetIp, targetPort)
		if err == nil {
			o := &RdmaClientObject{client: client, idle: time.Now().UnixNano()}
			clientPool.PutRdmaClientObjectToPool(o)
		}
	}
}

func (serverPool *ServerPool) PutRdmaServerObjectToPool(o *RdmaServerObject) {
	select {
	case serverPool.objects <- o:
		return
	default:
		if o.server != nil {
			//o.conn.Close()
		}
		return
	}
}

func (clientPool *ClientPool) PutRdmaClientObjectToPool(o *RdmaClientObject) {
	select {
	case clientPool.objects <- o:
		return
	default:
		if o.client != nil {
			//o.conn.Close()
		}
		return
	}
}

func (serverPool *ServerPool) GetServerFromPool() (server *Server, err error) {
	var (
		o *RdmaServerObject
	)
	for {
		select {
		case o = <-serverPool.objects:
		default:
			str := strings.Split(serverPool.target, ":")
			targetIp := str[0]
			targetPort := str[1]
			return NewRdmaServer(targetIp, targetPort)
		}
		if time.Now().UnixNano()-int64(o.idle) > serverPool.timeout {
			//_ = o.conn.Close()
			o = nil
			continue
		}
		return o.server, nil
	}
}

func (clientPool *ClientPool) GetClientFromPool() (client *Client, err error) {
	var (
		o *RdmaClientObject
	)
	for {
		select {
		case o = <-clientPool.objects:
		default:
			str := strings.Split(clientPool.target, ":")
			targetIp := str[0]
			targetPort := str[1]
			return NewRdmaClient(targetIp, targetPort)
		}
		if time.Now().UnixNano()-int64(o.idle) > clientPool.timeout {
			//_ = o.conn.Close()
			o = nil
			continue
		}
		return o.client, nil
	}
}

/*func (rcp *RdmaConnectPool) GetRdmaConn(targetAddr string) (c *Connection) {
	rcp.RLock()
	c, _ = rcp.connPools[targetAddr]
	rcp.RUnlock()

	return
}*/

/*func (rcp *RdmaConnectPool) PutRdmaConn(targetAddr string, c *Connection) {
	rcp.Lock()
	rcp.connPools[targetAddr] = c
	rcp.Unlock()
}*/

/*func (rcp *RdmaConnectPool) GetRdmaClientConn(targetAddr string) (c *ClientConnection) {
	rcp.RLock()
	c, _ = rcp.clientConnPools[targetAddr]
	rcp.RUnlock()

	return
}*/

/*func (rcp *RdmaConnectPool) PutRdmaClientConn(targetAddr string, c *ClientConnection) {
	rcp.Lock()
	rcp.clientConnPools[targetAddr] = c
	rcp.Unlock()
}*/

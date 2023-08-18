package rdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm -lrt -pthread
#include "client.c"
#include "server.c"
*/
import "C"
import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

const CONNECTED = 1
const DISCONNECTED = 0

type ConnectionCallback func(int, *Connection)
type ClientConnCallback func(int, *Buffer, *ClientConnection)
type RecvCallback func(buffer *Buffer) int

/*var connId int
var conns [100]*Connection
var clientConns [100]*ClientConnection*/
/*var clientId int
var clients [100]*Client

var serverId int
var servers [100]*Server*/

var rdmaConnectPool = NewRdmaConnectPool()

//export EpollAddSendEvent
func EpollAddSendEvent(fd C.int, ctx unsafe.Pointer) {
	GetEpoll().EpollAdd(int(fd), func() {
		C.transport_send_event_cb(ctx)
	})
}

//export EpollAddRecvEvent
func EpollAddRecvEvent(fd C.int, ctx unsafe.Pointer) {
	GetEpoll().EpollAdd(int(fd), func() {
		C.transport_recv_event_cb(ctx)
	})
}

//export ServerConnectionCallback
func ServerConnectionCallback(state C.int, cConn *C.Connection) {
	//server := servers[cConn.agentAddr]
	server := rdmaConnectPool.GetRdmaServer(cConn.agentAddr)
	var conn *Connection
	conn = rdmaConnectPool.GetRdmaConn(cConn.agentAddr)
	if conn == nil {
		conn = &Connection{}
		conn.cConn = cConn
		rdmaConnectPool.PutRdmaConn(cConn.agentAddr, conn)
	}
	/*    if cConn.connId >= 0{
	          conn = conns[cConn.connId]
	      }
	      if conn == nil {
	          connId++
	          conn = &Connection{}
	          conn.cConn = cConn
	          cConn.connId = C.int(connId)
	          conns[connId] = conn
	      }*/
	conn.state = int(state)
	server.cb(int(state), conn)
}

//export ClientConnectionCallback
func ClientConnectionCallback(state C.int, cConn *C.Connection) {
	//client := clients[cConn.agentId]
	client := rdmaConnectPool.GetRdmaClient(cConn.agentAddr)
	var conn *ClientConnection
	conn = rdmaConnectPool.GetRdmaClientConn(cConn.agentAddr)
	if conn == nil {
		conn = &ClientConnection{}
		conn.cConn = cConn
		rdmaConnectPool.PutRdmaClientConn(cConn.agentAddr, conn)
	}

	conn.state = int(state)
	sendBuf := &Buffer{ctx: new(proto.Packet)}
	sendBuf.ctx.Data = []byte("hello rdma!")
	client.cb(int(state), sendBuf, conn)
}

//export RecvMessageCallback
func RecvMessageCallback(bufCtx *C.MemoryEntry, cConn *C.Connection) C.int {
	if cConn == nil {
		return 0
	}
	if cConn.conntype == 1 {
		conn := rdmaConnectPool.GetRdmaConn(cConn.agentAddr)
		//conn := conns[cConn.connId]
		if conn == nil || conn.cb == nil {
			return 0
		}
		if bufCtx == nil {
			conn.cb(nil)
			return 0
		}

		ret := C.int(conn.cb(&Buffer{bufCtx}))
		return ret
	} else {
		conn := rdmaConnectPool.GetRdmaClientConn(cConn.agentAddr)
		//conn := clientConns[cConn.connId]
		if conn == nil || conn.cb == nil {
			return 0
		}
		if bufCtx == nil {
			conn.cb(nil)
			return 0
		}

		ret := C.int(conn.cb(&Buffer{bufCtx}))
		return ret
	}

}

type Server struct {
	localIp    string
	localPort  string
	remoteIp   string
	remotePort string
	fd         int
	conn_ev    unsafe.Pointer
	cb         ConnectionCallback
	cServer    unsafe.Pointer
}

func NewRdmaServer(targetIp, targetPort string) (server *Server, err error) {
	server = &Server{}
	server.remoteIp = ""   // TODO
	server.remotePort = "" // TODO
	server.localIp = targetIp
	server.localPort = targetPort
	nPort, _ := strconv.Atoi(server.localPort)
	serverAddr := strings.Join([]string{server.localIp, server.localPort}, ":")
	cCtx := C.StartServer(C.CString(server.localIp), C.ushort(nPort), C.string(serverAddr))
	if cCtx == nil {
		return nil, errors.New("server start failed")
	}

	server.cServer = unsafe.Pointer(cCtx)
	server.fd = int(cCtx.listen_id.channel.fd)
	server.conn_ev = unsafe.Pointer(cCtx.conn_ev)
	return server, nil
}

func (server *Server) Start(cb ConnectionCallback) {
	server.cb = cb

	GetEpoll().EpollAdd(server.fd, func() {
		C.connection_event_cb(unsafe.Pointer(server.conn_ev))
	})
}

type Client struct {
	localIp    string
	localPort  string
	remoteIp   string
	remotePort string
	fd         int
	conn_ev    unsafe.Pointer
	cb         ClientConnCallback
	ctx        unsafe.Pointer
}

func NewRdmaClient(targetIp, targetPort string) (client *Client, err error) {
	client = &Client{}
	client.remoteIp = targetIp
	client.remotePort = targetPort
	client.localIp = ""   //TODO
	client.localPort = "" //TODO
	targetAddr := strings.Join([]string{client.remoteIp, client.remotePort}, ":")
	cCtx := C.Connect(C.CString(client.remoteIp), C.CString(client.remotePort), C.string(targetAddr))
	if cCtx == nil {
		err = errors.New("client connect failed")
		return nil, err
	}

	client.ctx = unsafe.Pointer(cCtx)
	client.fd = int(cCtx.listen_id.channel.fd)
	client.conn_ev = unsafe.Pointer(cCtx.conn_ev)
	return client, nil
}

func (client *Client) Connect(memoryCapacity int, cb ClientConnCallback) {
	client.cb = cb

	GetEpoll().EpollAdd(client.fd, func() {
		C.connection_event_cb(client.conn_ev, memoryCapacity)
	})
}

type Connection struct {
	cConn *C.Connection
	state int
	cb    RecvCallback
	//bufs map[]*Buffer
}

type Buffer struct {
	ctx *C.MemoryEntry
	//ctx *proto.Packet
}

func (b *Buffer) Buf() []byte {
	return ((*[1 << 31]byte)(b.ctx.buf))[0:b.ctx.buf_len:b.ctx.buf_len]
}

func (c *Connection) SetRecvCallback(cb RecvCallback) {
	c.cb = cb
}

func (c *Connection) Write(buf *Buffer) int {
	ret := C.Write(c.cConn, buf.ctx)
	return int(ret)
}

func (c *Connection) WriteImm(buf *Buffer, len int) int {
	ret := C.WriteImm(c.cConn, buf.ctx, C.int(len))
	return int(ret)
}

func (c *Connection) Read(buf []byte) int {
	return 0
}

func (c *Connection) Send(buf *Buffer) int {
	ret := C.Send(c.cConn, buf.ctx)
	return int(ret)
}

func (c *Connection) Recv(buf []byte) int {
	cBuf := (*C.char)(unsafe.Pointer(&buf[0]))
	return int(C.Recv(c.cConn, cBuf, C.int(len(buf))))
}

func (c *Connection) GetBuffer(len int) *Buffer {
	entry := C.Malloc(c.cConn, C.int(len))

	/* var bufff *[1<<30]byte
	   bufff = (*[1<<30]byte)(entry.buf)*/

	return &Buffer{ctx: entry}
}

type Context struct {
	c   chan struct{}
	buf *Buffer
}

type ClientConnection struct {
	Connection
	pending chan *Context

	seq      int32
	contexts sync.Map
}

func (c *ClientConnection) ctx(seq int32) *Context {
	v, ok := c.contexts.Load(seq)
	if !ok {
		println("seq not found", seq)
	}
	return v.(*Context)
}

func (c *ClientConnection) Start() {
	c.SetRecvCallback(func(buf *Buffer) int {
		//println("recv resp ", *(*int32)(unsafe.Pointer(&buf.Buf()[0])))
		c.ctx(*(*int32)(unsafe.Pointer(&buf.Buf()[0]))).c <- struct{}{}
		return 0
	})

	c.pending = make(chan *Context)
	c.contexts = sync.Map{}

	go c.sendLoop()
}

func (c *ClientConnection) sendLoop() {
	for {
		select {
		case ctx := <-c.pending:
			c.seq += 1

			seq := (*int32)(unsafe.Pointer(&ctx.buf.Buf()[0]))
			*seq = c.seq
			//println("invoke do ", *(*int32)(unsafe.Pointer(&ctx.buf.Buf()[0])))
			c.contexts.Store(c.seq, ctx)

			if common.GParam.RdmaWrite {
				c.WriteImm(ctx.buf, -1)
			} else {
				c.Send(ctx.buf)
			}
		}
	}
}

func (c *ClientConnection) Invoke(buf *Buffer) {
	ctx := &Context{}
	ctx.c = make(chan struct{})
	ctx.buf = buf

	c.pending <- ctx
	<-ctx.c

	c.contexts.Delete(*(*int32)(unsafe.Pointer(&ctx.buf.Buf()[0])))
	//println("invoke ok ", *(*int32)(unsafe.Pointer(&ctx.buf.Buf()[0])))
}

/*func InitParam() {
    if common.GParam.RdmaWrite {
        C.initParam(C.int(common.GParam.IoSize), C.int(common.GParam.IoDeep), C.int(1))
    } else  {
        C.initParam(C.int(common.GParam.IoSize), C.int(common.GParam.IoDeep), C.int(0))
    }
}*/

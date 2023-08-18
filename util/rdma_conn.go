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

package util

import "C"
import (
	"github.com/cubefs/cubefs/util/rdma"
	"strings"
	"unsafe"
)

func NewRdmaServer(targetAddr string) (server *rdma.Server) {
	str := strings.Split(targetAddr, ":")
	targetIp := str[0]
	targetPort := str[1]
	server = rdma.StartServer(targetIp, targetPort, func(state int, conn *rdma.Connection) {
		println("server ConnectionCallBack ", state)
		if state == rdma.CONNECTED {
			conn.SetRecvCallback(func(buf *rdma.Buffer) int {
				println("serverRecvCallback: recv  ", *(*int32)(unsafe.Pointer(&buf.Buf()[0])))
				/*					if true { //TODO
										conn.WriteImm(buf, 4)
									} else {
										conn.Send(buf)
									}*/
				//common.Stat().AddSum(common.GParam.IoSize)
				return 1
			})
		} else {
			// TODO 连接未建立
		}
	})

	return server
}

func NewRdmaClient(targetAddr string, memoryCapacity int) (client *rdma.Client) {
	str := strings.Split(targetAddr, ":")
	targetIp := str[0]
	targetPort := str[1]
	client = rdma.Connect(targetIp, targetPort, memoryCapacity, func(state int, sendBuf *rdma.Buffer, conn *rdma.ClientConnection) {
		println("client ConnectionCallBack ", state)
		if state == rdma.CONNECTED {
			conn.Invoke(sendBuf)
		} else {
			// TODO 连接未建立
		}
	})

	return client
}

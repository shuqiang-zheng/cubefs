{
  "role": "datanode",
  "listen": "17310",
  "localIP": "_ip_",
  "rdmaPort": "18515",
  "rdmaIP": "_rdma_",
  "bindIp": "true",
  "raftHeartbeat": "17330",
  "raftReplica": "17340",
  "raftDir": "_dir_/raftlog/datanode",
  "logDir": "_dir_/logs",
  "warnLogDir": "_dir_/logs",
  "logLevel": "debug",
  "disks": [
  	"_dir_/disk:2048"
  ],
  "enableSmuxConnPool": "true",
  "masterAddr": [
      _master_addr_
]
}

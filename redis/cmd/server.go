package main

import (
	"log"
	"sync"

	"github.com/tidwall/redcon"
	"github.com/yifaaan/bitcask"
	bitcask_rds "github.com/yifaaan/bitcask/redis"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*bitcask_rds.RedisDataStruct
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	rds, err := bitcask_rds.NewRedisDatastruct(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_rds.RedisDataStruct),
	}
	bitcaskServer.dbs[0] = rds

	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()

}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connections.")

	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	client := &BitcaskClient{}
	svr.mu.Lock()
	defer svr.mu.Unlock()

	client.server = svr
	client.db = svr.dbs[0]

	conn.SetContext(client)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	svr.server.Close()
}

package raft

import (
	"fmt"
	"log"
	"net"
	"time"
)

type Transport interface {
	net.Listener
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

type RPCService interface {
	ServeRPC(string, []byte) ([]byte, error)
}

type ClusterRPC struct {
	tn Transport

	Service RPCService
}

func NewClusterRPC(tn Transport) *ClusterRPC {
	return &ClusterRPC{
		tn: tn,
	}
}

func (crpc *ClusterRPC) Serve() error {
	if crpc.Service == nil {
		return fmt.Errorf("service not set")
	}

	for {
		conn, err := crpc.tn.Accept()
		if err != nil {
			return err
		}

		go crpc.handleConn(conn)
	}
}

func (crpc *ClusterRPC) handleConn(conn net.Conn) {
	log.Println("Accepting request from:", conn.RemoteAddr())

	b, err := recvRpcMessage(conn)
	if err == nil {
		log.Println("Received bytes:", len(b))

		var resp []byte
		resp, err = crpc.Service.ServeRPC(conn.RemoteAddr().String(), b)
		if err != nil {
			log.Println(err)
			err = sendRpcMessage(conn, []byte(err.Error()))
		} else {
			if resp == nil || len(resp) < 1 {
				resp = []byte("ok")
			}
			err = sendRpcMessage(conn, resp)
		}
	}

	if err != nil {
		log.Println(err)
		conn.Close()
	}
}

func (crpc *ClusterRPC) Dial(address string, timeout time.Duration) (conn net.Conn, err error) {
	return crpc.tn.Dial(address, timeout)
}

func (crpc *ClusterRPC) Send(address string, data []byte, timeout time.Duration) ([]byte, error) {

	var resp []byte
	conn, err := crpc.Dial(address, timeout)
	if err == nil {
		if err = sendRpcMessage(conn, data); err == nil {
			resp, err = recvRpcMessage(conn)
		}
	}

	return resp, err
}

package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/euforia/sql-raft/raft/mux"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
)

const (
	muxRaftHeader byte = iota
	muxRpcHeader
)

const (
	JoinRpcOp byte = iota
)

/*
type RaftLayerConfig struct {
	DataDir      string
	BindAddr     string
	AdvertAddr   string
	EnableSingle bool
}
*/

type RaftLayer struct {
	bindAddr   string
	advertAddr string

	config *raft.Config
	Raft   *raft.Raft

	peers *raft.JSONPeers

	f *fsm

	obChan chan raft.Observation

	rpc *ClusterRPC
}

func NewRaftLayer(enableSingle bool, raftDir string, bindAddr, advAddr string) (*RaftLayer, error) {
	rl := &RaftLayer{
		bindAddr: bindAddr,
		config:   raft.DefaultConfig(),
		f:        &fsm{},
	}

	rl.config.LogOutput = ioutil.Discard

	os.MkdirAll(raftDir, 0777)

	peers, err := readPeersJSON(filepath.Join(raftDir, "peers.json"))
	if err != nil {
		return nil, err
	}

	if enableSingle && len(peers) <= 1 {
		//s.logger.Println("enabling single-node mode")
		rl.config.EnableSingleNode = true
		rl.config.DisableBootstrapAfterElect = false
	}

	var ln net.Listener
	if ln, err = net.Listen("tcp", bindAddr); err != nil {
		return nil, err
	}

	//var advIp string
	var addr *net.TCPAddr
	if addr, err = rl.initAdvertAddr(advAddr); err != nil {
		return nil, err
	}
	log.Println("Advertising on:", addr.String())

	muxTrans := mux.NewMuxedTransport(ln, addr)
	go muxTrans.Serve()

	transport := raft.NewNetworkTransport(muxTrans.Listen(muxRaftHeader), 3, 10*time.Second, rl.config.LogOutput)

	// Create peer storage.
	rl.peers = raft.NewJSONPeers(raftDir, transport)

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate the Raft systems.
	if rl.Raft, err = raft.NewRaft(rl.config, rl.f, logStore, logStore, snapshots, rl.peers, transport); err != nil {
		return nil, fmt.Errorf("new raft: %s", err)
	}

	// RPC server
	rl.rpc = NewClusterRPC(muxTrans.Listen(muxRpcHeader))

	return rl, nil
}

func (rl *RaftLayer) RegisterRpcService(rs RPCService) {
	if rl.rpc.Service != nil {
		log.Println("RPC service already registered:", rl.rpc.Service)
		return
	}

	rl.rpc.Service = rs
	go func() {
		if err := rl.rpc.Serve(); err != nil {
			log.Println("Failed to start cluster RPC:", err.Error())
		}
	}()
}

func (rl *RaftLayer) WatchForStateChange() chan raft.Observation {
	if rl.obChan == nil {

		rl.obChan = make(chan raft.Observation)
		ob := raft.NewObserver(rl.obChan, false, func(o *raft.Observation) bool {

			_, ok := o.Data.(raft.RequestVoteRequest)

			return !ok
		})

		rl.Raft.RegisterObserver(ob)
	}

	return rl.obChan

}

func (rl *RaftLayer) Join(remote string) error {
	data := append([]byte{JoinRpcOp}, []byte(rl.advertAddr)...)
	_, err := rl.rpc.Send(remote, data, 3*time.Second)
	return err
}

// return all peers excluding self
func (rl *RaftLayer) Peers() []string {
	peers, err := rl.peers.Peers()
	if err != nil {
		log.Println("ERROR", err)
		return []string{}
	}
	if len(peers) < 1 {
		return peers
	}

	out := make([]string, len(peers)-1)
	if len(out) < 1 {
		return out
	}

	i := 0
	for _, peer := range peers {
		if peer != rl.advertAddr {
			out[i] = peer
			i++
		}
	}
	return out
}

func (rl *RaftLayer) ServeRPC(remote string, payload []byte) ([]byte, error) {
	var (
		err  error
		resp []byte
	)

	op := payload[0]
	switch op {
	case JoinRpcOp:
		upeer := string(payload[1:])

		// join on the network the request was made one
		ip := strings.Split(remote, ":")[0]
		port := strings.Split(upeer, ":")[1]
		if port == "" {
			err = fmt.Errorf("port required: %s", upeer)
			break
		}
		peer := ip + ":" + port

		if rl.havePeer(peer) {
			log.Println("Already have peer:", peer)
			break
		}

		if rl.Raft.State() == raft.Leader {

			log.Printf("Adding: peer=%s\n", peer)
			f := rl.Raft.AddPeer(peer)
			err = f.Error()
		} else {
			// forward to leader
			log.Println("Fowarding join request to", rl.Raft.Leader())
			_, err = rl.rpc.Send(rl.Raft.Leader(), payload, 3*time.Second)
		}

	default:
		err = fmt.Errorf("op not supported: %d", op)
	}

	if err != nil {
		log.Println("ERR", err)
	}

	return resp, err
}

func (rl *RaftLayer) havePeer(peer string) bool {
	peers, _ := rl.peers.Peers()
	for _, p := range peers {
		if p == peer {
			return true
		}
	}
	return false
}

func (rl *RaftLayer) initAdvertAddr(advAddr string) (*net.TCPAddr, error) {
	// Determine advertising address
	var err error
	if (strings.HasPrefix(rl.bindAddr, "0.0.0.0") || strings.HasPrefix(rl.bindAddr, ":")) && advAddr == "" {
		if rl.advertAddr, err = selectIpAddress(); err != nil {
			return nil, err
		}
		rl.advertAddr += ":" + strings.Split(rl.bindAddr, ":")[1]

	} else {
		rl.advertAddr = advAddr
	}

	return net.ResolveTCPAddr("tcp", rl.advertAddr)
}

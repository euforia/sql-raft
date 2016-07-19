package main

import (
	//"fmt"
	"log"
	"sync"
	"time"

	"github.com/euforia/sql-raft/raft"
	hraft "github.com/hashicorp/raft"
)

const (
	InitRaftState hraft.RaftState = 100
)

type NodeState byte

func (n NodeState) String() string {
	switch n {
	case FollowerState:
		return "FollowerState"
	case ReassignedState:
		return "ReassignedState"
	case DemotedState:
		return "DemotedState"
	case PromotedState:
		return "PromotedState"
	case LeaderState:
		return "LeaderState"
	case UnknownState:
		return "UnknownState"
	}

	return string(n)
}

func (n NodeState) LeaderState() bool {
	return n == LeaderState || n == PromotedState
}

const (
	// New follower
	FollowerState NodeState = iota
	// leader assignment for following node changed
	ReassignedState
	// leader that became follower
	DemotedState
	// follower that became leader
	PromotedState
	// new leader
	LeaderState
	//
	UnknownState
)

type Resource interface {
	// if leader is provider the resource should be started as the follower
	Start(leader ...string) error
	Stop() error
	Promote() error
	Demote(leader string) error
	// applicable to follower when leader changes
	Reassign(leader string) error
}

type stateMgr struct {
	raftlayer *raft.RaftLayer

	currRaftState hraft.RaftState // current known local state - start as follower
	lastRaftState hraft.RaftState // last state recieved

	stateChanged chan hraft.Observation

	mu sync.Mutex

	waitTime time.Duration
	tmr      *time.Timer

	// db instance to manage
	db Resource
	// leader with which the db is initialized with.
	// this is used to update the follower with a new leader
	dbLeader string
}

func newStateMgr(rl *raft.RaftLayer, waitTime int, db Resource) *stateMgr {
	return &stateMgr{
		raftlayer:     rl,
		stateChanged:  rl.WatchForStateChange(),
		currRaftState: InitRaftState,
		waitTime:      time.Duration(waitTime) * time.Second,
		db:            db,
	}
}

func (sm *stateMgr) State() hraft.RaftState {
	return sm.currRaftState
}

func (sm *stateMgr) IsFollower() bool {
	return sm.currRaftState == hraft.Follower
}

func (sm *stateMgr) IsLeader() bool {
	return sm.currRaftState == hraft.Leader
}

func (sm *stateMgr) setLastState(s hraft.RaftState) {
	sm.lastRaftState = s
}

func (sm *stateMgr) syncCurrState() {
	// sync state
	if sm.currRaftState != sm.lastRaftState {
		sm.mu.Lock()

		sm.currRaftState = sm.lastRaftState

		sm.mu.Unlock()
		log.Println("State synced")
	}
}

func (sm *stateMgr) isStateInSync() bool {
	return sm.currRaftState == sm.lastRaftState
}

// state the node transitioned to, leader
func (sm *stateMgr) getChangedNodeState() (NodeState, string) {
	var (
		newInst    = sm.currRaftState == InitRaftState && sm.currRaftState != sm.lastRaftState
		currLeader = sm.raftlayer.Raft.Leader()
		nstate     NodeState
	)

	switch sm.lastRaftState {

	case hraft.Follower:
		if newInst {
			nstate = FollowerState
		} else {
			if sm.isStateInSync() {
				nstate = ReassignedState
			} else {
				nstate = DemotedState
			}
		}

	case hraft.Leader:
		if newInst {
			nstate = LeaderState
		} else {
			nstate = PromotedState
		}

	default:
		nstate = UnknownState
		log.Println("Unknown:", sm.lastRaftState)

	}

	return nstate, currLeader
}

func (sm *stateMgr) handleStateChange() (bool, error) {
	var (
		err    error
		commit = true
	)

	nstate, currLeader := sm.getChangedNodeState()
	log.Println("Transition/State:", nstate)

	peers := sm.raftlayer.Peers()
	log.Println("Peers:", peers)

	switch nstate {
	case FollowerState:
		log.Printf("[FollowerState] New follower --leader--> %s\n", currLeader)
		err = sm.db.Start(currLeader)
		if err != nil {
			log.Fatal(err)
		}

	case ReassignedState:
		log.Printf("[ReassignedState] %s --> %s\n", sm.dbLeader, currLeader)
		err = sm.db.Reassign(currLeader)

	case DemotedState:
		log.Printf("[DemotedState] %s --to--> %s\n", sm.currRaftState, sm.lastRaftState)
		err = sm.db.Demote(currLeader)

	case LeaderState:
		log.Println("[LeaderState] New leader:", currLeader)
		err = sm.db.Start()
		if err != nil {
			log.Fatal(err)
		}

	case PromotedState:
		log.Printf("[PromotedState] %s --to--> %s\n", sm.currRaftState, sm.lastRaftState)
		err = sm.db.Promote()

	default:
		commit = false
	}

	if err == nil && commit {
		sm.mu.Lock()
		sm.dbLeader = currLeader
		sm.mu.Unlock()
	}

	return commit, err
}

func (sm *stateMgr) start() {
	sm.tmr = time.NewTimer(sm.waitTime)

	go func() {

		for {
			<-sm.tmr.C
			// No state change for leader
			if sm.isStateInSync() {
				// skip if state (leader) hasn't changed
				if sm.currRaftState == hraft.Leader {
					log.Printf("[%s] No state change (skipping)\n", sm.currRaftState)
					continue
				} else if sm.currRaftState == hraft.Follower {
					// skip if state and leader haven't changed
					if sm.dbLeader == sm.raftlayer.Raft.Leader() {
						log.Printf("[%s] No state change (skipping)\n", sm.currRaftState)
						continue
					}
				}
			}

			commit, err := sm.handleStateChange()
			if err == nil {
				if commit {
					sm.syncCurrState()
				}
				continue
			}

			log.Println(err)
		}
	}()

	for {
		lc := <-sm.stateChanged
		log.Printf("[DEBUG] State event: %+v\n", lc.Data)
		printRaftStats(sm.raftlayer)

		// record latest state
		sm.setLastState(lc.Raft.State())

		if sm.isStateInSync() {
			log.Printf("[%s] No state change (skipping)\n", sm.currRaftState)
			continue
		}

		sm.tmr.Reset(sm.waitTime)
	}
}

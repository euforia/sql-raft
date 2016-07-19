package raft

import (
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
)

type fsmSnap struct{}

func (f *fsmSnap) Persist(sink raft.SnapshotSink) error {

	err := func() error {
		// Encode data.
		b := []byte(`{}`)
		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}
		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnap) Release() {}

//
// fsm
//
type fsm struct {
	mu sync.Mutex
}

func (f *fsm) Apply(l *raft.Log) interface{} {
	log.Printf("[raft.fsm] data='%s' index=%d term=%d type=%v\n", l.Data, l.Index, l.Term, l.Type)
	/*
		op := l.Data[0]
		switch op {
		case OpWrite:
			log.Printf("Writing: %s bytes", len(l.Data[1:]))
		default:
			return fmt.Errorf("invalid op: %d", op)
		}
	*/
	return nil
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the datastore.
	//o := f.m.Snapshot()
	return &fsmSnap{}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	//o := NewInMemDatastore()
	//if err := json.NewDecoder(rc).Decode(&o.m); err != nil {
	//	return err
	//}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	//f.m = o

	return nil
}

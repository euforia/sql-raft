package mux

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	// DefaultTimeout is the default length of time to wait for first byte.
	DefaultTimeout = 30 * time.Second
)

type MuxedTransport struct {
	Timeout time.Duration // Connection timeout

	ln   net.Listener
	addr net.Addr
	wg   sync.WaitGroup
	m    map[byte]*listener // Registered listeners
}

// NewMux returns a new instance of Mux for ln. If adv is nil,
// then the addr of ln is used.
func NewMuxedTransport(ln net.Listener, adv net.Addr) *MuxedTransport {
	addr := adv
	if addr == nil {
		addr = ln.Addr()
	}

	return &MuxedTransport{
		ln:      ln,
		addr:    addr,
		m:       make(map[byte]*listener),
		Timeout: DefaultTimeout,
	}
}

func (mux *MuxedTransport) Listen(header byte) *Layer {
	if _, ok := mux.m[header]; ok {
		panic(fmt.Sprintf("Listener already registered: %d", header))
	}

	ln := &listener{make(chan net.Conn)}
	mux.m[header] = ln

	return &Layer{
		ln:     ln,
		header: header,
		addr:   mux.addr,
	}
}

func (mux *MuxedTransport) Serve() error {
	//mux.Logger.Printf("mux serving on %s, advertising %s", mux.ln.Addr().String(), mux.addr)

	for {
		// Wait for the next connection.
		// If it returns a temporary error then simply retry.
		// If it returns any other error then exit immediately.
		conn, err := mux.ln.Accept()
		if err, ok := err.(interface {
			Temporary() bool
		}); ok && err.Temporary() {
			continue
		}

		if err != nil {
			// Wait for all connections to be demuxed
			mux.wg.Wait()
			for _, ln := range mux.m {
				close(ln.c)
			}
			return err
		}

		// Demux in a goroutine to
		mux.wg.Add(1)
		go mux.handleConn(conn)
	}
}

func (mux *MuxedTransport) handleConn(conn net.Conn) {
	defer mux.wg.Done()

	// Set a read deadline so connections with no data don't timeout.
	if err := conn.SetReadDeadline(time.Now().Add(mux.Timeout)); err != nil {
		conn.Close()
		log.Printf("[ERROR] tcp.Mux: cannot set read deadline: %s", err)
		return
	}

	// Read first byte from connection to determine handler.
	var typ [1]byte
	if _, err := io.ReadFull(conn, typ[:]); err != nil {
		conn.Close()
		log.Printf("[ERROR] tcp.Mux: cannot read header byte: %s", err)
		return
	}

	// Reset read deadline and let the listener handle that.
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		conn.Close()
		log.Printf("[ERROR] tcp.Mux: cannot reset set read deadline: %s", err)
		return
	}

	// Retrieve handler based on first byte.
	handler := mux.m[typ[0]]
	if handler == nil {
		conn.Close()
		log.Printf("[ERROR] tcp.Mux: handler not registered: %d", typ[0])
		return
	}

	// Send connection to handler.  The handler is responsible for closing the connection.
	handler.c <- conn
}

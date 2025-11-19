package gossip

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/AyKrimino/kriminoDB/internal/store"
)

type Gossip struct {
	mu    sync.RWMutex
	peers []string
	store store.DB
}

func NewGossip(store store.DB) *Gossip {
	return &Gossip{
		store: store,
		peers: make([]string, 0, 20),
	}
}

func (g *Gossip) ListenAndAccept(peerHost string, peerPort string) error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", peerHost, peerPort))
	if err != nil {
		return err
	}
	log.Printf("[GOSSIP] Listening for peers on %s:%s", peerHost, peerPort)

	go g.startAcceptLoop(listener)

	return nil
}

func (g *Gossip) startAcceptLoop(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[GOSSIP] TCP accept error: %s", err)
		}

		go g.handleConn(conn)
	}
}

func (g *Gossip) handleConn(conn net.Conn) {
	defer conn.Close()
	log.Printf("[GOSSIP] New peer connection from %s", conn.RemoteAddr())
	// TODO: read JSON messages later
}

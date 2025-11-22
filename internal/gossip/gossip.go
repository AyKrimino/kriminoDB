// Package gossip implements a simple peer-to-peer gossip protocol for
// propagating messages between nodes.
package gossip

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"sync"

	"github.com/AyKrimino/kriminoDB/internal/store"
)

// Gossip represents a gossip that keeps track of peers list and a store.
type Gossip struct {
	addr string

	mu    sync.RWMutex
	peers []string
	store store.DB
}

func NewGossip(store store.DB, addr string) *Gossip {
	peers := make([]string, 0, 20)
	peers = append(peers, addr)

	return &Gossip{
		addr:  addr,
		store: store,
		peers: peers,
	}
}

func (g *Gossip) ListenAndAccept() error {
	listener, err := net.Listen("tcp", g.addr)
	if err != nil {
		return err
	}
	log.Printf("[GOSSIP] Listening for peers on %s", g.addr)

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

// handleConn handles a single incoming peer connection.
func (g *Gossip) handleConn(conn net.Conn) {
	defer conn.Close()
	log.Printf("[GOSSIP] New peer connection from %s", conn.RemoteAddr())

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Bytes()
		var joinMsg JoinMessage
		if err := json.Unmarshal(line, &joinMsg); err != nil {
			log.Printf("[GOSSIP] Unmarshal error: %s", err)
			continue
		}

		log.Printf("[GOSSIP] %+v received from %s", joinMsg, conn.RemoteAddr().String())

		g.updatePeersList(joinMsg.Sender)

		joinResMsg := NewJoinResponseMessage(g.peers)
		joinResMsgB, err := json.Marshal(joinResMsg)
		if err != nil {
			log.Printf("[GOSSIP] Marshal error: %s", err)
		}
		joinResMsgB = append(joinResMsgB, '\n')
		n, err := conn.Write(joinResMsgB)
		if err != nil {
			log.Printf("[GOSSIP] conn write error: %s", err)
		}
		log.Printf("[GOSSIP] %s sent from %s to %s", joinResMsgB[:n], g.addr, conn.RemoteAddr().String())
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[GOSSIP] Scanner error: %s", err)
	}
}

// Join connects this Gossip node to a bootstrap peer at the given address.
// It sends a JOIN message and updates its peers list with the JOIN_RESPONSE.
func (g *Gossip) Join(bootstrapAddr string) {
	conn, err := net.Dial("tcp", bootstrapAddr)
	if err != nil {
		log.Printf("[GOSSIP] TCP dial error: %s", err)
	}

	joinMsg := NewJoinMessage(g.addr)
	joinMsgB, err := json.Marshal(joinMsg)
	if err != nil {
		log.Printf("[GOSSIP] Marshal error: %s", err)
		return
	}

	joinMsgB = append(joinMsgB, '\n')
	n, err := conn.Write(joinMsgB)
	if err != nil {
		log.Printf("[GOSSIP] conn write error: %s", err)
		return
	}
	log.Printf("[GOSSIP] %s sent from %s to %s", joinMsgB[:n], conn.LocalAddr().String(), bootstrapAddr)

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		line := scanner.Bytes()
		var joinResMsg JoinResponseMessage
		if err := json.Unmarshal(line, &joinResMsg); err != nil {
			log.Printf("[GOSSIP] Unmarshal error: %s", err)
			return
		}
		log.Printf("[GOSSIP] %+v received from %s", joinResMsg, bootstrapAddr)

		g.updatePeersList(joinResMsg.Peers...)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[GOSSIP] Scanner error: %s", err)
	}
}

func (g *Gossip) updatePeersList(newPeers ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()

outer:
	for _, p := range newPeers {
		if p == g.addr {
			continue outer
		}
		for _, pp := range g.peers {
			if pp == p {
				continue outer
			}
		}
		g.peers = append(g.peers, p)
	}
}

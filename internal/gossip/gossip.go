package gossip

import (
	"encoding/json"
	"log"
	"net"
	"sync"

	"github.com/AyKrimino/kriminoDB/internal/store"
)

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

func (g *Gossip) handleConn(conn net.Conn) {
	defer conn.Close()
	log.Printf("[GOSSIP] New peer connection from %s", conn.RemoteAddr())

	// Receive JOIN message
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Printf("[GOSSIP] Read error: %s", err)
	}
	log.Printf("[GOSSIP] we've read %s", buf[:n])

	var joinMsg JoinMessage
	if err = json.Unmarshal(buf[:n], &joinMsg); err != nil {
		log.Printf("[GOSSIP] Unmarshal error: %s", err)
	}
	log.Printf("[GOSSIP] %+v received from %s", joinMsg, conn.RemoteAddr().String())

	// Update peers list
	g.updatePeersList(joinMsg.Sender)

	// send JOIN_RESPONSE message
	joinResMsg := NewJoinResponseMessage(g.peers)
	joinResMsgB, err := json.Marshal(joinResMsg)
	if err != nil {
		log.Printf("[GOSSIP] Marshal error: %s", err)
	}

	n, err = conn.Write(joinResMsgB)
	if err != nil {
		log.Printf("[GOSSIP] conn write error: %s", err)
	}
	log.Printf("[GOSSIP] %s sent from %s to %s", joinResMsgB[:n], g.addr, conn.RemoteAddr().String())
}

// Join dials bootstrap
func (g *Gossip) Join(bootstrapAddr string) {
	conn, err := net.Dial("tcp", bootstrapAddr)
	if err != nil {
		log.Printf("[GOSSIP] TCP dial error: %s", err)
	}

	// send JOIN message
	joinMsg := NewJoinMessage(g.addr)
	joinMsgB, err := json.Marshal(joinMsg)
	if err != nil {
		log.Printf("[GOSSIP] Marshal error: %s", err)
	}

	n, err := conn.Write(joinMsgB)
	if err != nil {
		log.Printf("[GOSSIP] conn write error: %s", err)
	}

	log.Printf("[GOSSIP] %s sent from %s to %s", joinMsgB[:n], conn.LocalAddr().String(), bootstrapAddr)

	// Receive JOIN_RESPONSE message
	buf := make([]byte, 1024)
	n, err = conn.Read(buf)
	if err != nil {
		log.Printf("[GOSSIP] Read error: %s", err)
	}
	log.Printf("[GOSSIP] we've read %s", buf[:n])

	var joinResMsg JoinResponseMessage
	if err = json.Unmarshal(buf[:n], &joinResMsg); err != nil {
		log.Printf("[GOSSIP] Unmarshal error: %s", err)
	}
	log.Printf("[GOSSIP] %+v received from %s", joinResMsg, conn.RemoteAddr().String())

	// Update peers list
	g.updatePeersList(joinResMsg.Peers...)
}

func (g *Gossip) updatePeersList(newPeers ...string) {
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

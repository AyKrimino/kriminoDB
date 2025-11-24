// Package gossip implements a simple peer-to-peer gossip protocol for
// propagating messages between nodes.
package gossip

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"sync"
	"time"

	"github.com/AyKrimino/kriminoDB/internal/store"
	"github.com/AyKrimino/kriminoDB/internal/utils"
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

func (g *Gossip) ListenAndAccept(ready chan<- struct{}) error {
	listener, err := net.Listen("tcp", g.addr)
	if err != nil {
		return err
	}
	log.Printf("[GOSSIP] Listening for peers on %s", g.addr)

	if ready != nil {
		close(ready)
	}

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

		var header MessageHeader
		if err := json.Unmarshal(line, &header); err != nil {
			log.Printf("[GOSSIP] Unmarshal error: %s", err)
			continue
		}

		switch header.Type {
		case Join:
			g.handleJoinMessage(conn, line)
		case Update:
			g.handleUpdateMessage(conn, line)
		default:
			log.Printf("[GOSSIP] Unknown message type: %s", header.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[GOSSIP] Scanner error: %s", err)
	}
}

func (g *Gossip) handleUpdateMessage(conn net.Conn, line []byte) {
	var updateMsg UpdateMessage
	if err := json.Unmarshal(line, &updateMsg); err != nil {
		log.Printf("[GOSSIP] Unmarshal error: %s", err)
		return
	}
	log.Printf("[GOSSIP] %+v received from %s", updateMsg, conn.RemoteAddr().String())

	_, exists := g.store.Get(updateMsg.Key)
	if !exists {
		g.store.Set(updateMsg.Key, updateMsg.DataValue.Value)
	} else {
		g.store.Update(updateMsg.Key, updateMsg.DataValue)
	}
}

func (g *Gossip) handleJoinMessage(conn net.Conn, line []byte) {
	var joinMsg JoinMessage
	if err := json.Unmarshal(line, &joinMsg); err != nil {
		log.Printf("[GOSSIP] Unmarshal error: %s", err)
		return
	}

	log.Printf("[GOSSIP] %+v received from %s", joinMsg, conn.RemoteAddr().String())

	g.updatePeersList(joinMsg.Sender)

	g.mu.RLock()
	joinResMsg := NewJoinResponseMessage(g.peers)
	g.mu.RUnlock()

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

// Join connects this Gossip node to a bootstrap peer at the given address.
// It sends a JOIN message and updates its peers list with the JOIN_RESPONSE.
func (g *Gossip) Join(bootstrapAddr string) {
	conn, err := net.DialTimeout("tcp", bootstrapAddr, 3*time.Second)
	if err != nil {
		log.Printf("[GOSSIP] TCP dial error: %s", err)
		return
	}
	defer conn.Close()

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

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		line := scanner.Bytes()
		var joinResMsg JoinResponseMessage
		if err := json.Unmarshal(line, &joinResMsg); err != nil {
			log.Printf("[GOSSIP] Unmarshal error: %s", err)
			return
		}
		log.Printf("[GOSSIP] %+v received from %s", joinResMsg, bootstrapAddr)

		g.mu.RLock()
		if !joinResMsg.Validate() {
			log.Printf("[GOSSIP] invalid peers list.")
			return
		}
		g.mu.RUnlock()

		g.updatePeersList(joinResMsg.Peers...)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[GOSSIP] Scanner error: %s", err)
		return
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

	log.Printf("[GOSSIP] updated peers list %+v", g.peers)
}

func (g *Gossip) getDistinctRandomPeers(n int) []string {
	var randomPeers []string

outer:
	for {
		randomPeers = utils.GetRandomItems(g.peers, n)
		for _, p := range randomPeers {
			if p == g.addr {
				continue outer
			}
		}
		break outer
	}
	return randomPeers
}

func (g *Gossip) pushUpdate(key string, dataValue store.DataValue) {
	randomPeers := g.getDistinctRandomPeers(2)

	for _, p := range randomPeers {
		conn, err := net.DialTimeout("tcp", p, 3*time.Second)
		if err != nil {
			log.Printf("[GOSSIP] TCP dial error: %s", err)
		}

		conn.SetReadDeadline(time.Now().Add(3 * time.Second))

		updateMsg := NewUpdateMessge(key, dataValue)

		updateMsgB, err := json.Marshal(updateMsg)
		if err != nil {
			log.Printf("[GOSSIP] Marshal error: %s", err)
			continue
		}

		n, err := conn.Write(updateMsgB)
		if err != nil {
			log.Printf("[GOSSIP] conn write error: %s", err)
		}
		log.Printf("[GOSSIP] update message %s sent to %s", updateMsgB[:n], p)

		conn.Close()
	}
}

func (g *Gossip) Replicate(key string, dataValue store.DataValue) {
	g.pushUpdate(key, dataValue)
}

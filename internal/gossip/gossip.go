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

	ticker *time.Ticker
	stopCh chan struct{}

	mu               sync.RWMutex
	peers            []string
	store            store.DB
	seenMessageIDs   map[string]bool
	peersLastContact map[string]time.Time
	warnedPeers      map[string]bool
}

func NewGossip(store store.DB, addr string) *Gossip {
	peers := make([]string, 0, 20)
	peers = append(peers, addr)

	return &Gossip{
		addr:             addr,
		ticker:           time.NewTicker(2 * time.Second),
		stopCh:           make(chan struct{}),
		store:            store,
		peers:            peers,
		seenMessageIDs:   make(map[string]bool, 1000),
		peersLastContact: make(map[string]time.Time, 20),
		warnedPeers:      make(map[string]bool, 20),
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

	go g.sendGossip()

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

	scanner := bufio.NewScanner(conn)
	firstMessage := true

	for scanner.Scan() {
		line := scanner.Bytes()

		var header MessageHeader
		if err := json.Unmarshal(line, &header); err != nil {
			log.Printf("[GOSSIP] Unmarshal error: %s", err)
			continue
		}

		if firstMessage && header.Type == JoinType {
			log.Printf("[GOSSIP] New peer connection from %s joining the cluster", conn.RemoteAddr())
		}

		switch header.Type {
		case JoinType:
			g.handleJoinMessage(conn, line)
		case UpdateType:
			g.handleUpdateMessage(conn, line)
		case GossipType:
			g.handleGossipMessage(conn, line)
		case HeartbeatType:
			g.handleHeartbeatMessage(line)
		default:
			log.Printf("[GOSSIP] Unknown message type: %s", header.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[GOSSIP] Scanner error: %s", err)
	}
}

func (g *Gossip) handleHeartbeatMessage(line []byte) {
	var heartbeatMsg HeartbeatMessage
	if err := json.Unmarshal(line, &heartbeatMsg); err != nil {
		log.Printf("[GOSSIP] Unmarshal error: %s", err)
		return
	}
	log.Printf("[GOSSIP] Heartbeat from %s", heartbeatMsg.Sender)
	g.updatePeersLastSeenTime(heartbeatMsg.Sender)
}

func (g *Gossip) handleUpdateMessage(conn net.Conn, line []byte) {
	var updateMsg UpdateMessage
	if err := json.Unmarshal(line, &updateMsg); err != nil {
		log.Printf("[GOSSIP] Unmarshal error: %s", err)
		return
	}
	log.Printf("[GOSSIP] %+v received from %s", updateMsg, conn.RemoteAddr().String())

	g.updatePeersLastSeenTime(conn.RemoteAddr().String())

	_, exists := g.store.Get(updateMsg.Key)
	if !exists {
		g.store.Set(updateMsg.Key, updateMsg.DataValue.Value)
	} else {
		err := g.store.Update(updateMsg.Key, updateMsg.DataValue)
		if err != nil {
			log.Printf("[GOSSIP] error updating value: %s", err)
		}
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
	g.updatePeersLastSeenTime(joinMsg.Sender)

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
		return
	}
	log.Printf("[GOSSIP] %s sent from %s to %s", joinResMsgB[:n], g.addr, conn.RemoteAddr().String())
}

func (g *Gossip) markSeen(id string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.seenMessageIDs[id] {
		return false
	}
	g.seenMessageIDs[id] = true
	return true
}

func (g *Gossip) handleGossipMessage(conn net.Conn, line []byte) {
	var gossipMsg GossipMessage
	if err := json.Unmarshal(line, &gossipMsg); err != nil {
		log.Printf("[GOSSIP] Unmarshal error: %s", err)
		return
	}

	if !g.markSeen(string(gossipMsg.MessageID)) {
		return
	}

	if len(gossipMsg.Updates) > 0 || g.peerListChanged(gossipMsg.Peers) {
		log.Printf("[GOSSIP] %+v received new GOSSIP message from %s", gossipMsg, conn.RemoteAddr().String())
	}

	for key, dv := range gossipMsg.Updates {
		g.store.Update(key, dv)
	}

	g.updatePeersList(gossipMsg.Peers...)
	g.updatePeersLastSeenTime(gossipMsg.Peers...)
}

// peerListChanged checks if peers argument has peers
// that does not exist in the Gossip peers list
func (g *Gossip) peerListChanged(peers []string) bool {
	if len(peers) != len(g.peers) {
		return true
	}

	m := make(map[string]bool, len(g.peers))
	for _, p := range g.peers {
		m[p] = true
	}

	for _, p := range peers {
		if _, ok := m[p]; !ok {
			return true
		}
	}
	return false
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
		g.updatePeersLastSeenTime(joinResMsg.Peers...)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("[GOSSIP] Scanner error: %s", err)
		return
	}
}

func (g *Gossip) updatePeersLastSeenTime(peers ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	for _, p := range peers {
		g.peersLastContact[p] = now
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

			removed := g.removeDeadPeer(p)
			if removed {
				log.Printf("[GOSSIP] removed dead peer %s from peer list", p)
			}
			continue
		}

		updateMsg := NewUpdateMessge(key, dataValue)

		updateMsgB, err := json.Marshal(updateMsg)
		if err != nil {
			log.Printf("[GOSSIP] Marshal error: %s", err)
			continue
		}

		n, err := conn.Write(updateMsgB)
		if err != nil {
			log.Printf("[GOSSIP] conn write error: %s", err)
			continue
		}
		log.Printf("[GOSSIP] update message %s sent to %s", updateMsgB[:n], p)

		conn.Close()
	}
}

func (g *Gossip) Replicate(key string, dataValue store.DataValue) {
	g.pushUpdate(key, dataValue)
}

func (g *Gossip) gossipToRandomPeers(pendingUpdates map[string]store.DataValue) {
	g.mu.RLock()
	if len(g.peers) == 0 {
		return
	}
	g.mu.RUnlock()

	randomPeers := g.getDistinctRandomPeers(3)
	for _, p := range randomPeers {
		conn, err := net.DialTimeout("tcp", p, 3*time.Second)
		if err != nil {
			log.Printf("[GOSSIP] TCP dial error: %s", err)

			removed := g.removeDeadPeer(p)
			if removed {
				log.Printf("[GOSSIP] removed dead peer %s from peer list", p)
			}
			continue
		}

		gossipMsg := NewGossipMessage(pendingUpdates, g.peers)
		gossipMsgB, err := json.Marshal(gossipMsg)
		if err != nil {
			log.Printf("[GOSSIP] Marshal error: %s", err)
			continue
		}

		_, err = conn.Write(gossipMsgB)
		if err != nil {
			log.Printf("[GOSSIP] conn write error: %s", err)
			continue
		}

		conn.Close()
	}
}

func (g *Gossip) checkPeerLiveness() {
	g.mu.RLock()
	peersSnapshot := make([]string, len(g.peers))
	copy(peersSnapshot, g.peers)
	g.mu.RUnlock()

	for _, p := range peersSnapshot {
		if p == g.addr {
			continue
		}

		g.mu.RLock()
		t, ok := g.peersLastContact[p]
		if !ok {
			continue
		}
		g.mu.RUnlock()

		since := time.Since(t)

		if since < 10*time.Second {
			g.mu.Lock()
			delete(g.warnedPeers, p)
			g.mu.Unlock()
		}

		if since > 10*time.Second && since <= 20*time.Second {
			g.mu.RLock()
			alreadyWarned := g.warnedPeers[p]
			g.mu.RUnlock()

			if !alreadyWarned {
				g.mu.Lock()
				g.warnedPeers[p] = true
				g.mu.Unlock()
				log.Printf("[GOSSIP] [WARN] Peer %s unresponsive - removing in %v", p, 20*time.Second-since)
			}
		}

		if since > 20*time.Second {
			removed := g.removeDeadPeer(p)
			if removed {
				log.Printf("[GOSSIP] removed dead peer %s from peer list", p)
			}
		}
	}
}

func (g *Gossip) sendGossip() {
	defer close(g.stopCh)

	for {
		select {
		case <-g.stopCh:
			g.ticker.Stop()
			return
		case <-g.ticker.C:
			pendingUpdates := g.store.(*store.Store).GetPendingUpdates()

			g.sendHeartbeat()

			g.gossipToRandomPeers(pendingUpdates)
			g.checkPeerLiveness()
		}
	}
}

func (g *Gossip) removeDeadPeer(peer string) bool {
	removeIdx := -1
	for idx, p := range g.peers {
		if p == peer {
			removeIdx = idx
			break
		}
	}

	if removeIdx != -1 {
		g.peers = append(g.peers[:removeIdx], g.peers[removeIdx+1:]...)
		delete(g.peersLastContact, peer)
		return true
	}
	return false
}

func (g *Gossip) sendHeartbeat() {
	peers := g.getDistinctRandomPeers(1)
	if len(peers) == 0 {
		return
	}

	peer := peers[0]

	conn, err := net.DialTimeout("tcp", peer, 3*time.Second)
	if err != nil {
		log.Printf("[GOSSIP] TCP dial error: %s", err)
		g.removeDeadPeer(peer)
		return
	}
	defer conn.Close()

	heartbeatMsg := NewHeartbeatMessage(g.addr)
	heartbeatMsgB, err := json.Marshal(heartbeatMsg)
	if err != nil {
		log.Printf("[GOSSIP] Marshal error: %s", err)
		return
	}

	_, err = conn.Write(heartbeatMsgB)
	if err != nil {
		log.Printf("[GOSSIP] conn write error: %s", err)
		return
	}
	// log.Printf("[GOSSIP] %s sent from %s to %s", heartbeatMsgB[:n], g.addr, peer)
}

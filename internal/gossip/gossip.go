package gossip

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/AyKrimino/kriminoDB/internal/store"
)

type Gossip struct {
	listenAddr net.Addr

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

	g.listenAddr = listener.Addr()

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
}

// Join dials bootstrap
func (g *Gossip) Join(bootstrapAddr string) {
	conn, err := net.Dial("tcp", bootstrapAddr)
	if err != nil {
		log.Printf("[GOSSIP] TCP dial error: %s", err)
	}

	// send JOIN message
	joinMsg := NewJoinMessage(g.listenAddr.String())
	joinMsgB, err := json.Marshal(joinMsg)
	if err != nil {
		log.Printf("[GOSSIP] Marshal error: %s", err)
	}

	n, err := conn.Write(joinMsgB)
	if err != nil {
		log.Printf("[GOSSIP] conn write error: %s", err)
	}

	log.Printf("[GOSSIP] %s sent from %s to %s", joinMsgB[:n], conn.LocalAddr().String(), bootstrapAddr)
}

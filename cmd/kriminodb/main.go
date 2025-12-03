package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/AyKrimino/kriminoDB/internal/gossip"
	"github.com/AyKrimino/kriminoDB/internal/server"
	"github.com/AyKrimino/kriminoDB/internal/store"
)

func main() {
	clientPort := flag.String("client-port", "3000", "Client port for server to listen")
	peerPort := flag.String("peer-port", "4000", "Gossip port for nodes communication")
	bootstrapAddr := flag.String("bootstrap", "", "Address of existing node to join (like localhost:4000)")

	flag.Parse()

	conf := server.Config{
		Host: "localhost",
		Port: *clientPort,
	}

	st := store.NewStore(nil)

	g := gossip.NewGossip(st, fmt.Sprintf("localhost:%s", *peerPort))

	st.(*store.Store).SetReplicator(g)

	ready := make(chan struct{})
	go func() {
		if err := g.ListenAndAccept(ready); err != nil {
			log.Fatalf("[GOSSIP] Fatal error: %v", err)
		}
	}()
	<-ready

	if *bootstrapAddr != "" {
		log.Printf("[GOSSIP] Joining cluster via bootstrap node %s", *bootstrapAddr)
		g.Join(*bootstrapAddr)
	}

	serverErr := make(chan error, 1)
	go func() {
		srv := server.NewServer(st, conf)
		serverErr <- srv.Start()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		if err != nil {
			log.Printf("[SERVER] Error: %s", err)
		}
	case sig := <-sigCh:
		log.Printf("[SERVER] Received signal %s, shutting down...", sig)
	}

	g.Stop()
}

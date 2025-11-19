package main

import (
	"flag"
	"log"

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

	st := store.NewStore()

	g := gossip.NewGossip(st)
	go func() {
		if err := g.ListenAndAccept("localhost", *peerPort); err != nil {
			log.Fatalf("[GOSSIP] Fatal error: %v", err)
		}
	}()

	if *bootstrapAddr != "" {
		log.Printf("Joining cluster via bootstrap node %s", *bootstrapAddr)
		// TODO: gossip join bootstrap
	}

	srv := server.NewServer(st, conf)
	err := srv.Start()
	if err != nil {
		log.Fatal(err)
	}
}

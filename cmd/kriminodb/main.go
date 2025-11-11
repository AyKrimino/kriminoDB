package main

import (
	"log"

	"github.com/AyKrimino/kriminoDB/internal/server"
	"github.com/AyKrimino/kriminoDB/internal/store"
)

func main() {
	conf := server.Config{
		Host: "localhost",
		Port: "3000",
	}

	st := store.NewStore()
	srv := server.NewServer(st, conf)

	err := srv.Start()
	if err != nil {
		log.Fatal(err)
	}
}

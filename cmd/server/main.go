package main

import (
	"flag"
	"log"
	"os"

	"miniquic/link"
	"miniquic/transport"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	out := flag.String("out", "out.bin", "output file")
	flag.Parse()

	l, err := link.ListenUDP(*addr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if err := transport.Receive(l, f); err != nil {
		log.Fatal(err)
	}
}

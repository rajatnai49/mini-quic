package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"miniquic/link"
	"miniquic/transport"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "server address")
	file := flag.String("file", "in.bin", "file to send")
	flag.Parse()

	f, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fi, _ := f.Stat()

	l, err := link.DialUDP(*addr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	start := time.Now()
	if err := transport.Send(l, f); err != nil {
		log.Fatal(err)
	}

	el := time.Since(start).Seconds()
	mbps := float64(fi.Size()) * 8 / el / 1e6
	fmt.Printf("%.3f Mbps\n", mbps)
	fmt.Fprintf(os.Stderr, "bytes=%d time=%.2fs\n", fi.Size(), el)
}

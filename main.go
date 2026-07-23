package main

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Node struct {
	ID     int
	Server net.Listener
	peers  []string
}

func createProcess(id *int, peers []string) {
	server, err := net.Listen("tcp", "localhost:900"+strconv.Itoa(*id))
	if err != nil {
		fmt.Println(err)
		return
	}
	node := Node{ID: *id, Server: server, peers: peers}
	for {
		_, _ = node.Server.Accept()
	}
}

func main() {
	id := flag.Int("id", 0, "this node's ID")
	peers := flag.String("peers", "", "comma-separated peer addresses")
	flag.Parse()
	go createProcess(id, strings.Split(*peers, ","))
	select {}
}

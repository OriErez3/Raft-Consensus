package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	ID    int
	peers []string
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type AppendEntriesArgs struct {
	Term int
}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	log.Printf("node %d recieved AppendEntries from term %d", n.ID, args.Term)
	return nil
}

func accept(listener net.Listener) {
	for {
		con, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go rpc.ServeConn(con)
	}
}

func caller(callAddy string, args AppendEntriesArgs, carry chan string) {
	client, err := rpc.Dial("tcp", callAddy)
	if err != nil {
		fmt.Println(err)
		carry <- "Failure, dial issue"
		return
	}
	var reply AppendEntriesReply
	err = client.Call("Node.AppendEntries", args, &reply)
	if err != nil {
		carry <- "Failure, Call didn't work."
		return
	}
	carry <- "Success"
	defer client.Close()

}

func main() {
	id := flag.Int("id", 0, "this node's ID")
	peers := flag.String("peers", "", "comma-separated peer addresses")
	flag.Parse()
	server, err := net.Listen("tcp", "localhost:900"+strconv.Itoa(*id))
	if err != nil {
		fmt.Println(err)
		return
	}
	list_of_peers := strings.Split(*peers, ",")
	node := Node{ID: *id, peers: list_of_peers}
	carry := make(chan string)
	err = rpc.Register(&node)
	if err != nil {
		fmt.Println(err)
		return
	}
	go accept(server)
	args := AppendEntriesArgs{1}
	time.Sleep(time.Second * 10)
	for index, peer := range node.peers {
		if index == node.ID {
			continue
		}
		go caller(peer, args, carry)
	}
	for ret := range carry {
		fmt.Println(ret)
	}
}

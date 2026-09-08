package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Node struct {
	//A server
	ID          int
	peers       []string
	role        int //0 = Follower, 1 = Candidate, 2 = Leader
	currentTerm int
	Votedfor    int //-1 = Voted for no one
	mu          sync.Mutex
}

func (n *Node) increaseTerm(node Node) {
	node.mu.Lock()
	node.currentTerm += 1
	node.Votedfor = -1
	node.mu.Unlock()
}

type AppendEntriesReply struct {
	//Followers response to the leaders command
	Term    int
	Success bool
}

type AppendEntriesArgs struct {
	//Command the leader sends out and the follower needs to respond to
	Term int
}

func (n *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	//The function to tell followers what to do, and get the response.
	log.Printf("node %d recieved AppendEntries from term %d", n.ID, args.Term)
	return nil
}

// Accepts a connection.
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

// Tells other nodes what to do.
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

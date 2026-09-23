package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

var minTimeOut time.Duration = 150 * time.Millisecond
var maxTimeOut time.Duration = 300 * time.Millisecond

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return fmt.Sprintf("Role(%d)", int(r))
}

type Node struct {
	//A server
	ID          int
	peers       []string
	role        Role
	currentTerm int
	votedFor    int //-1 = Voted for no one
	mu          sync.Mutex
	lastHeard   time.Time
	timeOut     time.Duration
	VoteCounter int
}
type RequestVoteArgs struct {
	ID   int
	Term int
}
type RequestVoteReply struct {
	Term    int
	Granted bool
}

func (n *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if args.Term < n.currentTerm {
		reply.Granted = false
		reply.Term = n.currentTerm
		return nil
	}
	if args.Term > n.currentTerm {
		n.votedFor = -1
		n.role = Follower
		n.currentTerm = args.Term
	}
	if n.votedFor == -1 || n.votedFor == args.ID {
		reply.Granted = true
		n.votedFor = args.ID
		n.lastHeard = time.Now()
	}
	reply.Term = n.currentTerm
	return nil

}

func (n *Node) increaseTermLocked() {
	//Use function when the node is locked
	n.currentTerm += 1
	n.votedFor = -1
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

func (n *Node) ElectionTimer() {
	for {
		time.Sleep(time.Millisecond * 10)
		n.mu.Lock()
		switch role := n.role; {
		case role == Leader:
			n.mu.Unlock()
			continue
		default:
			if time.Since(n.lastHeard) > n.timeOut {
				n.increaseTermLocked()
				n.votedFor = n.ID
				n.role = Candidate
				n.VoteCounter = 1
				temp := n.currentTerm
				n.lastHeard = time.Now()
				n.timeOut = minTimeOut + rand.N(maxTimeOut-minTimeOut)
				fmt.Println(n.votedFor, n.role, n.currentTerm, n.timeOut)
				args := RequestVoteArgs{ID: n.ID, Term: temp}
				for i, v := range n.peers {
					if i != n.ID {
						go n.ReqVoteCaller(v, args)
					}
				}
			}

			n.mu.Unlock()
		}

	}
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
func (n *Node) ReqVoteCaller(callAddy string, args RequestVoteArgs) {
	client, err := rpc.Dial("tcp", callAddy)
	if err != nil {
		return
	}
	defer client.Close()
	var reply RequestVoteReply
	err = client.Call("Node.RequestVote", args, &reply)
	if err != nil {
		return

	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.currentTerm = reply.Term
		n.role = Follower
		n.votedFor = -1

		return
	}
	if n.role != Candidate {

		return
	}
	if n.currentTerm != args.Term {

		return
	}
	if reply.Granted {
		n.VoteCounter += 1
		if n.VoteCounter >= (len(n.peers)/2)+1 {
			n.role = Leader
		}
	}
	return

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
	randomDuration := minTimeOut + rand.N(maxTimeOut-minTimeOut)
	node := Node{ID: *id, peers: list_of_peers, lastHeard: time.Now(), timeOut: randomDuration}
	go node.ElectionTimer()
	//carry := make(chan string)
	err = rpc.Register(&node)
	if err != nil {
		fmt.Println(err)
		return
	}
	go accept(server)
	select {}
	// args := AppendEntriesArgs{1}
	// time.Sleep(time.Second * 10)
	// for index, peer := range node.peers {
	// 	if index == node.ID {
	// 		continue
	// 	}
	// 	go caller(peer, args, carry)
	// }
	// for ret := range carry {
	// 	fmt.Println(ret)
	// }
}

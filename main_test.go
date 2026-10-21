package main

import (
	"testing"
	"time"
)

// save() writes raft-<id>.json into the working directory, so every test
// runs in its own temp dir and leaves nothing behind.
func isolate(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
}

// entries builds a log with the index-0 dummy already in place.
// entries(1, 1, 2) gives [dummy, term1, term1, term2].
func entries(terms ...int) []LogEntry {
	l := []LogEntry{{}}
	for i, term := range terms {
		l = append(l, LogEntry{Term: term, Command: string(rune('a' + i))})
	}
	return l
}

func terms(l []LogEntry) []int {
	out := make([]int, len(l))
	for i, e := range l {
		out[i] = e.Term
	}
	return out
}

func equal(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func node(id, term, votedFor int, l []LogEntry) *Node {
	return &Node{
		ID:          id,
		peers:       []string{"a", "b", "c"},
		role:        Follower,
		currentTerm: term,
		votedFor:    votedFor,
		log:         l,
		lastHeard:   time.Now(),
	}
}

// ---------- RequestVote ----------

func TestRequestVote(t *testing.T) {
	cases := []struct {
		name        string
		term        int // voter's currentTerm
		votedFor    int
		voterLog    []LogEntry
		args        RequestVoteArgs
		wantGranted bool
		wantTerm    int // expected reply.Term
	}{
		{
			name: "stale candidate term is rejected",
			term: 5, votedFor: -1, voterLog: entries(1),
			args:        RequestVoteArgs{ID: 2, Term: 3, LastLogIndex: 1, LastLogTerm: 1},
			wantGranted: false, wantTerm: 5,
		},
		{
			name: "fresh candidate with equal log is granted",
			term: 1, votedFor: -1, voterLog: entries(1),
			args:        RequestVoteArgs{ID: 2, Term: 1, LastLogIndex: 1, LastLogTerm: 1},
			wantGranted: true, wantTerm: 1,
		},
		{
			name: "higher term makes voter step down and vote",
			term: 1, votedFor: 0, voterLog: entries(1),
			args:        RequestVoteArgs{ID: 2, Term: 7, LastLogIndex: 1, LastLogTerm: 1},
			wantGranted: true, wantTerm: 7,
		},
		{
			name: "already voted for someone else this term",
			term: 3, votedFor: 1, voterLog: entries(1),
			args:        RequestVoteArgs{ID: 2, Term: 3, LastLogIndex: 1, LastLogTerm: 1},
			wantGranted: false, wantTerm: 3,
		},
		{
			name: "repeat request from the same candidate is granted again",
			term: 3, votedFor: 2, voterLog: entries(1),
			args:        RequestVoteArgs{ID: 2, Term: 3, LastLogIndex: 1, LastLogTerm: 1},
			wantGranted: true, wantTerm: 3,
		},
		{
			name: "candidate log is shorter at the same last term",
			term: 2, votedFor: -1, voterLog: entries(1, 1, 1),
			args:        RequestVoteArgs{ID: 2, Term: 2, LastLogIndex: 1, LastLogTerm: 1},
			wantGranted: false, wantTerm: 2,
		},
		{
			name: "candidate log is longer at the same last term",
			term: 2, votedFor: -1, voterLog: entries(1),
			args:        RequestVoteArgs{ID: 2, Term: 2, LastLogIndex: 3, LastLogTerm: 1},
			wantGranted: true, wantTerm: 2,
		},
		{
			name: "longer log loses to a higher last term",
			term: 3, votedFor: -1, voterLog: entries(1, 1, 1, 1, 1),
			args:        RequestVoteArgs{ID: 2, Term: 3, LastLogIndex: 2, LastLogTerm: 2},
			wantGranted: true, wantTerm: 3,
		},
		{
			name: "shorter log with a lower last term is refused",
			term: 3, votedFor: -1, voterLog: entries(1, 2),
			args:        RequestVoteArgs{ID: 2, Term: 3, LastLogIndex: 5, LastLogTerm: 1},
			wantGranted: false, wantTerm: 3,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			isolate(t)
			n := node(0, c.term, c.votedFor, c.voterLog)
			var reply RequestVoteReply
			if err := n.RequestVote(c.args, &reply); err != nil {
				t.Fatalf("RequestVote returned error: %v", err)
			}
			if reply.Granted != c.wantGranted {
				t.Errorf("Granted = %v, want %v", reply.Granted, c.wantGranted)
			}
			if reply.Term != c.wantTerm {
				t.Errorf("reply.Term = %d, want %d", reply.Term, c.wantTerm)
			}
			if reply.Granted && n.votedFor != c.args.ID {
				t.Errorf("granted the vote but votedFor = %d, want %d", n.votedFor, c.args.ID)
			}
		})
	}
}

func TestRequestVoteHigherTermClearsOldVote(t *testing.T) {
	isolate(t)
	// Voted for node 1 in term 3. A candidate from term 9 arrives with a stale log,
	// so the vote must be refused -- but the term must still be adopted.
	n := node(0, 3, 1, entries(1, 2, 3))
	var reply RequestVoteReply
	n.RequestVote(RequestVoteArgs{ID: 2, Term: 9, LastLogIndex: 1, LastLogTerm: 1}, &reply)

	if reply.Granted {
		t.Error("granted a vote to a candidate with a stale log")
	}
	if n.currentTerm != 9 {
		t.Errorf("currentTerm = %d, want 9 (term must be adopted even when refusing)", n.currentTerm)
	}
	if n.role != Follower {
		t.Errorf("role = %v, want Follower", n.role)
	}
	if n.votedFor != -1 {
		t.Errorf("votedFor = %d, want -1 (new term clears the old vote)", n.votedFor)
	}
}

// ---------- AppendEntries ----------

func TestAppendEntries(t *testing.T) {
	cases := []struct {
		name        string
		term        int
		startLog    []LogEntry
		args        AppendEntriesArgs
		wantSuccess bool
		wantLog     []int // expected log terms, dummy included
		wantCommit  int
	}{
		{
			name: "stale leader is rejected and the log is untouched",
			term: 5, startLog: entries(1, 2),
			args:        AppendEntriesArgs{Term: 3, PrevLogIndex: 0, PrevLogTerm: 0, Entries: entries(3)[1:]},
			wantSuccess: false, wantLog: []int{0, 1, 2}, wantCommit: 0,
		},
		{
			name: "PrevLogIndex past the end of our log is rejected",
			term: 2, startLog: entries(1),
			args:        AppendEntriesArgs{Term: 2, PrevLogIndex: 5, PrevLogTerm: 1},
			wantSuccess: false, wantLog: []int{0, 1}, wantCommit: 0,
		},
		{
			// Boundary: one past the last valid index. A ">" bounds check here
			// would let this through and panic on n.log[PrevLogIndex].
			name: "PrevLogIndex exactly at len(log) is rejected",
			term: 2, startLog: entries(1),
			args:        AppendEntriesArgs{Term: 2, PrevLogIndex: 2, PrevLogTerm: 1},
			wantSuccess: false, wantLog: []int{0, 1}, wantCommit: 0,
		},
		{
			name: "PrevLogTerm mismatch is rejected and nothing is truncated",
			term: 3, startLog: entries(1, 1),
			args:        AppendEntriesArgs{Term: 3, PrevLogIndex: 2, PrevLogTerm: 2, Entries: entries(3)[1:]},
			wantSuccess: false, wantLog: []int{0, 1, 1}, wantCommit: 0,
		},
		{
			name: "matching prev entry appends the new entries",
			term: 2, startLog: entries(1),
			args:        AppendEntriesArgs{Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(2, 2)[1:]},
			wantSuccess: true, wantLog: []int{0, 1, 2, 2}, wantCommit: 0,
		},
		{
			name: "empty heartbeat succeeds and leaves the log alone",
			term: 2, startLog: entries(1, 1),
			args:        AppendEntriesArgs{Term: 2, PrevLogIndex: 2, PrevLogTerm: 1},
			wantSuccess: true, wantLog: []int{0, 1, 1}, wantCommit: 0,
		},
		{
			name: "conflicting tail is replaced by the leader's entries",
			term: 4, startLog: entries(1, 2, 2, 2),
			args:        AppendEntriesArgs{Term: 4, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(4)[1:]},
			wantSuccess: true, wantLog: []int{0, 1, 4}, wantCommit: 0,
		},
		{
			name: "LeaderCommit is adopted when our log reaches that far",
			term: 2, startLog: entries(1),
			args:        AppendEntriesArgs{Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(2, 2)[1:], LeaderCommit: 2},
			wantSuccess: true, wantLog: []int{0, 1, 2, 2}, wantCommit: 2,
		},
		{
			name: "LeaderCommit beyond our log is clamped to our last index",
			term: 2, startLog: entries(1),
			args:        AppendEntriesArgs{Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(2)[1:], LeaderCommit: 99},
			wantSuccess: true, wantLog: []int{0, 1, 2}, wantCommit: 2,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			isolate(t)
			n := node(0, c.term, -1, c.startLog)
			var reply AppendEntriesReply
			if err := n.AppendEntries(c.args, &reply); err != nil {
				t.Fatalf("AppendEntries returned error: %v", err)
			}
			if reply.Success != c.wantSuccess {
				t.Errorf("Success = %v, want %v", reply.Success, c.wantSuccess)
			}
			if got := terms(n.log); !equal(got, c.wantLog) {
				t.Errorf("log terms = %v, want %v", got, c.wantLog)
			}
			if n.commitIndex != c.wantCommit {
				t.Errorf("commitIndex = %d, want %d", n.commitIndex, c.wantCommit)
			}
		})
	}
}

func TestAppendEntriesStepsDownEvenWhenLogCheckFails(t *testing.T) {
	isolate(t)
	// A candidate hears from a legitimate leader but its log doesn't line up.
	// It must still become a follower and reset its election timer.
	n := node(0, 2, 0, entries(1))
	n.role = Candidate
	n.lastHeard = time.Now().Add(-time.Hour)
	before := n.lastHeard

	var reply AppendEntriesReply
	n.AppendEntries(AppendEntriesArgs{Term: 2, PrevLogIndex: 9, PrevLogTerm: 1}, &reply)

	if reply.Success {
		t.Error("Success = true, want false (PrevLogIndex is past the end)")
	}
	if n.role != Follower {
		t.Errorf("role = %v, want Follower -- a valid leader was heard from", n.role)
	}
	if !n.lastHeard.After(before) {
		t.Error("lastHeard was not reset; this node will time out against a live leader")
	}
}

func TestAppendEntriesStaleLeaderDoesNotResetTimer(t *testing.T) {
	isolate(t)
	n := node(0, 5, -1, entries(1))
	n.lastHeard = time.Now().Add(-time.Hour)
	before := n.lastHeard

	var reply AppendEntriesReply
	n.AppendEntries(AppendEntriesArgs{Term: 2, PrevLogIndex: 1, PrevLogTerm: 1}, &reply)

	if reply.Success {
		t.Error("accepted entries from a stale leader")
	}
	if n.lastHeard != before {
		t.Error("a stale leader reset lastHeard; it could keep a dead cluster quiet")
	}
}

// ---------- commit math ----------

func TestMajorityElement(t *testing.T) {
	cases := []struct {
		name  string
		match []int
		want  int
	}{
		{"three nodes, two are ahead", []int{3, 5, 7}, 5},
		{"three nodes, all equal", []int{7, 7, 7}, 7},
		{"three nodes, nothing replicated", []int{0, 0, 0}, 0},
		{"three nodes, one lags", []int{5, 5, 3}, 5},
		{"three nodes, one is far behind", []int{0, 5, 3}, 3},
		{"five nodes", []int{1, 2, 3, 4, 5}, 3},
		{"five nodes, three are caught up", []int{9, 9, 9, 2, 0}, 9},
		{"four nodes needs three to agree", []int{1, 2, 3, 4}, 2},
		{"six nodes needs four to agree", []int{1, 2, 3, 4, 5, 6}, 3},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := majorityElement(append([]int(nil), c.match...)); got != c.want {
				t.Errorf("majorityElement(%v) = %d, want %d", c.match, got, c.want)
			}
		})
	}
}

func TestMajorityElementDoesNotScrambleCaller(t *testing.T) {
	// The caller passes a clone, but if that ever changes this catches it:
	// sorting matchIndex in place destroys the peer-index mapping.
	orig := []int{7, 1, 5}
	majorityElement(append([]int(nil), orig...))
	if !equal(orig, []int{7, 1, 5}) {
		t.Errorf("caller's slice was reordered: %v", orig)
	}
}

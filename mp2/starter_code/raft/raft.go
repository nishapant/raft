package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//

var wg sync.WaitGroup

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// General Consts
const (
	Min_Duration = 500
	Max_Duration = 900
)

type State int

const (
	FOLLOWER  State = 0
	LEADER    State = 1
	CANDIDATE State = 2
)

type Vote int

const (
	No      Vote = 0
	Yes     Vote = 1
	Waiting Vote = 2
)

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mutex sync.Mutex          // L-ock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// You may also need to add other state, as per your implementation.

	// Misc
	total_nodes int

	// Channels
	applyChannel      chan ApplyMsg
	vote_message_ch   chan bool
	append_message_ch chan bool
	reply_message_ch  chan RequestVoteReply

	// Current
	curr_state State
	curr_term  int

	// Timer
	timer   *time.Timer
	timeout time.Duration

	// Leader
	curr_leader  int
	commit_idx   int // index of highest log entry known to be committed [paper]
	last_applied int // index of highest log entry applied to state machine [paper]

	// Candidate
	votes     []Vote
	yes_votes int

	// Follower
	voted_for int

	// Messages
	log []LogEntry
	// note: these arrs are len(peers) with index rf.me as 0
	clientNextIndex  []int // The next index to check to see if it matches
	clientMatchIndex []int // The last index to which we KNOW the entries match between this server and the other one
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mutex.Lock()
	term := rf.curr_term
	isleader := rf.curr_state == LEADER
	rf.mutex.Unlock()

	return term, isleader
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId  int // Candidate ID
	RequestTerm  int // The term that the candidate is on
	LastLogIndex int // The last index of the candidate's log entries
	LastLogTerm  int // The last term of the candidate's log entries
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	PeerId      int
	CurrTerm    int
	VoteGranted bool
}

// RPC for appending entries to log
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry // Array with the entries to append

	LeaderCommitIdx int // The highest index that the leader can commit until

}

type AppendEntriesReply struct {
	CurrTerm   int
	CurrLeader int
	Success    bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".

	// Follow response logic from slides
	// If curr term is greater than the argument term
	rf.mutex.Lock()
	curr_term := rf.curr_term
	rf.mutex.Unlock()

	if args.RequestTerm < curr_term {
		reply.VoteGranted = false
		return
	}

	// If curr term is less than argument term (we are old)
	if args.RequestTerm > rf.curr_term {
		rf.mutex.Lock()
		rf.curr_state = FOLLOWER
		rf.curr_term = args.RequestTerm
		rf.voted_for = -1
		rf.yes_votes = 0
		rf.mutex.Unlock()
	}

	self_term, self_index := rf.get_last_log_entry_info()

	voted_condition := (rf.voted_for == -1 || rf.voted_for == args.CandidateId)
	log_deny_condition := (args.LastLogTerm > self_term) ||
		((args.LastLogTerm == self_term) && (args.LastLogIndex > self_index))

	log.Printf("log deny condition is %t on raft %d", log_deny_condition, rf.me)
	if voted_condition && !log_deny_condition {
		rf.mutex.Lock()
		reply.VoteGranted = true
		reply.CurrTerm = rf.curr_term
		rf.voted_for = args.CandidateId
		rf.mutex.Unlock()
	}

	// Add reply to request vote channel
	if voted_condition && !log_deny_condition {
		rf.vote_message_ch <- true
	}
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Reset timer
	rf.append_message_ch <- true

	// Mutex
	rf.mutex.Lock()

	// log.Printf("Heartbeat received from %d to %d", args.LeaderId, rf.me)
	// Heartbeat
	if args.Term > rf.curr_term { // if we are receiving message from new leader
		rf.curr_state = FOLLOWER
		rf.curr_leader = args.LeaderId
		rf.curr_term = args.Term
		rf.voted_for = -1
	}

	// the other node is not up to date
	// receiving message from an old leader
	if rf.curr_term > args.Term {
		reply.CurrTerm = rf.curr_term
		reply.CurrLeader = rf.curr_leader
		reply.Success = false
		return
	}

	// Log entries

	// For all AppendEntryReply instances, set the curr leader and curr term
	reply.CurrLeader = rf.curr_leader
	reply.CurrTerm = rf.curr_term

	_, self_last_index := rf.get_last_log_entry_info()

	// Case 1: Our log is entry
	if self_last_index == -1 {
		if args.PrevLogIndex == -1 {
			reply.Success = true
			rf.log = append(rf.log, args.Entries...)
			return
		} else {
			reply.Success = false
			return
		}

	}

	// Case 2: Not yet reached the last element in our log array
	if self_last_index < args.PrevLogIndex {
		reply.Success = false
		return
	}

	// Case 3: Reached elements in our log array
	if self_last_index >= args.PrevLogIndex {
		// Case 2.1: Found match! Append the entries, reply true
		if len(rf.log) == 0 {
			if self_last_index == args.PrevLogIndex {
				rf.log = append(rf.log, args.Entries...)
				reply.Success = true
				return
			}
			reply.Success = false
			return
		}

		if rf.log[self_last_index].Term == args.PrevLogTerm {
			rf.log = append(rf.log, args.Entries...)
			reply.Success = true
			return
		}
		// Case 2.2: No match :( Delete entry, reply false
		rf.log = rf.log[:len(rf.log)-1]
		reply.Success = false
	}

	// STEP 5 [from paper]: Commit Indexs
	if args.LeaderCommitIdx > rf.commit_idx {
		rf.commit_idx = min(args.LeaderCommitIdx, self_last_index)
	}
	rf.mutex.Unlock()

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// log.Printf("id %d: Received Reply from: %d with response: %t", rf.me, reply.PeerId, reply.VoteGranted)
	// rf.reply_message_ch <- *reply
	log.Printf("id: %d Sending votes...", rf.me)

	if reply.VoteGranted == true {
		rf.mutex.Lock()
		rf.yes_votes = rf.yes_votes + 1
		log.Printf("got vote from %d for id %d", reply.PeerId, rf.me)
		rf.mutex.Unlock()
	} else if reply.VoteGranted == false {
		rf.mutex.Lock()
		if rf.curr_term < reply.CurrTerm {
			rf.curr_state = FOLLOWER
			rf.curr_term = reply.CurrTerm
			rf.yes_votes = 0
			rf.voted_for = -1
			rf.ResetTimer() ///// CHECK TO MAKE SURE THIS IS OK IN A MUTEX
		}
		rf.mutex.Unlock()
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := rf.curr_term
	isLeader := (rf.curr_leader == rf.me)

	rf.mutex.Lock()
	if isLeader {
		entry := LogEntry{
			Term:    term,
			Command: command,
		}
		rf.curr_state = LEADER
		rf.log = append(rf.log, entry)
		index = len(rf.log)

	}
	rf.mutex.Unlock()
	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	log.Printf("Making raft, id: %d", me)
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Misc
	rf.total_nodes = len(peers)

	// Channels
	rf.applyChannel = applyCh
	rf.vote_message_ch = make(chan bool)
	rf.append_message_ch = make(chan bool)
	rf.reply_message_ch = make(chan RequestVoteReply)

	// Current
	rf.curr_state = FOLLOWER
	rf.curr_term = 1

	// Your initialization code here (2A, 2B).
	// Leader
	rf.curr_leader = -1

	// Candidate
	rf.votes = make([]Vote, rf.total_nodes-1)
	rf.yes_votes = 0

	// Follower
	rf.voted_for = -1

	// Messages
	rf.log = make([]LogEntry, 0)
	rf.clientNextIndex = make([]int, rf.total_nodes)
	rf.clientMatchIndex = make([]int, rf.total_nodes)

	// Reset Timer
	rf.ResetTimer()

	////////// Start threads
	// 1) Handle heartbeat
	go rf.HeartbeatHandler()
	// 2) Handle messages / election / everything else
	go rf.GeneralHandler()
	// 3) Handle apply message channel
	go rf.ApplyMsgHandler()

	return rf
}

// HANDLERS
// Appends stuff to the apply msg channel when something is committed
// Must be 1 indexed: https://campuswire.com/c/G3023A061/feed/565
func (rf *Raft) ApplyMsgHandler() {
	for {
		rf.mutex.Lock()
		if rf.commit_idx > rf.last_applied {
			rf.last_applied = rf.last_applied + 1
			idx := rf.last_applied
			new_msg := ApplyMsg{}
			new_msg.Command = rf.log[idx].Command
			new_msg.CommandIndex = idx + 1
			rf.applyChannel <- new_msg
		}

		rf.mutex.Unlock()

		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) HeartbeatHandler() {
	for {
		rf.mutex.Lock()
		curr_state := rf.curr_state
		rf.mutex.Unlock()

		if curr_state == LEADER {
			// log.Printf("Sending heartbeats as leader: %d", rf.me)
			for index, _ := range rf.peers {
				if index != rf.me {
					args := AppendEntriesArgs{
						Term:            rf.curr_term,
						LeaderId:        rf.me,
						PrevLogTerm:     -1,
						PrevLogIndex:    -1,
						Entries:         []LogEntry{},
						LeaderCommitIdx: -1,
					}
					reply := AppendEntriesReply{}

					// Send append entries
					go rf.sendAppendEntries(index, &args, &reply)
				}
			}

		}

		time.Sleep(time.Millisecond * 200)
	}
}

func (rf *Raft) GeneralHandler() {
	rf.ResetTimer()

	for {
		// Grab current state for this iteration
		rf.mutex.Lock()
		curr_state := rf.curr_state
		rf.mutex.Unlock()

		if curr_state == LEADER {
			// log.Printf("IS LEADER %d\n", rf.me)

			// Handle AppendEntries RPC's
			rf.mutex.Lock()
			rf.HandleLogConsensus()
			rf.mutex.Unlock()

		} else if curr_state == FOLLOWER {
			log.Printf("IS FOLLOWER %d\n", rf.me)

			select {
			case <-rf.append_message_ch:
				log.Printf("APPEND MESSAGE SENT %d\n", rf.me)
				rf.ResetTimer()
			case <-rf.vote_message_ch:
				log.Printf("VOTE MESSAGE, id: %d\n", rf.me)
				rf.ResetTimer()
			case <-rf.timer.C:
				log.Printf("TIMED OUT (as follower) %d\n", rf.me)
				rf.mutex.Lock()
				rf.curr_state = CANDIDATE
				rf.curr_leader = -1
				rf.mutex.Unlock()

				rf.ResetTimer()
			}

		} else if curr_state == CANDIDATE {
			// rf.mutex.Lock()

			select {
			// case <-rf.vote_message_ch:
			// 	log.Printf("VOTE MESSAGE RECEIVED %d\n", rf.me)
			// 	rf.ResetTimer()
			case <-rf.append_message_ch:
				rf.ResetTimer()
			case <-rf.timer.C:
				log.Printf("TIMED OUT (as candidate) %d\n", rf.me)
				rf.ResetTimer()

				// Call Election after timeout
				rf.mutex.Lock()
				rf.StartElection()
				rf.mutex.Unlock()
			default:
				// As default, check if we have the majority vote to become leader
				rf.mutex.Lock()
				if rf.HasMajorityVote() {
					log.Printf("there is a majority vote %d --------------", rf.me)
					rf.curr_leader = rf.me
					rf.curr_state = LEADER

					log.Printf("is leader now, curr leader %d", rf.curr_leader)
					_, self_last_index := rf.get_last_log_entry_info()

					for i := range rf.peers {
						rf.clientNextIndex[i] = self_last_index + 1
						rf.clientMatchIndex[i] = 0
					}
					rf.ResetTimer()
					log.Printf("Reset the timer...")
				}

				// rf.CheckMajorityVote()
				rf.mutex.Unlock()
				// log.Printf("Reaching in %d", rf.me)
			}
			// rf.mutex.Unlock()
		}
		// log.Printf("id %d state %d", rf.me, rf.curr_state)
	}

}

// HELPER METHODS

// LEADER

// Wrapped in mutex - General Handler
func (rf *Raft) HandleLogConsensus() {
	// log.Printf("leader is %d", rf.me)
	self_last_term, _ := rf.get_last_log_entry_info()

	if len(rf.log) == 0 {
		// log.Printf("leader id: %d, log is empty", rf.me)
		return
	}

	for i := 0; i < len(rf.log); i++ {
		// log.Printf("id: %d, index: %d, term: %d", rf.me, i, rf.log[i].Term)
	}

	for index, _ := range rf.peers {
		if index != rf.me {
			server_id := index
			last_idx_follower := rf.clientNextIndex[server_id]

			var new_entries []LogEntry
			if last_idx_follower == -1 { // send our entire log
				new_entries = rf.log
			} else {
				new_entries = rf.log[last_idx_follower:] // LogEntry{} // THIS NEEDS TO CHANGE TO SOMETHING IDK
			}
			args := AppendEntriesArgs{
				Term:            rf.curr_term,
				LeaderId:        rf.me,
				PrevLogTerm:     self_last_term,
				PrevLogIndex:    last_idx_follower,
				Entries:         new_entries,
				LeaderCommitIdx: rf.commit_idx,
			}
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(index, &args, &reply)

			// If there is another leader with a greater curr term
			if reply.CurrTerm > rf.curr_term {
				rf.curr_term = reply.CurrTerm
				rf.curr_leader = reply.CurrLeader
				rf.curr_state = FOLLOWER
				rf.yes_votes = 0
				rf.voted_for = -1
				return
			}
			if reply.Success == true {
				// This means that we got the right index! update stuff
				rf.clientNextIndex[server_id] = rf.clientNextIndex[server_id] + len(new_entries)
				rf.clientMatchIndex[server_id] = rf.clientNextIndex[server_id] - 1
			} else {
				// update next index to be one less than it was before???
				rf.clientNextIndex[server_id] = rf.clientNextIndex[server_id] - 1
			}
		}
	}
}

// CANDIDATE

// Wrapped in Mutex - GeneralHandler
func (rf *Raft) StartElection() {
	log.Printf("STARTED ELECTION, id: %d\n", rf.me)

	// Reset what we need to
	rf.yes_votes = 1
	rf.curr_term = rf.curr_term + 1
	rf.voted_for = rf.me

	// Create args and reply objects and send to all peers
	for index, _ := range rf.peers {
		// log.Printf("index which is the peer: %d", index)
		if index != rf.me {
			self_term, self_index := rf.get_last_log_entry_info()
			args := RequestVoteArgs{
				CandidateId:  rf.me,
				RequestTerm:  rf.curr_term,
				LastLogIndex: self_term,
				LastLogTerm:  self_index,
			}
			reply := RequestVoteReply{
				PeerId:      index,
				CurrTerm:    -1,
				VoteGranted: false,
			}

			// CHANGE THIS bc the reply thing isn't going to be updated in the separate thread bc i think diff stacks??
			go rf.sendRequestVote(index, &args, &reply)
		}
	}

	time.Sleep(time.Millisecond * 300)
	log.Printf("id %d: End of started election", rf.me)
	// go rf.CountVotes()

}

// func (rf *Raft) CountVotes() {
// 	log.Printf("In count votes, id: %d", rf.me)
// 	if rf.yes_votes >=
// }

// func (rf *Raft) CountVotes() {
// 	log.Printf("In count votes, id: %d", rf.me)
// 	for {
// 		select {
// 		case reply := <-rf.reply_message_ch:
// 			// log.Printf("id: %d, reply channel vote granted: %t", rf.me, reply.VoteGranted)
// 			if reply.VoteGranted == true {
// 				rf.mutex.Lock()
// 				rf.yes_votes = rf.yes_votes + 1
// 				rf.mutex.Unlock()
// 			}
// 		case <-rf.timer.C:
// 			// log.Printf("timer ran out lolololol")
// 			rf.ResetTimer()
// 			return
// 		}
// 	}
// }

// Wrapped in mutex - GeneralHandler
func (rf *Raft) CheckMajorityVote() {
	if rf.HasMajorityVote() {
		log.Printf("there is a majority vote %d --------------", rf.me)
		rf.curr_leader = rf.me
		rf.curr_state = LEADER

		log.Printf("is leader now, curr leader %d", rf.curr_leader)
		_, self_last_index := rf.get_last_log_entry_info()

		for i := range rf.peers {
			rf.clientNextIndex[i] = self_last_index + 1
			rf.clientMatchIndex[i] = 0
		}
		rf.ResetTimer()
	}
}

// Wrapped in mutex - CheckMajorityVote
func (rf *Raft) HasMajorityVote() bool {
	majority := int(math.Ceil(float64(rf.total_nodes) / 2.0))
	if rf.yes_votes >= majority {
		return true
	}
	return false
}

// TIMER

// Resets the timer
// 1) Sets the new duration of the timer to a randomized value
// 2) Creates a new timer with that duration
func (rf *Raft) ResetTimer() {
	duration := RandomNum(Min_Duration, Max_Duration)
	// rf.mutex.Lock()
	rf.timeout = time.Duration(duration) * time.Millisecond
	// rf.mutex.Unlock()
	rf.timer = time.NewTimer(rf.timeout)
}

// Creates random number in range [min, max] TODO: STRESS TEST
func RandomNum(min int, max int) int {
	rand.Seed(time.Now().UnixNano())
	rand_num := rand.Intn(max-min+1) + min
	return rand_num
}

// GENERAL

// Returns (term, index) for the last entry of any raft
func (rf *Raft) get_last_log_entry_info() (int, int) {
	if len(rf.log) != 0 {
		last_entry := rf.log[len(rf.log)-1]
		return last_entry.Term, len(rf.log) - 1
	}

	return rf.curr_term, 0
}

// min function (which is a little sad)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

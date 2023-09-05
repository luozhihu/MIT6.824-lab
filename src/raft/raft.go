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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
//--------------------------------------------
const Follower, Candidate, Leader int = 1, 2, 3
const tickInterval = 100 * time.Millisecond
const heartbeatTimeout = 150 * time.Millisecond

const baseElectionTimeout = 300
const None = -1

type PeerTracker struct {
	nextIndex  uint64
	matchIndex uint64

	lastAck time.Time
}

type RequestAppendEntriesArgs struct {
	LeaderTerm   int        // Leader的Term
	PrevLogIndex int        // 新日志条目的上一个日志的索引
	PrevLogTerm  int        // 新日志的上一个日志的任期
	Logs         []ApplyMsg // 需要被保存的日志条目,可能有多个
	LeaderCommit int        // Leader已提交的最高的日志项目的索引
	LeaderId     int
}

type RequestAppendEntriesReply struct {
	FollowerTerm  int  // Follower的Term,给Leader更新自己的Term
	Success       bool // 是否推送成功
	ConflictIndex int  // 冲突的条目的下标
	ConflictTerm  int  // 冲突的条目的任期
}
//-----------------------------------------------
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//------------------------------------------------------------
	state            int           // 节点状态，Candidate-Follower-Leader
	currentTerm      int           // 当前的任期
	votedFor         int           // 投票给谁
	heartbeatTimeout time.Duration // 心跳定时器
	electionTimeout  time.Duration //选举计时器
	lastElection     time.Time     // 上一次的选举时间，用于配合since方法计算当前的选举时间是否超时
	lastHeartbeat    time.Time     // 上一次的心跳时间，用于配合since方法计算当前的心跳时间是否超时
	peerTrackers     []PeerTracker // keeps track of each peer's next index, match index, etc.
	//-------------------------------------------------------------------------

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//-------------------------
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	//---------------------------
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//----------------------------------
	Term        int
	CandidateId int

	//----------------------------------
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	//---------------------------------
	Term        int
	VoteGranted bool
	//-------------------------------
}

// example RequestVote RPC handler.

/*
作为服务器，收到的选举出现的情况
1.选举人的任期小于我的任期，投反对票
2.选举人大于我的任期，投同意票，并更新自己的任期
3.选举人等于自己的任期，投反对票
*/


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//--------------------------------------
	rf.mu.Lock()
	defer rf.mu.Unlock()
	////fmt.Printf("[%d] begins grasping the lock...", rf.me)
	//fmt.Printf("%v[RequestVote] from %v at args term: %v and current term: %v\n", args.CandidateId, rf.me, args.Term, rf.currentTerm)
	//竞选leader的节点任期小于等于自己的任期，则反对票(为什么等于情况也反对票呢？因为candidate节点在发送requestVote rpc之前会将自己的term+1)
	if args.Term < rf.currentTerm {
		DPrintf("%v: candidate的任期是%d, 小于我，所以拒绝", rf.SayMeL(), args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	DPrintf("%v:candidate为%d,任期是%d", rf.SayMeL(), args.CandidateId, args.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		//rf.mu.Unlock()
		rf.votedFor = None
		rf.state = Follower
		DPrintf("%v:candidate %d的任期比自己大，所以修改rf.votedFor从%d到-1", rf.SayMeL(), args.CandidateId, rf.votedFor)
		//rf.ToFollower()
		//rf.mu.Lock()
	}
	reply.Term = rf.currentTerm
	//Lab2B的日志复制直接确定为true
	update := true
	//任期大于自己，
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		//竞选任期大于自身任期，则更新自身任期，并转为follower
		rf.votedFor = args.CandidateId
		rf.state = Follower
		//rf.mu.Unlock()
		rf.resetElectionTimer() //自己的票已经投出时就转为follower状态
		//electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
		//rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
		//rf.lastElection = time.Now()
		reply.VoteGranted = true // 默认设置响应体为投同意票状态
		DPrintf("%v: 同意把票投给%d, 它的任期是%d", rf.SayMeL(), args.CandidateId, args.Term)
		//rf.mu.Lock()
	} else {
		reply.VoteGranted = false
		DPrintf("%v:我已经投票给节点%d, 这次候选人的id为%d， 不符合要求，拒绝投票", rf.SayMeL(),
			rf.votedFor, args.CandidateId)
	}
	//---------------------------------------------------
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
//-------------------------------------------------------------
func (rf *Raft) SayMeL() string {

	//return fmt.Sprintf("[Server %v as %v at term %v]", rf.me, rf.state, rf.currentTerm)
	return "success"
}
func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}
/*
收到了leader发来的心跳
1.如果心跳的任期小于自己，则返回false，并把自己的任期告诉leader
2.如果leader大于自己的任期，那么更新自己的任期
3.如果等于，那么正常返回，并且重置超时时间
*/
func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	DPrintf("receiving heartbeat from leader %d and gonna get the lock...", args.LeaderId)
	rf.mu.Lock() // 加接收心跳方的锁
	reply.Success = true
	DPrintf("\n  %d receive heartbeat at leader %d's term %d, and my term is %d", rf.me, args.LeaderId, args.LeaderTerm, rf.currentTerm)
	// 旧任期的leader抛弃掉,
	if args.LeaderTerm < rf.currentTerm {
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	//DPrintf(111, "reset self electionTimer ")
	rf.mu.Lock()
	rf.resetElectionTimer()

	rf.state = Follower

	// 需要转变自己的身份为Follower
	// 承认来者是个合法的新leader，则任期一定大于自己，此时需要设置votedFor为-1以及
	if args.LeaderTerm > rf.currentTerm {
		rf.votedFor = None
		rf.currentTerm = args.LeaderTerm
		reply.FollowerTerm = rf.currentTerm // 将更新了的任期传给主结点
	}
	rf.mu.Unlock()

	// 重置自身的选举定时器，这样自己就不会重新发出选举需求（因为它在ticker函数中被阻塞住了）
	//log.Printf("[%v]'s electionTimeout is reset and its state converts to %v", rf.me, rf.state)
}
func (rf *Raft) AppendEntries(targetServerId int, heart bool, args *RequestAppendEntriesArgs) {

	reply := RequestAppendEntriesReply{}
	rf.mu.Lock()

	DPrintf("%v: is ready to send heartbeat to %d", rf.SayMeL(), targetServerId)
	rf.mu.Unlock()

	if heart {
		rf.sendRequestAppendEntries(targetServerId, args, &reply)

	}

}
func (rf *Raft) StartAppendEntries(heart bool) {
	// 所有节点共享同一份request参数
	args := RequestAppendEntriesArgs{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimer()
	args.LeaderTerm = rf.currentTerm
	args.LeaderId = rf.me
	// 并行向其他节点发送心跳，让他们知道此刻已经有一个leader产生
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.AppendEntries(i, heart, &args)
	}
}
//------------------------------------------------------
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		//-----------------------------------------------------
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		//rf.mu.Lock()
		switch state {
		case Follower:
			fallthrough
		case Candidate:
			//如果超时了那么就选举
			if rf.pastElectionTimeout() { //#A
				rf.StartElection()
			} 
		case Leader:
			// 只有Leader节点才能发送心跳和日志给从节点
			isHeartbeat := false
			// 检测是需要发送单纯的心跳还是发送日志
			// 心跳定时器过期则发送心跳，否则发送日志
			if rf.pastHeartbeatTimeout() {
				isHeartbeat = true
				rf.resetHeartbeatTimer()
			}
			rf.StartAppendEntries(isHeartbeat)

		}
		//------------------------------------------------

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//--------------------------------------------------------------
	rf.currentTerm = 0
	rf.votedFor = None
	rf.state = Follower                    //设置节点的初始状态为follower
	rf.heartbeatTimeout = heartbeatTimeout // 这个是固定的
	//---------------------------------------------------------------

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
//------------------------------------------------
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimer()
	rf.becomeCandidate()
	done := false
	votes := 1
	term := rf.currentTerm
	DPrintf("[%d] attempting an election at term %d...", rf.me, rf.currentTerm)
	args := RequestVoteArgs{rf.currentTerm, rf.me}

	for i, _ := range rf.peers {
		if rf.me == i {
			continue
		}
		// 开启协程去尝试拉选票
		go func(serverId int) {
			var reply RequestVoteReply
			//发起竞选
			ok := rf.sendRequestVote(serverId, &args, &reply)
			//log.Printf("[%d] finish sending request vote to %d", rf.me, serverId)
			//DPrintf(111, "%v: the reply term is %d and the voteGranted is %v", rf.SayMeL(), reply.Term, reply.VoteGranted)
			//如果被拒绝，或者出错了，直接丢弃
			if !ok || !reply.VoteGranted {
				//DPrintf(111, "%v: cannot give a Vote to %v args.term=%v\n", rf.SayMeL(), serverId, args.Term)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 丢弃无效票
			//if term != reply.Term {
			//	return
			//}
			//在竞选过程中，当前的任期由于某些原因（选举超时，有更高的选举）增加了
			if rf.currentTerm > reply.Term {
				return
			}
			// 统计票数
			votes++
			if done || votes <= len(rf.peers)/2 {
				// 在成为leader之前如果投票数不足需要继续收集选票
				// 同时在成为leader的那一刻，就不需要管剩余节点的响应了，因为已经具备成为leader的条件
				return
			}
			done = true
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			DPrintf("\n%v: [%d] got enough votes, and now is the leader(currentTerm=%d, state=%v)!\n", rf.SayMeL(), rf.me, rf.currentTerm, rf.state)
			rf.state = Leader // 将自身设置为leader
			DPrintf("ready to send heartbeat to other nodes")
			go rf.StartAppendEntries(true) // 立即发送心跳

		}(i)
	}
}
func (rf *Raft) pastElectionTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f := time.Since(rf.lastElection) > rf.electionTimeout
	return f
}

func (rf *Raft) resetElectionTimer() {
	//rf.mu.Lock()
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
	DPrintf("%v: 选举的超时时间设置为%d", rf.SayMeL(), rf.electionTimeout)
	//rf.mu.Unlock()

}
func (rf *Raft) becomeCandidate() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	DPrintf("%v: 选举时间超时，将自身变为Candidate并且发起投票...", rf.SayMeL())

}
func (rf *Raft) ToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	rf.votedFor = None
	DPrintf("%v: I am converting to a follower.", rf.SayMeL())
}
//------------------------------------------------

//-----------------------------------------------
//log_replication
func (rf *Raft) pastHeartbeatTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeat = time.Now()
}
//------------------------------------------------
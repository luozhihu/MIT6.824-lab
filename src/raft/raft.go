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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	
	"6.5840/labgob"
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

type RequestAppendEntriesArgs struct {
	LeaderTerm   int // Leader的Term
	LeaderId     int // leaderid
	PrevLogIndex int // 新日志条目的上一个日志的索引
	PrevLogTerm  int // 新日志的上一个日志的任期
	//Logs         []ApplyMsg // 需要被保存的日志条目,可能有多个
	Entries      []Entry
	LeaderCommit int // Leader已提交的最高的日志项目的索引
	
}

type RequestAppendEntriesReply struct {
	FollowerTerm int  // Follower的Term,给Leader更新自己的Term
	Success      bool // 是否推送成功
	PrevLogIndex int
	PrevLogTerm  int
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
	//--------------------------------------------------------------------------
	log              *Log          // 日志记录

	//Volatile state
	commitIndex int // commitIndex是本机提交的
	lastApplied int // lastApplied是该日志在所有的机器上都跑了一遍后才会更新？

	applyHelper *ApplyHelper
	applyCond   *sync.Cond
	//-------------------------------------------------------------------------------

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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm) // 持久化任期
	e.Encode(rf.votedFor)    // 持久化votedFor
	e.Encode(rf.log)         // 持久化日志
	data := w.Bytes()
	go rf.persister.SaveRaftState(data)
}


// restore previously persisted state.
func (rf *Raft) readPersist() {
	stateData := rf.persister.ReadRaftState()
	if stateData == nil || len(stateData) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2C).
	if stateData != nil && len(stateData) > 0 { // bootstrap without any state?
		r := bytes.NewBuffer(stateData)
		d := labgob.NewDecoder(r)
		rf.votedFor = 0 // in case labgob waring
		if d.Decode(&rf.currentTerm) != nil ||
			d.Decode(&rf.votedFor) != nil ||
			d.Decode(&rf.log) != nil {
			//   error...
			DPrintf("%v: readPersist decode error\n", rf.SayMeL())
			panic("")
		}
		Lab2CPrintf("server id: %v, rf.votedFor: %v, rf.currentTerm: %v, rf.log: %v.\n",rf.me, rf.votedFor, rf.currentTerm, rf.log)
	}
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
	//------------------------------
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry
	//-----------------------------
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

//返回值是（日志号，日志任期，是否是leader）
//如果是follwer那么直接返回（-1，当前任期，false）
//如果是leader那么将log保存然后复制到其他服务器，并且立即返回
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	//--------------------------------------------
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state != Leader {
		//如果不是leader直接返回
		isLeader = false
		return index, term, isLeader
	}
	index = rf.log.LastLogIndex + 1
	// 开始发送AppendEntries rpc

	DPrintf("%v: a command index=%v cmd=%T %v come", rf.SayMeL(), index, command, command)
	//将log保存到raft.log中
	rf.log.appendL(Entry{term, command})
	rf.persist()
	//rf.resetTrackedIndex()
	DPrintf("%v: check the newly added log index：%d", rf.SayMeL(), rf.log.LastLogIndex)
	//开启一个线程将日志复制给其他节点
	go rf.StartAppendEntries(false)
	//-----------------------------------------------------------------
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
func (rf *Raft) sendRequestAppendEntries(isHeartbeat bool, server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	var ok bool
	if isHeartbeat {
		ok = rf.peers[server].Call("Raft.HandleHeartbeatRPC", args, reply)
	} else {
		ok = rf.peers[server].Call("Raft.HandleAppendEntriesRPC", args, reply)
	}
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
//向其他节点发送心跳或者日志
func (rf *Raft) AppendEntries(targetServerId int, heart bool) {
	//如果是心跳
	if heart {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock() //必须解锁，否则会造成死锁
			return
		}
		reply := RequestAppendEntriesReply{}
		args := RequestAppendEntriesArgs{}
		args.LeaderTerm = rf.currentTerm
		args.LeaderId = rf.me
		DPrintf("%v: %d is a leader, ready sending heartbeart to follower %d....", rf.SayMeL(), rf.me, targetServerId)
		rf.mu.Unlock()
		// 发送心跳包
		//心跳包只包含两个信息：leader任期和leader的id
		ok := rf.sendRequestAppendEntries(true, targetServerId, &args, &reply)
		//网络出错了，直接返回
		if !ok {
			return
		}
		if reply.Success {
			// 返回成功则说明接收了心跳
			return
		}
		rf.mu.Lock()
		//别的线程已经接收到了一个更新任期的节点，那么直接返回
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		//回复的日志比我小则无事发生
		if reply.FollowerTerm < rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		// 拒绝接收心跳，则可能是因为任期导致的
		if reply.FollowerTerm > rf.currentTerm {
			//收到一个比我任期还大的回复，那么自己就变成follower
			rf.votedFor = None
			rf.state = Follower
			rf.currentTerm = reply.FollowerTerm
			
		}
		rf.mu.Unlock()
		return
	} else {
		//如果不是心跳
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		args := RequestAppendEntriesArgs{}
		args.PrevLogIndex = min(rf.log.LastLogIndex, rf.peerTrackers[targetServerId].nextIndex-1)
		if args.PrevLogIndex+1 < rf.log.FirstLogIndex {
			//FirstLogIndex的初始值是1且不会变化，所以几乎不可能发生
			DPrintf("此时 %d 节点的nextIndex为%d,LastLogIndex为 %d, 最后一项日志为：\n", rf.me, rf.peerTrackers[rf.me].nextIndex,
				rf.log.LastLogIndex)
			return
		}
		args.LeaderTerm = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PrevLogTerm = rf.getEntryTerm(args.PrevLogIndex)
		args.Entries = rf.log.getAppendEntries(args.PrevLogIndex + 1)
		DPrintf("%v: the len of log entries: %d is ready to send to node %d!!! and the entries are %v\n",
			rf.SayMeL(), len(args.Entries), targetServerId, args.Entries)
		rf.mu.Unlock()

		//fmt.Printf("\n %d is a leader, ready sending log entries to follower %d with args leaderTerm:%d, PrevLogIndex: %d, PrevLogTerm:%d, lastEntry:%v....", rf.me, targetServerId, args.LeaderTerm, args.PrevLogIndex, args.PrevLogTerm, args.Entries[args.PrevLogIndex])
		/*非心跳包包含六个信息：
		term：自己的任期号，
		leaderId：自己的id，
		prevLogIndex：前一个日志的日志号，
		prevLogTerm：前一个日志的任期号，
		entris：当前日志，
		leaderCommit：已提交的日志号*/
		reply := RequestAppendEntriesReply{}

		ok := rf.sendRequestAppendEntries(false, targetServerId, &args, &reply)
		if !ok {
			//网络错误直接返回
			//DPrintf(111, "%v: cannot request AppendEntries to %v args.term=%v\n", rf.SayMeL(), targetServerId, args.LeaderTerm)
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader {
			//rf.mu.Unlock()
			return
		}
		// 丢弃旧的rpc响应。旧的rpc响应意味着旧的日志
		if reply.FollowerTerm < rf.currentTerm {
			return
		}

		DPrintf("%v: get reply from %v reply.Term=%v reply.Success=%v reply.PrevLogTerm=%v reply.PrevLogIndex=%v myinfo:rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v\n",
			rf.SayMeL(), targetServerId, reply.FollowerTerm, reply.Success, reply.PrevLogTerm, reply.PrevLogIndex, rf.log.FirstLogIndex, rf.log.LastLogIndex)
		if reply.FollowerTerm > rf.currentTerm {
			//收到一个比我任期还大的回复，那么自己就变成follower
			rf.state = Follower
			rf.currentTerm = reply.FollowerTerm
			rf.votedFor = None
			rf.persist()
			return
		}
		DPrintf("%v: get append reply reply.PrevLogIndex=%v reply.PrevLogTerm=%v reply.Success=%v heart=%v\n", rf.SayMeL(), reply.PrevLogIndex, reply.PrevLogTerm, reply.Success, heart)

		if reply.Success {
			//如果成功了就修改这个节点的下一个日志号
			rf.peerTrackers[targetServerId].nextIndex = args.PrevLogIndex + len(args.Entries) + 1
			rf.peerTrackers[targetServerId].matchIndex = args.PrevLogIndex + len(args.Entries)
			DPrintf("success! now trying to commit the log...\n")
			//然后尝试进行提交日志，（大于一半才能成功）
			rf.tryCommitL(rf.peerTrackers[targetServerId].matchIndex)
			return
		}

		//reply.Success is false
		if rf.log.empty() { //判掉为空的情况 方便后面讨论
			//go rf.InstallSnapshot(serverId)
			return
		}
		if reply.PrevLogIndex+1 < rf.log.FirstLogIndex {
			return
		}
		//rf.mu.Unlock()
		//回复的日志号大于leader的日志号
		if reply.PrevLogIndex > rf.log.LastLogIndex {
			rf.peerTrackers[targetServerId].nextIndex = rf.log.LastLogIndex + 1
		} else if rf.getEntryTerm(reply.PrevLogIndex) == reply.PrevLogTerm {
			//如果follower的节点的任期等于同样日志号的leader的日志的任期
			// 因为响应方面接收方做了优化，作为响应方的从节点可以直接跳到索引不匹配但是等于任期PrevLogTerm的第一个提交的日志记录
			rf.peerTrackers[targetServerId].nextIndex = reply.PrevLogIndex + 1
		} else {
			// 此时rf.getEntryTerm(reply.PrevLogIndex) != reply.PrevLogTerm，也就是说此时索引相同位置上的日志提交时所处term都不同，
			// 则此日志也必然是不同的，所以可以安排跳到前一个当前任期的第一个节点
			//如果follower的节点的任期等于同样日志号的leader的日志的任期
			PrevIndex := reply.PrevLogIndex
			for PrevIndex >= rf.log.FirstLogIndex && rf.getEntryTerm(PrevIndex) == rf.getEntryTerm(reply.PrevLogIndex) {
				PrevIndex--
			}
			rf.peerTrackers[targetServerId].nextIndex = PrevIndex + 1
		}

	}
}
// 并行向其他节点发送心跳或者日志，让他们知道此刻已经有一个leader产生
func (rf *Raft) StartAppendEntries(heart bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	rf.resetElectionTimer()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.AppendEntries(i, heart)

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


// 通知tester接收这个日志消息，然后供测试使用
func (rf *Raft) sendMsgToTester() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		DPrintf("%v: it is being blocked...", rf.SayMeL())
		//等待日志的到来
		rf.applyCond.Wait()

		for rf.lastApplied+1 <= rf.commitIndex {
			i := rf.lastApplied + 1
			rf.lastApplied++
			if i < rf.log.FirstLogIndex {
				DPrintf("%v: apply index=%v but rf.log.FirstLogIndex=%v rf.lastApplied=%v\n",
					rf.SayMeL(), i, rf.log.FirstLogIndex, rf.lastApplied)
				panic("error happening")
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.getOneEntry(i).Command,
				CommandIndex: i,
			}
			DPrintf("%s: next apply index=%v lastApplied=%v len entries=%v "+
				"LastLogIndex=%v cmd=%v\n", rf.SayMeL(), i, rf.lastApplied, len(rf.log.Entries),
				rf.log.LastLogIndex, rf.log.getOneEntry(i).Command)
			rf.applyHelper.tryApply(&msg)
		}
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
	rf.log = NewLog()
	//---------------------------------------------------------------

	// initialize from state persisted before a crash
	rf.readPersist()	
	Lab2CPrintf("make: server id: %v, rf.votedFor: %v, rf.currentTerm: %v, rf.log: %v.\n",rf.me, rf.votedFor, rf.currentTerm, rf.log)
	//-----------------------------------------------------------
	//rf.log = NewLog()
	rf.applyHelper = NewApplyHelper(applyCh, rf.lastApplied)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.peerTrackers = make([]PeerTracker, len(rf.peers)) //对等节点追踪器
	rf.applyCond = sync.NewCond(&rf.mu)
	//----------------------------------------------------------

	// start ticker goroutine to start elections
	go rf.ticker()
	//--------------------------
	// 通知tester接收这个日志消息，然后供测试使用
	go rf.sendMsgToTester() // 供config协程追踪日志以测试
	//-------------------------

	return rf
}


//------------------------------------------------

func (rf *Raft) getLastEntryTerm() int {
	if rf.log.LastLogIndex >= rf.log.FirstLogIndex {
		return rf.log.getOneEntry(rf.log.LastLogIndex).Term
	}
	return -1
}
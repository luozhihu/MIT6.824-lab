package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"
// import "bytes"
// import "6.5840/labgob"
type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(state)
}
// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}


// func (rf *Raft) testreadPersist(){
// 	stateData := rf.persister.ReadRaftState()
// 	if stateData == nil || len(stateData) < 1 { // bootstrap without any state?
// 		return
// 	}
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// Your code here (2C).
// 	if stateData != nil && len(stateData) > 0 { // bootstrap without any state?
// 		r := bytes.NewBuffer(stateData)
// 		d := labgob.NewDecoder(r)
// 		votedFor := 0 // in case labgob waring
// 		currentTerm := 0
// 		log := Log{}
// 		if d.Decode(currentTerm) != nil ||
// 			d.Decode(votedFor) != nil ||
// 			d.Decode(log) != nil {
// 			//   error...
// 			DPrintf("%v: readPersist decode error\n", rf.SayMeL())
// 			panic("")
// 		}
// 		Lab2CPrintf("server id: %v, rf.votedFor: %v, rf.currentTerm: %v, rf.log: %v.\n",rf.me, votedFor, currentTerm, log)
// 	}
// }
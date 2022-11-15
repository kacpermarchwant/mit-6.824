package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	ClerkId   int64
	CommandId int
	Op        string
}

func (op Op) String() string {
	switch op.Op {
	case "Get":
		return fmt.Sprintf("{G %s}", op.Key)
	case "Put":
		return fmt.Sprintf("{P %s:%s}", op.Key, op.Value)
	case "Append":
		return fmt.Sprintf("{A %s:+%s}", op.Key, op.Value)
	default:
		return ""
	}
}

type applyResponse struct {
	Value     string
	CommandId int
	Err       Err
}

type Command struct {
	op         Op
	responseCh chan applyResponse
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister             *raft.Persister
	lastAppliedCommandIdx int

	pendingCommands map[int]Command
	clerkResponses  map[int64]applyResponse
	lastResponses   map[string]string

	snapshotTrigger chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	op := Op{
		Op:        "Get",
		Key:       args.Key,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	}

	kv.mu.Lock()

	index, term, isLeader := kv.rf.Start(op)

	if term == 0 {
		kv.mu.Unlock()
		reply.Err = ErrInitElection
		return
	}

	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	responseCh := make(chan applyResponse)
	kv.pendingCommands[index] = Command{op: op, responseCh: responseCh}
	kv.mu.Unlock()

	for !kv.killed() {
		wrongLeader := false

		select {
		case response, ok := <-responseCh:
			if !ok {
				reply.Err = ErrShutdown
				return
			}

			*reply = GetReply{Err: response.Err, Value: response.Value}
			return
		case <-time.After(100 * time.Millisecond):
			currentTerm, _ := kv.rf.GetState()

			if term != currentTerm {
				reply.Err = ErrWrongLeader
				wrongLeader = true
			}
		}

		if wrongLeader {
			break
		}
	}

	go func() { <-responseCh }() // may cause a deadlock otherwise

	if kv.killed() {
		reply.Err = ErrShutdown
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	op := Op{
		Op:        args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
	}

	kv.mu.Lock()

	index, term, isLeader := kv.rf.Start(op)

	if term == 0 {
		kv.mu.Unlock()
		reply.Err = ErrInitElection
		return
	}

	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	// fmt.Println(op, args.CommandId)

	responseCh := make(chan applyResponse)
	kv.pendingCommands[index] = Command{op: op, responseCh: responseCh}
	kv.mu.Unlock()

	for !kv.killed() {
		wrongLeader := false

		select {
		case response, ok := <-responseCh:
			if !ok {
				reply.Err = ErrShutdown
				return
			}

			reply.Err = response.Err
			return
		case <-time.After(100 * time.Millisecond):
			currentTerm, _ := kv.rf.GetState()

			if term != currentTerm {
				reply.Err = ErrWrongLeader
				wrongLeader = true
			}
		}

		if wrongLeader {
			break
		}
	}

	go func() { <-responseCh }() // may cause a deadlock otherwise

	if kv.killed() {
		reply.Err = ErrShutdown
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.lastAppliedCommandIdx = kv.rf.LastIncludedIndex
	kv.snapshotTrigger = make(chan bool, 1)
	kv.clerkResponses = make(map[int64]applyResponse)
	kv.pendingCommands = make(map[int]Command)
	kv.lastResponses = make(map[string]string)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.responseHandler(kv.lastAppliedCommandIdx)
	go kv.snapshoter()

	return kv
}

func (kv *KVServer) responseHandler(lastSnapshoterTriggeredCommandIndex int) {
	var response string

	for message := range kv.applyCh {
		if message.SnapshotValid {
			kv.mu.Lock()

			kv.lastAppliedCommandIdx = message.SnapshotIndex
			kv.readSnapshot(message.Snapshot)

			for _, cmd := range kv.pendingCommands {
				cmd.responseCh <- applyResponse{Err: ErrWrongLeader}
			}

			kv.pendingCommands = make(map[int]Command)

			kv.mu.Unlock()

			continue
		}

		if !message.CommandValid {
			continue
		}

		if message.CommandIndex-lastSnapshoterTriggeredCommandIndex > 50 {
			select {
			case kv.snapshotTrigger <- true:
				lastSnapshoterTriggeredCommandIndex = message.CommandIndex
			default:
			}
		}

		op := message.Command.(Op)
		kv.mu.Lock()

		kv.lastAppliedCommandIdx = message.CommandIndex
		lastClerkResponse := kv.clerkResponses[op.ClerkId]

		if lastClerkResponse.CommandId >= op.CommandId {
			// return cached response to keep linearizability
			response = lastClerkResponse.Value
		} else {
			switch op.Op {
			case "Get":
				response = kv.lastResponses[op.Key]
			case "Put":
				kv.lastResponses[op.Key] = op.Value
				response = ""
			case "Append":
				kv.lastResponses[op.Key] = kv.lastResponses[op.Key] + op.Value
				response = ""
			}

			kv.clerkResponses[op.ClerkId] = applyResponse{Value: response, CommandId: op.CommandId}
		}

		cmd, ok := kv.pendingCommands[message.CommandIndex]

		if ok {
			delete(kv.pendingCommands, message.CommandIndex)
		}

		kv.mu.Unlock()

		if ok {
			if cmd.op != op {
				cmd.responseCh <- applyResponse{Err: ErrWrongLeader}
			} else {
				cmd.responseCh <- applyResponse{Err: OK, Value: response}
			}
		}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, cmd := range kv.pendingCommands {
		close(cmd.responseCh)
	}
}

func (kv *KVServer) snapshoter() {
	if kv.maxraftstate < 0 {
		return
	}

	for !kv.killed() {
		stateSize := kv.persister.RaftStateSize()
		fillPercentage := float64(stateSize) / float64(kv.maxraftstate)
		thresholdReached := fillPercentage > 0.9

		// fmt.Println(fillPercentage)

		if thresholdReached {
			kv.mu.Lock()

			if data := kv.saveSnapshot(); data == nil {
				println("Snapshot failed")
			} else {
				kv.rf.Snapshot(kv.lastAppliedCommandIdx, data)
			}

			kv.mu.Unlock()

			fillPercentage = 0.0
		}

		select {
		case <-time.After(time.Duration((1-fillPercentage)*100) * time.Millisecond):
		case <-kv.snapshotTrigger:
		}
	}
}

func (kv *KVServer) saveSnapshot() []byte {
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)

	err := e.Encode(kv.lastResponses)

	if err != nil {
		fmt.Println("saveSnapshot: ", err)
		return nil
	}

	err = e.Encode(kv.clerkResponses)

	if err != nil {
		fmt.Println("saveSnapshot: ", err)
		return nil
	}

	return b.Bytes()
}

func (kv *KVServer) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)

	var lastResponses map[string]string
	var clerkResponses map[int64]applyResponse

	err := d.Decode(&lastResponses)

	if err != nil {
		fmt.Println("readSnapshot: ", err)
		return
	}

	err = d.Decode(&clerkResponses)

	if err != nil {
		fmt.Println("readSnapshot: ", err)
		return
	}

	kv.lastResponses = lastResponses
	kv.clerkResponses = clerkResponses
}

package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id        int64
	commandId int
	leader    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()

	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.commandId = 1
	ck.leader = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	args := &GetArgs{
		ClerkId:   ck.id,
		CommandId: ck.commandId,
		Key:       key,
	}

	ck.commandId += 1

	leader := ck.leader

	for {
		reply := &GetReply{}

		ok := ck.servers[leader].Call("KVServer.Get", args, reply)

		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			leader = (leader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = leader

		if reply.Err == ErrNoKey {
			return ""
		}

		if reply.Err == OK {
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.id,
		CommandId: ck.commandId,
	}

	ck.commandId += 1

	leader := ck.leader

	for {
		reply := &PutAppendReply{}

		ok := ck.servers[leader].Call("KVServer.PutAppend", args, reply)

		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			leader = (leader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = leader

		if reply.Err == OK {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"

const (
	sleepUnit int = 100
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id		int64
	leader	int
	msgId	int
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
	ck.msgId = 1
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs {
		Key:	key,
		MsgId:	ck.msgId,
		ClientId:	ck.id,
	}
	ck.msgId++

	for {
		reply := GetReply {}
		leader := ck.leader
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else if reply.Err == OK {
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		}
		time.Sleep(time.Duration(sleepUnit) * time.Millisecond)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs {
		Key:	key,
		Value:	value,
		Op:		op,
		MsgId:	ck.msgId,
		ClientId:	ck.id,
	}
	ck.msgId++

	for {
		reply := GetReply {}
		leader := ck.leader
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else if reply.Err == OK {
			return
		}
		time.Sleep(time.Duration(sleepUnit) * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

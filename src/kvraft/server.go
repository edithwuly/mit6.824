package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	submitTimeOut int = 300
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId	int64
	MsgId		int
	Key			string
	Value		string
	Method		string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data	map[string]string
	replys	map[int64]map[int]ApplyReply
	waiting	map[int64]map[int]chan ApplyReply
}


func (kv *KVServer) Submit(op Op) ApplyReply {
	kv.mu.Lock()
	if lastReply, ok := kv.replys[op.ClientId][op.MsgId]; ok {
		kv.mu.Unlock()
		return lastReply
	}

	_, _, isleader := kv.rf.Start(op)
	if !isleader {
		kv.mu.Unlock()
		return ApplyReply {
			Err:	ErrWrongLeader,
		}
	}

	ch := make(chan ApplyReply)
	if _, ok := kv.waiting[op.ClientId]; !ok {
		kv.waiting[op.ClientId] = make(map[int]chan ApplyReply)
	}
	kv.waiting[op.ClientId][op.MsgId] = ch
	kv.mu.Unlock()
	for {
		select {
		case reply := <- ch:
			kv.mu.Lock()
			delete(kv.waiting[op.ClientId], op.MsgId)
			kv.mu.Unlock()
			return reply
		case <-time.After(time.Duration(submitTimeOut) * time.Millisecond):
			kv.mu.Lock()
			delete(kv.waiting[op.ClientId], op.MsgId)
			kv.mu.Unlock()
			return ApplyReply {
				Err:	ErrTimeOut,
			}
		}
	}

	return ApplyReply{}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op {
		ClientId:	args.ClientId,
		MsgId:		args.MsgId,
		Key:		args.Key,
		Method:		"Get",
	}

	applyReply := kv.Submit(op)
	reply.Err = applyReply.Err
	reply.Value = applyReply.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op {
		ClientId:	args.ClientId,
		MsgId:		args.MsgId,
		Key:		args.Key,
		Value:		args.Value,
		Method:		args.Op,
	}

	applyReply := kv.Submit(op)
	reply.Err = applyReply.Err
}


func (kv *KVServer) WaitForApplyMsg() {
	for {
		select {
		case applyMsg := <- kv.applyCh:
			if !applyMsg.CommandValid {
				continue
			}

			kv.mu.Lock()
			op := applyMsg.Command.(Op)

			reply, ok := kv.replys[op.ClientId][op.MsgId]
			if !ok {
				if op.Method == "Put" {
					kv.data[op.Key] = op.Value
				} else if op.Method == "Append" {
					kv.data[op.Key] += op.Value
				}
	
				reply = ApplyReply {}
				if value, ok := kv.data[op.Key]; ok {
					reply.Err = OK
					reply.Value = value
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
				}
				if _, ok := kv.replys[op.ClientId]; !ok {
					kv.replys[op.ClientId] = make(map[int]ApplyReply)
				}
				kv.replys[op.ClientId][op.MsgId] = reply
			}
			kv.mu.Unlock()
			
			if ch, ok := kv.waiting[op.ClientId][op.MsgId]; ok {
				ch <- reply
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
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

	kv.data = make(map[string]string)
	kv.waiting = make(map[int64]map[int]chan ApplyReply)
	kv.replys = make(map[int64]map[int]ApplyReply)

	go kv.WaitForApplyMsg()

	return kv
}

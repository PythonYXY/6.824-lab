package raftkv

import (
	"../labrpc"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	id int64
	seqNum int
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
	ck.seqNum = 0
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
	//DPrintf("client %v: get %s", ck.id, key)
	// You will have to modify this function.
	index := ck.lastLeader
	for {
		args := GetArgs{
			Key:    key,
		}
		reply := GetReply{}
		ok := ck.servers[index].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return reply.Value
		}
		index = (index + 1) % len(ck.servers)
	}
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
	//DPrintf("client %v: %s key: %s, value: %s", ck.id, op, key, value)
	// You will have to modify this function.
	index := ck.lastLeader
	args := PutAppendArgs{
		Key:key,
		Value:value,
		Op:op,
		Cid: ck.id,
		SeqNum: ck.seqNum,
	}
	ck.seqNum++
	for {
		reply := PutAppendReply{}
		ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return
		}
		index = (index + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}



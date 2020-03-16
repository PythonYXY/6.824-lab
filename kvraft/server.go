package raftkv

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"strconv"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key string
	Value string
	Cid int64
	SeqNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	persister *raft.Persister

	maxraftstate int // snapshot if log grows this big
	timeout time.Duration

	// Your definitions here.
	db map[string]string
	chMap map[int]chan Op
	cid2Seq map[int64]int
	killCh chan bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	originalOp := Op{"Get", args.Key, strconv.FormatInt(nrand(), 10), 0, 0}

	reply.WrongLeader = true

	index, _, isLeader := kv.rf.Start(originalOp)
	if !isLeader {
		return
	}

	ch := kv.putIfAbsent(index)
	op := beNotified(ch)

	if equalOP(op, originalOp) {
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.db[op.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	originalOp := Op{args.Op, args.Key, args.Value, args.Cid, args.SeqNum}

	reply.WrongLeader = true

	index, _, isLeader := kv.rf.Start(originalOp)
	if !isLeader {
		return
	}

	ch := kv.putIfAbsent(index)
	op := beNotified(ch)

	if equalOP(op, originalOp) {
		reply.WrongLeader = false
		return
	}
}

func (kv *KVServer) putIfAbsent(idx int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chMap[idx]; !ok {
		kv.chMap[idx] = make(chan Op, 1)
	}
	return kv.chMap[idx]
}

func equalOP(a Op, b Op) bool {
	return a.Key == b.Key && a.Value == b.Value && a.OpType == b.OpType && a.Cid == b.Cid && a.SeqNum == b.SeqNum
}

func beNotified(ch chan Op) Op  {
	select {
	case op := <-ch:
		return op
	case <- time.After(time.Duration(600) * time.Millisecond):
		return Op{}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killCh <- true
}

func (kv *KVServer) readSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cid2Seq map[int64]int

	if d.Decode(&db) != nil || d.Decode(&cid2Seq) != nil {
		log.Fatalf("read snapshot err: %v", kv.me)
	} else {
		kv.db, kv.cid2Seq = db, cid2Seq
	}
}

func (kv *KVServer) needSnapShot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	threshold := 10
	return kv.maxraftstate > 0 &&
		kv.maxraftstate - kv.persister.RaftStateSize() < kv.maxraftstate / threshold
}

func (kv *KVServer) doSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.cid2Seq)
	kv.mu.Unlock()
	kv.rf.DoSnapShot(index, w.Bytes())
}

func send(ch chan Op, op Op) {
	select {
	case <-ch:
	default:
	}
	ch <- op
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
	kv.cid2Seq = make(map[int64]int)
	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op)
	kv.persister = persister
	kv.readSnapShot(kv.persister.ReadSnapshot())
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan bool, 1)

	go func() {
		for {
			select {
			case <-kv.killCh:
				return
			case applyMsg := <-kv.applyCh:
				if !applyMsg.CommandValid {
					kv.readSnapShot(applyMsg.SnapShot)
					continue
				}

				op := applyMsg.Command.(Op)
				kv.mu.Lock()
				maxSeq, found := kv.cid2Seq[op.Cid]
				if !found || op.SeqNum > maxSeq {
					switch op.OpType {
					case "Put":
						kv.db[op.Key] = op.Value
					case "Append":
						kv.db[op.Key] += op.Value
					}
					kv.cid2Seq[op.Cid] = op.SeqNum
				}
				kv.mu.Unlock()

				index := applyMsg.CommandIndex

				ch := kv.putIfAbsent(index)

				if kv.needSnapShot() {
					go kv.doSnapShot(index) // 在doSnapShot中会使用raft的锁，为了避免在这里阻塞，需要异步调用函数
				}
				send(ch, op)
			}
		}
	}()
	return kv
}

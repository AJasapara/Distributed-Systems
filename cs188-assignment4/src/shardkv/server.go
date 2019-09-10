package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	ID   int64
	Seq   int
	Op	  string
	Key   string
	Value string
	Extra interface{}
}

type ConfigState struct {
	KVStore  map[string]string
	ClientRequestMap  map[int64]int
	ClientRepliesMap  map[int64]GetReply
}

func (cs *ConfigState) Init() {
	cs.KVStore = map[string]string{}
	cs.ClientRequestMap = map[int64]int{}
	cs.ClientRepliesMap = map[int64]GetReply{}
}

func (cs *ConfigState) Update(other *ConfigState) {
	for key, value := range other.KVStore {
		cs.KVStore[key] = value
	}

	for cli, seq := range other.ClientRequestMap {
		cseq := cs.ClientRequestMap[cli]
		if cseq < seq {
			cs.ClientRequestMap[cli] = seq
			cs.ClientRepliesMap[cli] = other.ClientRepliesMap[cli]
		}
	}
}

type TransferStateArgs struct {
	ConfigNum  int
	Shard      int
}

type TransferStateReply struct {
	Err     Err
	ConfigState  ConfigState
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	gid int64 // my replica group ID
	last_seq   int
	seq        int
	config     shardmaster.Config
	configstate     ConfigState
}

func (kv *ShardKV) logOperation(op Op) {
	seq := kv.seq
	wait_init := 10 * time.Millisecond
	DPrintf("Server %d:%d logOperation %v\n", kv.gid, kv.me, op)
	wait := wait_init
	for {
		fate, v := kv.px.Status(seq)
		if fate == paxos.Decided {
			vop := v.(Op)
			DPrintf("Server %d:%d : seq %d : %v\n", kv.gid, kv.me, seq, vop)
			if op.Op == vop.Op && op.Seq == vop.Seq && (op.Op == "Reconfigure" || op.ID == vop.ID){
				break
			}
			seq++
			wait = wait_init
		} else {
			DPrintf("Server %d:%d starts a new paxos instance : %d %v\n", kv.gid, kv.me, seq, op)
			kv.px.Start(seq, op)
			time.Sleep(wait)
			if wait < time.Second {
				wait *= 2
			}
		}
	}
	kv.seq = seq + 1
}

func (kv *ShardKV) catchUp() (rep *GetReply) {
	seq := kv.last_seq
	for seq < kv.seq {
		_, v := kv.px.Status(seq)
		op := v.(Op)
		if op.Op == "Reconfigure" {
			kv.config = kv.sm.Query(op.Seq)
			extra := op.Extra.(ConfigState)
			kv.configstate.Update(&extra)
		} else if op.Op == "Put" || op.Op == "Append" {
			if kv.gid == kv.config.Shards[key2shard(op.Key)] {
				if op.Op == "Put" {
					kv.configstate.KVStore[op.Key] = op.Value
				} else if op.Op == "Append" {
					kv.configstate.KVStore[op.Key] += op.Value
				}
				rep = &GetReply{OK, ""}
				kv.configstate.ClientRequestMap[op.ID] = op.Seq
				kv.configstate.ClientRepliesMap[op.ID] = *rep
			} else {
				rep = &GetReply{ErrWrongGroup, ""}
			}
		} else {
			if kv.gid == kv.config.Shards[key2shard(op.Key)] {
				value, ok := kv.configstate.KVStore[op.Key]
				if ok {
					rep = &GetReply{OK, value}
				} else {
					rep = &GetReply{ErrNoKey, ""}
				}
				kv.configstate.ClientRequestMap[op.ID] = op.Seq
				kv.configstate.ClientRepliesMap[op.ID] = *rep
			} else {
				rep = &GetReply{ErrWrongGroup, ""}
			}
		}
		kv.px.Done(seq)
		seq++
	}
	kv.last_seq = seq
	return
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("server %d:%d Get: client: %d seq: %d key: %s\n", kv.gid, kv.me, args.ID, args.Seq, args.Key)
	kv.catchUp()
	last_seq := kv.configstate.ClientRequestMap[args.ID]
	if args.Seq < last_seq {
		return nil
	} else if args.Seq == last_seq {
		reply.Err, reply.Value = kv.configstate.ClientRepliesMap[args.ID].Err, kv.configstate.ClientRepliesMap[args.ID].Value
		return nil
	}
	op := Op{args.ID, args.Seq, "Get", args.Key, "", nil}
	kv.logOperation(op)
	rep := kv.catchUp()
	reply.Err, reply.Value = rep.Err, rep.Value
	return nil
}


// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("server %d:%d PutAppend: client: %d seq: %d op: %s key: %s value: %s\n", kv.gid, kv.me, args.ID, args.Seq, args.Op, args.Key, args.Value)
	kv.catchUp()
	last_seq := kv.configstate.ClientRequestMap[args.ID]
	if args.Seq < last_seq {
		return nil
	} else if args.Seq == last_seq {
		reply.Err = kv.configstate.ClientRepliesMap[args.ID].Err
		return nil
	}
	op := Op{args.ID, args.Seq, args.Op, args.Key, args.Value, nil}
	kv.logOperation(op)
	reply.Err = kv.catchUp().Err
	return nil
}

func (kv *ShardKV) transfer(gid int64, shard int) (*ConfigState) {
	for _, server := range kv.config.Groups[gid] {
		var reply TransferStateReply
		ok := call(server, "ShardKV.TransferState", &TransferStateArgs{kv.config.Num, shard}, &reply)
		if ok && reply.Err == OK {
			return &reply.ConfigState
		}
	}
	return nil
}

func (kv *ShardKV) TransferState(args *TransferStateArgs, reply *TransferStateReply) error {
	if kv.config.Num < args.ConfigNum {
		reply.Err = "Error: Server not ready."
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("RPC TransferState : server %d:%d : args %v\n", kv.gid, kv.me, args)
	reply.ConfigState.Init()
	for key := range kv.configstate.KVStore {
		if key2shard(key) == args.Shard {
			value := kv.configstate.KVStore[key]
			reply.ConfigState.KVStore[key] = value
		}
	}
	for client := range kv.configstate.ClientRequestMap {
		reply.ConfigState.ClientRequestMap[client] = kv.configstate.ClientRequestMap[client]
		reply.ConfigState.ClientRepliesMap[client] = kv.configstate.ClientRepliesMap[client]
	}
	reply.Err = OK
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.catchUp()
	latest_config := kv.sm.Query(-1)
	for n := kv.config.Num + 1; n <= latest_config.Num; n++ {
		config := kv.sm.Query(n)
		kv.catchUp()
		var configstate ConfigState
		configstate.Init()
		for shard := 0; shard < shardmaster.NShards; shard++ {
			gid := kv.config.Shards[shard]
			if config.Shards[shard] == kv.gid && gid != 0 && gid != kv.gid {
				ret := kv.transfer(gid, shard)
				if ret == nil {
					return
				}
				configstate.Update(ret)
			}
		}
		op := Op{Seq:config.Num, Op:"Reconfigure", Extra:configstate}
		kv.logOperation(op)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(ConfigState{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.configstate.Init()

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)


	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
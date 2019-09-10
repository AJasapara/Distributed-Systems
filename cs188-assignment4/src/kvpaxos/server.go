package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	ID int64
	Operation string
	Key string
	Value string
	Destination string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	peers      []string
	// Your definitions here.
	key_val_store map[string]string
	logCounter int
	duplicateRequests map[int64]*GetReply
}

func (kv *KVPaxos) InterpretLog(seq int) {
	for i := kv.logCounter; i <= seq; i+=1{
		status, val := kv.px.Status(i)
		if status == paxos.Decided{
	        op := val.(Op)
	        //DPrintf("KVPaxos.InterpretLog(%v): Interpeted Log for op_id: %v and i: %d\n", kv.me, op.ID, i)
	        // Check for duplicate requests
	        _, duplicate := kv.duplicateRequests[op.ID]
	        if duplicate == true {
	        	continue
	        }
	        //DPrintf("KVPaxos.InterpretLog(%v): Not Duplicate Interpeted Log for op_id: %v and i: %d\n", kv.me, op.ID, i)
	        if op.Operation == "Get" {
	        	getValue, ok := kv.key_val_store[op.Key]
						if op.Destination == kv.peers[kv.me] {
							kv.duplicateRequests[op.ID] = &GetReply{"", getValue}
						}
	        	if ok == true {
	        		//kv.duplicateRequests[op.ID] = &GetReply{"", getValue}
	        		//DPrintf("Get %d request key: %s value: %s", kv.me, op.Key, getValue)
							//DPrintf("KVPaxos.InterpretLog(%v): Get request key: %v, value: %v\n", kv.me, op.Key, getValue)
	        	} else {
	        		kv.duplicateRequests[op.ID] = &GetReply{ErrNoKey, ""}
	        		//DPrintf("KVPaxos.InterpretLog(%v): Get request key: %v Not Found\n", kv.me, op.Key)
	        	}

	        } else if op.Operation == "Put"{
	        	kv.key_val_store[op.Key] = op.Value
						kv.duplicateRequests[op.ID] = &GetReply{"", kv.key_val_store[op.Key]}
	        	//DPrintf("Put %d request key: %s value: %s map: %+v\n", kv.me, op.Key, op.Value, kv.key_val_store)
						//DPrintf("KVPaxos.InterpretLog(%v): Put request key: %v, value: %v, map: %+v\n", kv.me, op.Key, kv.key_val_store[op.Key], kv.key_val_store)
	        } else if op.Operation == "Append"{
	        	original_value, ok := kv.key_val_store[op.Key]
	        	if ok == false {
	        		original_value = ""
	        	}
	        	new_value := original_value + op.Value
	        	kv.key_val_store[op.Key] = new_value
						kv.duplicateRequests[op.ID] = &GetReply{"", kv.key_val_store[op.Key]}
	        	//DPrintf("Append %d request key: %s value: %s map: %+v\n", kv.me, op.Key, new_value, kv.key_val_store)
						//DPrintf("KVPaxos.InterpretLog(%v): Append request key: %v, value: %v, map: %+v\n", kv.me, op.Key, kv.key_val_store[op.Key], kv.key_val_store)
	        }
	    }
	}
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	//DPrintf("KVPaxos.Get(%v): Received Get request: %+v\n", kv.me, args)
	op := Op{args.ID, "Get", args.Key, "", kv.peers[kv.me]}
	kv.mu.Lock()
	//DPrintf("KVPaxos.Get(%v): Lock acquired for Get request ID: %v\n", kv.me, args.ID)
	_, duplicateFound := kv.duplicateRequests[op.ID]
	if duplicateFound == true {
		//DPrintf("KVPaxos.Get(%v): Duplicate detected for Get request ID: %v\n", kv.me, args.ID)
		//DPrintf("KVPaxos.Get(%v): Value stored: %v\n", kv.me, kv.key_val_store[args.Key])
		reply.Value = kv.key_val_store[args.Key]
		reply.Err = ""
		kv.mu.Unlock()
		return nil
	}
	seq := kv.RunPaxos(op)
	//DPrintf("KVPaxos.Get(%v): Ran Paxos for ID: %v and got back seq: %v\n", kv.me, args.ID, seq)
	kv.InterpretLog(seq)
	//DPrintf("KVPaxos.Get(%v): Ran InterpretLog for ID: %v for seq: %v\n", kv.me, args.ID, seq)
	kv.px.Done(seq)
	//DPrintf("KVPaxos.Get(%v): Ran Done for ID: %v for seq: %v\n", kv.me, args.ID, seq)
	kv.logCounter = seq + 1
	value := kv.duplicateRequests[op.ID]
	//DPrintf("KVPaxos.Get(%v): Succesful Get Request for ID: %v and err: %v\n", kv.me, args.ID, value.Err)
	//DPrintf("KVPaxos.Get(%v): Completed Get Request for ID: %v with key: %v, value: %v\n", kv.me, args.ID, args.Key, value.Value)
	//DPrintf("KVPaxos.Get(%v): key_val_store[%v]: %v\n", kv.me, op.Key, kv.key_val_store[op.Key])
	reply.Value = value.Value
	//DPrintf("KVPaxos.Get(%v): Get reply.Value for ID: %v with key: %v, value: %v\n", kv.me, args.ID, args.Key, reply.Value)
	reply.Err = value.Err
	kv.duplicateRequests[op.ID] = &GetReply{}
	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	//DPrintf("KVPaxos.PutAppend(%v): Received PutAppend request: %+v\n", kv.me, args)
	op := Op{args.ID, args.Op, args.Key, args.Value, kv.peers[kv.me]}
	kv.mu.Lock()
	_, duplicateFound := kv.duplicateRequests[op.ID]
	if duplicateFound == true {
		kv.mu.Unlock()
		return nil
	}
	seq := kv.RunPaxos(op)
	//DPrintf("KVPaxos.PutAppend(%v): Ran Paxos and got back seq: %v\n", kv.me, seq)
	kv.InterpretLog(seq)
	kv.px.Done(seq)
	kv.logCounter = seq + 1
	//value := kv.duplicateRequests[op.ID]
	//DPrintf("KVPaxos.PutAppend(%v): Succesful Put/Append Request for ID: %v and err: %v\n", kv.me, args.ID, value.Err)
	//DPrintf("KVPaxos.PutAppend(%v): Completed Put/Append Request for ID: %v with key: %v, value: %v\n", kv.me, args.ID, args.Key, value.Value)
	reply.Err = ""
	kv.duplicateRequests[op.ID] = &GetReply{}
	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) RunPaxos (op Op) int {
	seq := kv.logCounter
	for {
		kv.px.Start(seq, op)
		//DPrintf("Called %d runPaxos with seq: %d and Op: %v and map: %+v\n", kv.me, seq, op.ID, kv.key_val_store)
		//DPrintf("KVPaxos.RunPaxos(%v): Called runPaxos with seq: %d and Op ID: %v:\n", kv.me, seq, op.ID)
		to := 10 * time.Millisecond
	    for {
	        status, val := kv.px.Status(seq)
	        if status == paxos.Decided{
	        	value := val.(Op)
	        	//DPrintf("KVPaxos.RunPaxos(%v): Paxos ID Decided: %v with seq number: %d\n", kv.me, value.ID, seq)
	        	// Compare values to see if original request was accepted
	        	if value == op {
	        		return seq
	        	}
	        	break
	        }
	        time.Sleep(to)
	        if to < 10 * time.Second {
	            to *= 2
	        }
	    }
	    seq += 1
	}
	// Should never return this value
	return -1
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	//DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.peers = servers

	// Your initialization code here.
	kv.key_val_store = make(map[string]string)
	kv.logCounter = 0
	kv.duplicateRequests = make(map[int64]*GetReply)

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()
	return kv
}

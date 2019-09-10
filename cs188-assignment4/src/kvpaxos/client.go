package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"
import "time"
import "fmt"
import "log"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	currServer int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key, nrand()}
	for {
		for _, server := range ck.servers {
			reply := GetReply{}
			ok := call(server, "KVPaxos.Get", &args, &reply)
			if ok == true {
				DPrintf("KVPaxos.Clerk.Get(): ID: %v, Made request for key: %v to server: %v\n", args.ID, key, server)
				if reply.Err == "" {
					if reply.Value == "" {
						DPrintf("KVPaxos.Clerk.Get(): ID: %v, Requested key: %v, got back empty string from server: %v\n", args.ID, key, server)
					}

					if reply.Value != "" {
						DPrintf("KVPaxos.Clerk.Get(): ID: %v, Requested key: %v, got back string (%v) from server: %v\n", args.ID, key, reply.Value, server)
					}
					return reply.Value
				}
				if reply.Err == ErrNoKey {
					DPrintf("KVPaxos.Clerk.Get(): Server did not find value for key: %v\n", key)
					return ""
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ""
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{key, value, op, nrand()}
	for {
		for _, server := range ck.servers {
			reply := PutAppendReply{}
			ok := call(server, "KVPaxos.PutAppend", &args, &reply)
			if ok == true {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
//import "log"
import "crypto/rand"
import "math/big"

type Clerk struct {
	vs 	*viewservice.Clerk
	// Your declarations here
	CurrentView		viewservice.View

}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.CurrentView = viewservice.View{}
	ck.CurrentView.Viewnum = 0
	ck.CurrentView.Primary = ""
	ck.CurrentView.Backup = ""

	//log.SetFlags(//log.Ldate | //log.Ltime | //log.Lmicroseconds)

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

func (ck *Clerk) UpdateView() {
	// Note: The ViewService initially treated all pings as being from an available servver.
	// This would have been an issue because it then would have treated a Client requesting the View as an avilable server.
	// So I have added a field called "Client" (bool) to PingArgs in viewservice/common.go.
	// Whenever the Client wants the current view, it will set the "Client" field in PingArgs to true.
	// If a server is pinging the ViewService, it will set the "Client" field in PingArgs to false.

	// The code in ViewService.Ping now will distinguish between client pings and server pings.

	////log.Printf("Client.UpdateView(): Updating View\n")
	reply := viewservice.PingReply{}
	ok := call(ck.vs.ClerkServer(), "ViewServer.Ping", viewservice.PingArgs{ck.vs.ClerkMe(), ck.CurrentView.Viewnum, true}, &reply)
	if ok == true {
		ck.CurrentView.Viewnum = reply.View.Viewnum
		ck.CurrentView.Primary = reply.View.Primary
		ck.CurrentView.Backup = reply.View.Backup
	}

	////log.Printf("Client.UpdateView(): Finished updating view. ok: %v, CurrentView: %v\n", ok, ck.CurrentView)
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	//log.Printf("Client.Get(): About to request (key: %v)\n", key)

	for ck.CurrentView.Primary == "" {
		ck.UpdateView()
	}

	if ck.CurrentView.Primary != "" {
		ok := false
		usingCachedPrimary := true
		for ok == false {
			if (usingCachedPrimary == false) || (ck.CurrentView.Primary == "") {
				ck.UpdateView()
				usingCachedPrimary = true
			}

			args := GetArgs{Key: key}
			reply := GetReply{}
			ok = call(ck.CurrentView.Primary, "PBServer.Get", &args, &reply)
			if ok == true {
				if reply.Err == "" {
					//log.Printf("Client.Get(): Success. (Key: %v, Value: %v)\n", key, reply.Value)
					return reply.Value
				}

				if reply.Err == ErrNoKey {
					//log.Printf("Client.Get(): (Key: %v) not found\n", key)
					return ""
				}

				if reply.Err != "" {
					usingCachedPrimary = false
					ok = false
					continue
				}
			}

			if ok == false {
				////log.Printf("Client.Get(): Request didn't work. Trying again\n")
				usingCachedPrimary = false
			}
		}
	}

	// Technically, this code should never execute because the for loop will repeat until the request is answered
	// But Go was complaining about no return statement, so I added this
	return ""
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	if op == "Put" {
		//log.Printf("Client.PutAppend(): About to put (key: %v, value: %v)\n", key, value)
	}
	if op == "Append" {
		//log.Printf("Client.PutAppend(): About to append (key: %v, value: %v)\n", key, value)
	}

	for ck.CurrentView.Primary == "" {
		ck.UpdateView()
	}

	if ck.CurrentView.Primary != "" {
		ok := false
		usingCachedPrimary := true
		args := PutAppendArgs{key, value, op, nrand()}
		for ok == false {
			if (usingCachedPrimary == false) || (ck.CurrentView.Primary == "") {
				ck.UpdateView()
				usingCachedPrimary = true
			}
			reply := PutAppendReply{}
			ok = call(ck.CurrentView.Primary, "PBServer.PutAppend", &args, &reply)

			if ok == true {
				if reply.Err != "" {
					usingCachedPrimary = false
					ok = false
					continue
				}
				//log.Printf("Client.PutAppend(): Success. (Key: %v, Value: %v) put/append\n", key, value)
				return
			}

			if ok == false {
				//log.Printf("Client.PutAppend(): Request didn't work. Trying again\n")
				usingCachedPrimary = false
			}
		}
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

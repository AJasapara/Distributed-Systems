package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ServerState struct {
	mostRecentViewNum uint
	mostRecentPingTime time.Time
}

func (serverState *ServerState) String() string {
	return fmt.Sprintf(
		"ServerState{viewNum: %v, pingTime: %v:%v:%v}",
		serverState.mostRecentViewNum,
		serverState.mostRecentPingTime.Hour(),
		serverState.mostRecentPingTime.Minute(),
		serverState.mostRecentPingTime.Second())
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	currentView View
	serversMostRecentPings map[string]*ServerState
	primaryAck bool
	backupAck bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	if args.Client == true {
		log.Printf("ViewServer.Ping(): Received ping from client: (args: %+v)\n", args)
		vs.mu.Lock()
		reply.View = vs.currentView
		vs.mu.Unlock()
	}

	if args.Client != true {
		server := args.Me
		serverMostRecentViewnum := args.Viewnum
		//log.Printf("ViewServer.Ping(): From %v with viewNum: %v\n", server, serverMostRecentViewnum)
		vs.mu.Lock()
		newServerState := &ServerState{serverMostRecentViewnum, time.Now()}
		vs.serversMostRecentPings[server] = newServerState
		//log.Printf("ViewServer.Ping(): Updated serversMostRecentPings: %v\n", vs.serversMostRecentPings)

		// Determine if the client is the primary and if the primary has acknowledged view
		if server == vs.currentView.Primary {
			if vs.primaryAck == false && serverMostRecentViewnum == vs.currentView.Viewnum {
				vs.primaryAck = true
				log.Printf("ViewServer.Ping(): Primary Ack received from %v\n", server)
			}
		}

		if server == vs.currentView.Backup {
			if vs.backupAck == false && serverMostRecentViewnum == vs.currentView.Viewnum {
				vs.backupAck = true
				log.Printf("ViewServer.Ping(): Backup Ack received from %v\n", server)
			}
		}
		vs.mu.Unlock()

		//log.Printf("ViewServer.Ping(): Current view: %v\n", vs.currentView)
		reply.View = vs.currentView
	}

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//args will be empty because the definition of GetArgs has no members
	vs.mu.Lock()
	log.Printf("ViewServer.Get(): Current view: %v\n", vs.currentView)
	reply.View = vs.currentView
	vs.mu.Unlock()

	return nil
}


// isAvailable() checks for 2 things:
// 1. That the most recent ping is less than DeadPings back in time
// 2. That the most recent ping's viewnum is up to 1 behind the current view number
func (serverState *ServerState) isAvailable(server string, vs *ViewServer) bool {
	now := time.Now()
	oldestAcceptedTime := now.Add(-(DeadPings * PingInterval))
	if serverState.mostRecentPingTime.Before(oldestAcceptedTime) {
		log.Println("ViewServer.isAvailable(): Server is unavailable")
		log.Printf("ViewServer.isAvailable(): oldestAcceptedTime: %v\n", oldestAcceptedTime)
		log.Printf("ViewServer.isAvailable(): most recent ping: %v\n", serverState.mostRecentPingTime)
		return false
	}

	if vs.currentView.Viewnum != 0 {
		if server == vs.currentView.Primary {
			if vs.primaryAck == true && (serverState.mostRecentViewNum < (vs.currentView.Viewnum - 1)) {
				return false
			}
		}

		if server == vs.currentView.Backup {
			if vs.backupAck == true && (serverState.mostRecentViewNum < (vs.currentView.Viewnum - 1)) {
				return false
			}
		}
	}

	return true
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.

	// First off, determine if we can get a primary server
	if vs.currentView.Primary == "" {
		// Scan the serversMostRecentPings map to see if there is an available server
		vs.mu.Lock()
		for key := range vs.serversMostRecentPings {
			// If the time of the most recent ping is less than DeadPings, assign primary
			if vs.serversMostRecentPings[key].isAvailable(key, vs) {
				vs.currentView.Primary = key
				vs.currentView.Viewnum = vs.currentView.Viewnum + 1
				log.Printf("ViewServer.tick(): Primary updated: %v\n", vs.currentView.Primary)
				log.Printf("VIewServer.tick(): New view number: %v\n", vs.currentView.Viewnum)
				vs.primaryAck = false
				vs.backupAck = false
				break
			}
		}
		vs.mu.Unlock()
	}

	// Now determine if we can get a backup server
	if vs.currentView.Backup == "" {
		// Scan the serversMostRecentPings map to see if there is an available server
		vs.mu.Lock()
		for key := range vs.serversMostRecentPings {
			// If the time of the most recent ping is less than DeadPings, assign backup
			if vs.serversMostRecentPings[key].isAvailable(key, vs) && key != vs.currentView.Primary{
				vs.currentView.Viewnum = vs.currentView.Viewnum + 1
				vs.currentView.Backup = key
				log.Printf("ViewServer.tick(): Backup updated: %v\n", vs.currentView.Backup)
				log.Printf("ViewServer.tick(): New view number: %v\n", vs.currentView.Viewnum)
				vs.primaryAck = false
				vs.backupAck = false
				break
			}
		}
		vs.mu.Unlock()
	}

	// Determine if primary has died
	vs.mu.Lock()
	primaryName := vs.currentView.Primary
	if primaryName != "" {
		log.Println("ViewServer.tick(): Checking if primary is available.")
		log.Printf("ViewServer.tick(): Primary viewnum: %v, vs viewnum: %v", vs.serversMostRecentPings[primaryName].mostRecentViewNum, vs.currentView.Viewnum)
		if (vs.serversMostRecentPings[primaryName].isAvailable(primaryName, vs) == false) {
			if vs.primaryAck == true && vs.currentView.Backup != "" {
				vs.currentView.Primary = vs.currentView.Backup
				vs.primaryAck = false
				vs.backupAck = false
				vs.currentView.Backup = ""
				vs.currentView.Viewnum = vs.currentView.Viewnum + 1
				log.Printf("ViewServer.tick(): Primary died. Updated to: %v\n", vs.currentView.Primary)
				log.Printf("ViewServer.tick(): Backup is now: %v\n", vs.currentView.Backup)
				log.Printf("ViewServer.tick(): New view number: %v\n", vs.currentView.Viewnum)
			}
		}
	}
	vs.mu.Unlock()

	// Determine if backup has died
	vs.mu.Lock()
	backupName := vs.currentView.Backup
	if backupName != "" {
		log.Println("ViewServer.tick(): Checking if backup is available.")
		log.Printf("ViewServer.tick(): Backup viewnum: %v, vs viewnum: %v", vs.serversMostRecentPings[backupName].mostRecentViewNum, vs.currentView.Viewnum)
		if vs.serversMostRecentPings[backupName].isAvailable(backupName, vs) == false {
			vs.currentView.Backup = ""
			vs.currentView.Viewnum = vs.currentView.Viewnum + 1
			vs.primaryAck = false
			vs.backupAck = false

			log.Println("ViewServer.tick(): Backup died.")
			log.Printf("ViewServer.tick(): Backup is now: %v\n", vs.currentView.Backup)
			log.Printf("ViewServer.tick(): New view number: %v\n", vs.currentView.Viewnum)
		}
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{}
	vs.currentView.Viewnum = 0
	vs.currentView.Primary = ""
	vs.currentView.Backup = ""

	vs.serversMostRecentPings = make(map[string]*ServerState)

	vs.primaryAck = false
	vs.backupAck = false

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}

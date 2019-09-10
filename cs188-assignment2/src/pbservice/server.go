package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "container/list"

const (
	Primary = "Primary"
	Backup = "Backup"
	None = "None"
)

type ServerStatus string

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	status 	   ServerStatus
	currentView *viewservice.View
	key_val_store map[string]string
	job_id_store map[int64]bool
	job_counter  int64
	backup_queue *list.List
	disconnected bool
}

func (pb *PBServer) ForwardToBackup(args *ForwardBackupArgs, reply *ForwardBackupReply) error{
	pb.mu.Lock()
	log.Printf("Just received to ForwardToBackup: %+v append args: %+v because my last id: %d\n", args, args.AppendArgs, pb.job_counter)
	localStatus := pb.status
	localJobCounter := pb.job_counter
	pb.mu.Unlock()
	if localStatus == Primary {
		reply.Err = ErrWrongServer
		return nil
	} 
	if args.JobCounter > localJobCounter + 1 {
		pb.mu.Lock()
		pb.backup_queue.PushBack(args)
		pb.mu.Unlock()
		log.Printf("Just added to backup_queue: %+v append args: %+v because my last id: %d\n", args, args.AppendArgs, pb.job_counter)
		reply.Err = ""
		return nil
	} else if args.JobCounter == localJobCounter + 1 {
		appendReply := PutAppendReply{}
		pb.PutAppend(args.AppendArgs, &appendReply)
		if appendReply.Err == "" {
			reply.Err = ""
			pb.mu.Lock()
			e := pb.backup_queue.Front()
			pb.mu.Unlock()
			for e != nil {
				pb.mu.Lock()
				localJobCounter = pb.job_counter
				pb.mu.Unlock()
				//log.Printf("List details: element: %+v length: %d\n", e.Value.(*ForwardBackupArgs), pb.backup_queue.Len())
			    backupArgs := e.Value.(*ForwardBackupArgs)
			    if backupArgs.JobCounter == localJobCounter + 1 {
			    	appendReply := PutAppendReply{}
			    	log.Printf("Just removed from backup_queue: %+v append args: %+v and my last id: %d\n", backupArgs, backupArgs.AppendArgs, localJobCounter)
			    	pb.PutAppend(backupArgs.AppendArgs, &appendReply)
			    	pb.mu.Lock()
			    	pb.backup_queue.Remove(e)
			    	log.Printf("Backup queue length: %d and last id: %d\n", pb.backup_queue.Len(), localJobCounter)
			    	if pb.backup_queue.Len() == 0 {
			    		break
			    	}
			    	e = pb.backup_queue.Front()
			    	pb.mu.Unlock()
			    } else {
			    	e = e.Next()
			    }
			}
			
		} else {
			reply.Err = Err("PutAppend Operation failed with error: " + appendReply.Err)
		}
	}
	return nil
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	if pb.disconnected == true {
		reply.Err = ErrWrongServer
		return nil
	}
	key := args.Key
	log.Printf("PBServer.Get(): Received Get request with args: %+v\n", args)

	pb.mu.Lock()
	value, ok := pb.key_val_store[key]
	pb.mu.Unlock()

	if ok == true {
		reply.Value = value
		reply.Err = ""
		log.Printf("PBServer.Get(): Success. (Key: %v, Value: %v)\n", key, value)
	}

	if ok == false {
		reply.Err = ErrNoKey
		reply.Value = ""
		log.Printf("PBServer.Get(): Key %v not found\n", key)
	}

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	job, ok := pb.job_id_store[args.ID]
	pb.mu.Unlock()
	if ok ==true && job == true {
	    return nil
	}
	key := args.Key
	value := args.Value
	operation := args.Operation
	var localJobCounter int64

	log.Printf("PBServer.PutAppend(): %s Received PutAppend request with args: %+v\n", pb.status, args)

	if operation == "Put" {
		pb.mu.Lock()
		pb.key_val_store[key] = value
		pb.job_id_store[args.ID] = true
		pb.job_counter += 1
		localJobCounter = pb.job_counter
		reply.Err = ""
		log.Printf("PBServer.PutAppend(): %s %s successful. Map: %v\n", pb.status, operation, pb.key_val_store)
		pb.mu.Unlock()
	}

	if operation == "Append" {
		pb.mu.Lock()
		original_value, ok := pb.key_val_store[key]
		if ok == false {
			original_value = ""
		}
		new_value := original_value + value
		pb.key_val_store[key] = new_value
		pb.job_id_store[args.ID] = true
		pb.job_counter += 1
		localJobCounter = pb.job_counter
		reply.Err = ""
		log.Printf("PBServer.PutAppend(): %s %s successful. Map: %v\n", pb.status, operation, pb.key_val_store)
		pb.mu.Unlock()
	}
	if pb.status == Primary && pb.currentView.Backup != "" {
		ok := false
		for ok == false {
			forwardReply := ForwardBackupReply{}
			log.Printf("Called %s backup with args: %+v and appendArgs: %+v", operation, ForwardBackupArgs{args, localJobCounter}, args)
			ok = call(pb.currentView.Backup, "PBServer.ForwardToBackup", ForwardBackupArgs{args, localJobCounter}, &forwardReply)
			if ok == true{
				if forwardReply.Err != "" {
					reply.Err = "Backup Forwarding Failed."
				} else {
					log.Printf("Successfully forwarded to %s to backup with args: %+v and appendArgs: %+v", operation, ForwardBackupArgs{args, localJobCounter}, args)
				}
			}
			if ok == false{
				reply.Err = "Failed to call ForwardToBackup"
				log.Printf("Failed to call ForwardToBackup in %s for args: %+v\n", operation, args)
			}
		}
	}

	return nil
}

// Copy the key_val_store
func (pb *PBServer) CopyKeyValStore(args *CopyKeyValStoreArgs, reply *CopyKeyValStoreReply) error {
	log.Printf("PBServer.CopyKeyValStore(): About to copy the key val store to backup from source: (%v)\n", args.SourceServer)
	pb.mu.Lock()
	for key, value := range args.KeyValStore {
		pb.key_val_store[key] = value
	}
	for key, value := range args.JobIDStore {
		pb.job_id_store[key] = value
	}
	pb.job_counter = args.JobCounter
	reply.Err = ""
	log.Printf("PBServer.CopyKeyValStore(): Backup's map is now: %v\n", pb.key_val_store)
	pb.mu.Unlock()

	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	log.Printf("PBServer.tick(): Status: %v (Type: %T), Whoami: %v\n", pb.status, pb.status, pb.me)
	var reply viewservice.PingReply
	ok := call(pb.vs.ClerkServer(), "ViewServer.Ping", viewservice.PingArgs{pb.me, pb.currentView.Viewnum, false}, &reply)
	backupAppeared := false
	if ok == true {
		pb.mu.Lock()
		if pb.currentView != nil && pb.currentView.Viewnum < reply.View.Viewnum{
			if reply.View.Primary == pb.me && pb.status != Primary {
				pb.status = Primary
				log.Printf("PBServer.tick(): Was just set to primary.\n")
			} else if reply.View.Backup == pb.me && pb.status != Backup {
				pb.status = Backup
				log.Printf("PBServer.tick(): Was just set to backup.\n")
			} else if reply.View.Primary == pb.me && pb.status == Primary {
			} else if reply.View.Backup == pb.me && pb.status == Backup {
			} else {
				pb.status = None
			}

			if (pb.currentView.Backup) == "" && (reply.View.Backup != "") {
				log.Printf("PBServer.tick(): Whoami: %v, Backup appeared: (%v)\n", pb.me, reply.View.Backup)
				backupAppeared = true
			}
		}
		pb.currentView = &reply.View
		pb.mu.Unlock()

		// If a new backup comes online, then transfer all the key-value store to the backup
		log.Printf("Whoami: %v, Status: %v\n", pb.me, pb.status)
		log.Printf("pb.status == Primary: %v\n", pb.status == Primary)
		if backupAppeared && (pb.status == Primary) {
			// Send RPC to backup with complete key-value store
			pb.mu.Lock()
			args := CopyKeyValStoreArgs{pb.me, pb.key_val_store, pb.job_id_store, pb.job_counter}
			reply := CopyKeyValStoreReply{}
			ok := call(pb.currentView.Backup, "PBServer.CopyKeyValStore", &args, &reply)
			pb.mu.Unlock()
			if ok == true {
				if reply.Err == "" {
					log.Printf("PBServer.tick(): Copy of key_val_store to backup successful.\n")
				}

				if reply.Err != "" {
					log.Printf("PBServer.tick(): Could not copy key_val_store to backup.\n")
				}
			}

			if ok == false {
				log.Printf("PBServer.tick(): Request to copy backup didn't work.\n")
			}

			backupAppeared = false
		}
	}
	if ok == false {
		ok := call(pb.vs.ClerkServer(), "ViewServer.Ping", viewservice.PingArgs{pb.me, pb.currentView.Viewnum, false}, &reply)
		if ok == false {
			pb.disconnected = true
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.currentView = new(viewservice.View)
	pb.currentView.Viewnum = 0
	pb.key_val_store = make(map[string]string)
	pb.job_id_store = make(map[int64]bool)
	pb.job_counter = 0
	pb.backup_queue = list.New()
	pb.disconnected = false

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}

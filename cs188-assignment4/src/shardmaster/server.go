package shardmaster

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
import "math/big"

// Note that in order to import crypto's rand package for use in the nrand()
// function, (which was copied from kvpaxos/client.go), I needed to rename the
// package to "crypto_rand" so that the "rand" from crypto does not conflict
// with the rand from the math package.
import crypto_rand "crypto/rand"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	max_config_so_far int
	logCounter int
	duplicateRequests map[int64]Config
}


type Op struct {
	// Not all of these fields will be usable for all operations.
	// However, we will keep all possible fields in this Op and the servers, while
	// using Paxos, will pass around this Op type for proposal values.

	// Upon interpreting their logs, they will figure out what the Operation is
	// and therefore will know what fields are necessary to consult.

	// Note that is the responsibility of the server who wants to start a Paxos
	// sequence to correctly fill out all the fields of the Op based on the
	// Operation.

	// Valid operations are: Query, Join, Leave, Move

	Operation string
	OperationID int64
	Num int
	GID int64
	Servers []string
	Shard int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crypto_rand.Int(crypto_rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (sm *ShardMaster) InterpretLog(seq int) {
	// NOTE THAT THIS FUNCTION SHOULD ONLY BE CALLED WHEN THE CALLING FUNCTION
	// HAS A LOCK!

	for i := sm.logCounter; i<=seq; i+=1 {
		status, val := sm.px.Status(i)

		if status == paxos.Decided {
			op := val.(Op)

			request, requestFound := sm.duplicateRequests[op.OperationID]
			if requestFound == true {
				log.Printf("ShardMaster.InterpretLog(%v): Duplicate request found: %+v\n", sm.me, request)
				continue
			}

			if op.Operation == "Query" {
				log.Printf("ShardMaster.InterpretLog(%v): Handling query for seq: %v\n", sm.me, i)
				if (op.Num == -1 || op.Num > sm.max_config_so_far) {
					sm.duplicateRequests[op.OperationID] = sm.configs[sm.max_config_so_far]
				}

				if !(op.Num == -1 || op.Num > sm.max_config_so_far) {
					sm.duplicateRequests[op.OperationID] = sm.configs[op.Num]
				}
			}

			if op.Operation == "Join" {
				log.Printf("ShardMaster.InterpretLog(%v): Handling join for seq: %v\n", sm.me, i)
				// Create new config from previous
				sm.max_config_so_far = sm.max_config_so_far + 1
				sm.configs = append(sm.configs, sm.CopyPreviousConfig(sm.max_config_so_far))

				// Then add the new group to the Groups member for the config
				sm.configs[sm.max_config_so_far].Groups[op.GID] = op.Servers

				// Rebalance shards
				log.Printf("ShardMaster.InterpretLog(%v): Join state before rebalancing: %+v\n", sm.me, sm.configs[sm.max_config_so_far])
				sm.RebalanceShards()
				log.Printf("ShardMaster.InterpretLog(%v): Join state after rebalancing: %+v\n", sm.me, sm.configs[sm.max_config_so_far])
			}

			if op.Operation == "Leave" {
				log.Printf("ShardMaster.InterpretLog(%v): Handling leave for seq: %v\n", sm.me, i)
				// Create new config from previous
				sm.max_config_so_far = sm.max_config_so_far + 1
				sm.configs = append(sm.configs, sm.CopyPreviousConfig(sm.max_config_so_far))

				// Now remove the group specified in args.GID
				log.Printf("ShardMaster.InterpretLog(%v): State right now: %+v\n", sm.me, sm.configs[sm.max_config_so_far])
				log.Printf("ShardMaster.InterpretLog(%v): Deleting group: %v\n", sm.me, op.GID)
				delete(sm.configs[sm.max_config_so_far].Groups, op.GID)

				// Also, set the shards in that group to be unallocated so that CreateGroupToShardMap()
				// and RebalanceShards() can reallocate those shards

				// Note that 0 is used to indicate an invalid group assignment
				for shardIndex, group := range sm.configs[sm.max_config_so_far].Shards {
					if group == op.GID {
						sm.configs[sm.max_config_so_far].Shards[shardIndex] = 0
					}
				}

				// Now rebalance the shards among remaining groups
				log.Printf("ShardMaster.InterpretLog(%v): Leave state before rebalancing: %+v\n", sm.me, sm.configs[sm.max_config_so_far])
				sm.RebalanceShards()
				log.Printf("ShardMaster.InterpretLog(%v): Leave state after rebalancing: %+v\n", sm.me, sm.configs[sm.max_config_so_far])
			}

			if op.Operation == "Move" {
				log.Printf("ShardMaster.InterpretLog(%v): Handling move for seq: %v\n", sm.me, i)
				// Create new config from previous
				sm.max_config_so_far = sm.max_config_so_far + 1
				sm.configs = append(sm.configs, sm.CopyPreviousConfig(sm.max_config_so_far))

				// Now change shard assignment
				sm.configs[sm.max_config_so_far].Shards[op.Shard] = op.GID
			}

			if op.Operation != "Query" && op.Operation != "Join" && op.Operation != "Leave" && op.Operation != "Move" {
				log.Printf("ShardMaster.InterpretLog(%v): Operation (%v) not allowed.\n", sm.me, op.Operation)
			}
		}
	}
}

func (sm *ShardMaster) CopyPreviousConfig(currentConfigNum int) Config {
	// NOTE THAT THIS FUNCTION SHOULD ONLY BE CALLED WHEN THE CALLING FUNCTION
	// HAS A LOCK!

	// Create new config
	new_config := Config{}
	// Set new config's Num equal to max config seen so far
	new_config.Num = currentConfigNum

	// Set new config's Groups parameter
	new_config.Groups = make(map[int64][]string)

	// Copy previous config's Groups map to the new config only if there is a previous config
	if currentConfigNum != 1 {
		for gid, servers := range sm.configs[currentConfigNum - 1].Groups {
			new_config.Groups[gid] = servers
		}
	}

	// Copy previous config's Shards to new config
	new_config.Shards = sm.configs[currentConfigNum - 1].Shards

	return new_config
}

func (sm *ShardMaster) CreateGroupToShardMap() map[int64][]int {
	// Get a group->shards mapping (i.e. reverse the Shards parameter so it's a map[int64][]int)
	gid_to_shards := make(map[int64][]int)
	for shard_index, group := range sm.configs[sm.max_config_so_far].Shards {
		// If any group is 0, that means the shard has either never been assigned
		// a group, or it was assigned to a group, but then the group left.
		// So for now, just assign those shards to the first valid group and then
		// RebalanceShards will take care of re-allocating the shards
		if group == 0 {
			// Find lowest valid group id
			valid_group_id := int64(-1)
			for key, _ := range sm.configs[sm.max_config_so_far].Groups {
				if valid_group_id != -1 {
					if key < valid_group_id {
						valid_group_id = key
					}
				} else {
					valid_group_id = key
				}
			}

			gid_to_shards[valid_group_id] = append(gid_to_shards[valid_group_id], shard_index)
		} else {
			gid_to_shards[group] = append(gid_to_shards[group], shard_index)
		}
	}

	return gid_to_shards
}

func (sm *ShardMaster) RebalanceShards() {
	// NOTE THAT THIS FUNCTION SHOULD ONLY BE CALLED WHEN THE CALLING FUNCTION
	// HAS A LOCK!

	// At the beginning of the server initialization, all shards are assigned to group id 0, which
	// is invalid. Upon the first valid group joining, just assign all shards to
	// the id of the first valid group

	// Get all the possible group IDs and add them to an array
	group_ids := make([]int64, 0, len(sm.configs[sm.max_config_so_far].Groups))
	for group, _ := range sm.configs[sm.max_config_so_far].Groups {
		group_ids = append(group_ids, group)
	}
	log.Printf("ShardMaster.RebalanceShards(%v): group_ids: %v\n", sm.me, group_ids)

	// If there is only 1 group in the group_ids, then this is the first group to
	// join, so assign all shards to this valid group id
	if len(group_ids) == 1 {
		valid_group_id := int64(-1)
		for key, _ := range sm.configs[sm.max_config_so_far].Groups {
			valid_group_id = key
			break
		}

		// Assign all shards to the valid group id
		for shardIndex, _ := range sm.configs[sm.max_config_so_far].Shards {
			sm.configs[sm.max_config_so_far].Shards[shardIndex] = valid_group_id
		}
		return
	}

	// If the above case is not true, then that means we have more than 1 group and
	// we should rebalance.

	// Iterate through all groups
	// For each group, there are 2 cases:
	// 	1. A group handles less than floor(#shards/#groups) shards:
	//    Then keep pulling highest index shards from lowest group id with > floor(#shards/#groups)
	//    until you handle floor(#shards/#groups) shards
	//  2. A group handles > floor(#shards/#groups) + 1 shards:
	//    Then keep giving highest index shards to lowest group id in case 1 until you handle
	//    floor(#shards/#groups) + 1 shards

	// Calculate floor(#shards/#groups)
	minimum_shard_amt := len(sm.configs[sm.max_config_so_far].Shards) / len(group_ids)
	log.Printf("ShardMaster.RebalanceShards(%v): minimum_shard_amt: %v\n", sm.me, minimum_shard_amt)

	// Get a group->shards mapping (i.e. reverse the Shards parameter so it's a map[int64][]int)
	gid_to_shards := sm.CreateGroupToShardMap()
	log.Printf("ShardMaster.RebalanceShards(%v): gid_to_shards: %v\n", sm.me, gid_to_shards)

	if len(group_ids) > len(sm.configs[sm.max_config_so_far].Shards) {
		// If there are more groups than shards, and all shards are already allocated,
		// then don't do anything
		allShardsAllocated := true
		for _, group := range sm.configs[sm.max_config_so_far].Shards {
			if group == 0 {
				allShardsAllocated = false
				break
			}
		}

		if allShardsAllocated == true {
			return
		} else {
			// This case would happen if there are more groups than shards and not all
			// shards are allocated, which could happen if a leave request were issued
			// for a group that previously held a shard.

			// So determine which shards are unallocated and round robin assign them to
			// available servers.

			// Determine shards that are unallocated
			unallocatedShards := make([]int, 0)
			for shardIndex, group := range sm.configs[sm.max_config_so_far].Shards {
				if group == 0 {
					unallocatedShards = append(unallocatedShards, shardIndex)
				}
			}

			// For each unallocated shard, find the lowest group id with no shards
			// Any group that doesn't have shards will be in group_ids, but not in gid_to_shards
			availableGroups := make(map[int64]bool)
			for _, groupNum := range group_ids {
				_, groupFound := gid_to_shards[groupNum]

				if groupFound == false {
					availableGroups[groupNum] = true
				}
			}

			if len(availableGroups) > 0 {
				for _, shard := range unallocatedShards {
					// Find the lowest available group id
					lowest_group_id := int64(-1)
					for group_id, _ := range availableGroups {
						if lowest_group_id != -1 {
							if group_id < lowest_group_id {
								lowest_group_id = group_id
							}
						} else {
							lowest_group_id = group_id
						}
					}
					if lowest_group_id != -1 {
						sm.configs[sm.max_config_so_far].Shards[shard] = lowest_group_id
						delete(availableGroups, lowest_group_id)
					}
				}
			}
		}
	}

	// Do balancing next

	for _, group := range group_ids {
		if len(gid_to_shards[group]) < minimum_shard_amt {
			// Keep pulling shards from lowest group id with > minimum_shard_amt until
			// you handle minimum_shard_amt shards

			for len(gid_to_shards[group]) < minimum_shard_amt {
				// Calculate the lowest group id that handles > minimum_shard_amt
				shardGiver := int64(-1)
				for groupNum, _ := range gid_to_shards {
					if len(gid_to_shards[groupNum]) > minimum_shard_amt {
						if shardGiver != -1 {
							if groupNum < shardGiver {
								shardGiver = groupNum
							}
						} else {
							shardGiver = groupNum
						}
					}
				}

				// This case would happen if there are no other groups to consider
				// In that case, do not reassign the shard
				if shardGiver == -1 {
					break // out of the outer for loop
				}

				// Calculate the shard giver's highest index shard
				highestShard := -1
				for _, shard := range gid_to_shards[shardGiver] {
					if shard > highestShard {
						highestShard = shard
					}
				}

				// Assign the shard to the current group
				sm.configs[sm.max_config_so_far].Shards[highestShard] = group

				// Re-calculate the gid->shard mapping
				gid_to_shards = sm.CreateGroupToShardMap()
			}
		}

		if len(gid_to_shards[group]) > (minimum_shard_amt + 1) {
			// Keep giving shards to any group that has less than minimum_shard_amt
			// until you handle minimum_shard_amt + 1 shards

			for len(gid_to_shards[group]) > (minimum_shard_amt + 1) {
				// Calculate highest index shard to give to recipient group
				highestShard := -1
				for _, shard := range gid_to_shards[group] {
					if shard > highestShard {
						highestShard = shard
					}
				}

				// Find the lowest group id that handles < minimum_shard_amt
				shardRecipient := int64(-1)
				for groupNum, _ := range gid_to_shards {
					if len(gid_to_shards[groupNum]) <= minimum_shard_amt {
						if shardRecipient != -1 {
							if groupNum < shardRecipient {
								shardRecipient = groupNum
							}
						} else {
							shardRecipient = groupNum
						}
					}
				}

				// This case would happen if there are no other groups to consider
				// In that case, do not reassign the shard
				if shardRecipient == -1 {
					break // out of the outer for loop
				}

				// Assign the shard to the current group
				sm.configs[sm.max_config_so_far].Shards[highestShard] = shardRecipient

				// Re-calculate the gid -> shard mapping for each iteration of the outer for loop
				gid_to_shards = sm.CreateGroupToShardMap()
			}
		}
	}

	// After having assigned shards, it is possible that some shards will still have
	// value 0 in the Shards field of the config. This could happen if those shards
	// were never reassigned.

	// So go through the Shards field and set each 0 entry to be what it should be,
	// based on the gid_to_shards mapping
	for shardIndex, group := range sm.configs[sm.max_config_so_far].Shards {
		if group == 0 {
			// If the group is 0, then that shard has not been reallocated
			// So figure out which group now owns that shard by going through the
			// gid_to_shards map
			assigned_group := int64(-1)

			for group, _ := range gid_to_shards {
				for _, shard := range gid_to_shards[group] {
					if shard == shardIndex {
						assigned_group = group
						break
					}
				}
				if assigned_group != -1 {
					break
				}
			}
			sm.configs[sm.max_config_so_far].Shards[shardIndex] = assigned_group
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// TODO: Need to handle duplicate Join requests

	// Because the client does not send an operation ID, the only way to detect
	// duplicate Join requests is to see if the same GID has been seen already?
	// TODO: Confirm this on Piazza

	// TODO: What do we do if the group identifier is not unique?
	// Can a server join an already existing group?

	sm.mu.Lock()
	log.Printf("ShardMaster.Join(%v): Received Join request: %+v\n", sm.me, args)
	_, gid_found := sm.configs[sm.max_config_so_far].Groups[args.GID]
	if gid_found == true {
		log.Printf("ShardMaster.Join(%v): gid (%v) already exists, will return immediately.\n", sm.me, args.GID)
		sm.mu.Unlock()
		return nil
	}

	op := Op{}
	op.Operation = "Join"
	op.OperationID = nrand()
	op.GID = args.GID
	op.Servers = args.Servers
	//The Num (int) and Shard (int) parameters do not have to be set

	log.Printf("ShardMaster.Join(%v): Running paxos for ID: %v\n", sm.me, op.OperationID)
	seq := sm.RunPaxos(op)

	log.Printf("ShardMaster.Join(%v): Interpreting log for ID: %v\n", sm.me, op.OperationID)
	sm.InterpretLog(seq)

	log.Printf("ShardMaster.Join(%v): Calling Done(seq: %v) for ID: %v\n", sm.me, seq, op.OperationID)
	sm.px.Done(seq)

	sm.logCounter = seq + 1

	sm.mu.Unlock()

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// TODO: Need to handle duplicate Leave requests
	// TODO: What do you do if the args.GID doesn't exist?

	// Because the client does not send a unique operation ID in the request,
	// the only way to tell if the request is a duplicate is if the group ID doesn't exist
	// TODO: Confirm this on Piazza

	sm.mu.Lock()
	log.Printf("ShardMaster.Leave(%v): Received Leave request: %+v\n", sm.me, args)
	_, gid_found := sm.configs[sm.max_config_so_far].Groups[args.GID]

	if gid_found == false {
		log.Printf("ShardMaster.Leave(%v): gid (%v) does not exist, will return immediately.\n", sm.me, args.GID)
		sm.mu.Unlock()
		return nil
	}

	op := Op{}
	op.Operation = "Leave"
	op.OperationID = nrand()
	op.GID = args.GID
	//The Num (int), Servers ([]string), Shard (int) parameters do not have to be set

	log.Printf("ShardMaster.Leave(%v): Running paxos for ID: %v\n", sm.me, op.OperationID)
	seq := sm.RunPaxos(op)

	log.Printf("ShardMaster.Leave(%v): Interpreting log for ID: %v\n", sm.me, op.OperationID)
	sm.InterpretLog(seq)

	log.Printf("ShardMaster.Join(%v): Calling Done(seq: %v) for ID: %v\n", sm.me, seq, op.OperationID)
	sm.px.Done(seq)

	sm.logCounter = seq + 1

	sm.mu.Unlock()

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	op := Op{}
	op.Operation = "Move"
	op.OperationID = nrand()
	op.GID = args.GID
	op.Shard = args.Shard
	//The Num (int) and Servers ([]string) parameters do not have to be set

	sm.mu.Lock()
	log.Printf("ShardMaster.Move(%v): Received Move request: %+v\n", sm.me, args)

	seq := sm.RunPaxos(op)

	sm.InterpretLog(seq)

	sm.px.Done(seq)

	sm.logCounter = seq + 1

	sm.mu.Unlock()

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	log.Printf("ShardMaster.Query(%v): Received Query request: %+v\n", sm.me, args)

	op := Op{}
	op.Operation = "Query"
	op.OperationID = nrand()
	op.Num = args.Num
	//The GID (int64), Servers ([]string), and Shard (int) parameters do not have to be set

	seq := sm.RunPaxos(op)

	sm.InterpretLog(seq)

	sm.px.Done(seq)

	sm.logCounter = seq + 1

	reply.Config = sm.duplicateRequests[op.OperationID]

	sm.duplicateRequests[op.OperationID] = Config{}

	sm.mu.Unlock()

	return nil
}

func (sm *ShardMaster) RunPaxos (op Op) int {
	seq := sm.logCounter
	for {
		sm.px.Start(seq, op)
		to := 10 * time.Millisecond
	    for {
	        status, val := sm.px.Status(seq)
	        if status == paxos.Decided{
	        	value := val.(Op)
	        	// Compare values to see if original request was accepted
	        	if value.OperationID == op.OperationID {
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

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.configs[0].Num = 0

	sm.max_config_so_far = 0
	sm.logCounter = 0

	sm.duplicateRequests = make(map[int64]Config)

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}

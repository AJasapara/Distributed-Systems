package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type AgreementInstance struct {
	Sequence int
	Status Fate
	N_p int //Highest proposal number promised to accept
	N_a int //Highest proposal number accepted
	V_a interface{} //Value accepted
}

type PrepareArgs struct {
	Sequence int
	N int
	From int
}

type PrepareReply struct {
	Sequence int
	Ok bool
	From int
	N int
	N_a int
	V_a interface{}
}

type AcceptArgs struct {
	Sequence int
	N int //This is n in the pseudocode
	V interface{}
	From int
}

type AcceptReply struct {
	Sequence int
	Ok bool
	From int
	N int //This is n in the pseudocode
}

type DecidedArgs struct {
	Sequence int
	N int // This is n in the pseudocode
	V interface{}
	From int
	Sender_chain map[string]bool
}

type DecidedReply struct {
	Sequence int
	Ok bool
	From int
	N int
	V interface{}
}

type DoneArgs struct {
	Whoami string
	Seq_X int
	Source string
	Sender_chain map[string]bool
}

type DoneReply struct {
	Ok bool
	Whoami string
	Seq_X int
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	agreementInstances map[int]*AgreementInstance
	peerDoneVals map[string]int
	doneVal int

}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) RunPaxosProtocol(seq int, v interface{}) {
	N_counter := 0

	agreementDecided := false

	for agreementDecided == false {
		args := PrepareArgs{}
		args.Sequence = seq
		args.N = N_counter
		args.From = px.me

		prepare_replies := make(map[string]PrepareReply)

		for index, peer := range px.peers {
			reply := PrepareReply{}

			if index == px.me {
				//log.Printf("Paxos.RunPaxosProtocol(%v): Index is me, calling LocalPrepare() for (seq: %v, N: %v) immediately.\n", px.me, seq, N_counter)
				px.LocalPrepare(&args, &reply)

				if reply.Ok == true {
					prepare_replies[peer] = PrepareReply{reply.Sequence, reply.Ok, reply.From, reply.N, reply.N_a, reply.V_a}
					//log.Printf("Paxos.RunPaxosProtocol(%v): propose for (seq: %v, N: %v) accepted from: %v\n", px.me, seq, N_counter, peer)
				}

				if reply.Ok == false {
					prepare_replies[peer] = PrepareReply{reply.Sequence, reply.Ok, reply.From, reply.N, reply.N_a, reply.V_a}
					//log.Printf("Paxos.RunPaxosProtocol(%v): propose for (seq: %v, N: %v) rejected from: %v\n", px.me, seq, N_counter, peer)
					continue
				}
			}

			if index != px.me {
				//log.Printf("Paxos.RunPaxosProtocol(%v): Calling PrepareRPC(%v) for (seq: %v, N: %v).\n", px.me, index, seq, N_counter)
				ok := call(peer, "Paxos.PrepareRPC", &args, &reply)

				if ok == true {
					if reply.Ok == true {
						prepare_replies[peer] = PrepareReply{reply.Sequence, reply.Ok, reply.From, reply.N, reply.N_a, reply.V_a}
						//log.Printf("Paxos.RunPaxosProtocol(%v): propose for (seq: %v, N: %v) accepted from: %v\n", px.me, seq, N_counter, peer)
					}

					if reply.Ok == false {
						prepare_replies[peer] = PrepareReply{reply.Sequence, reply.Ok, reply.From, reply.N, reply.N_a, reply.V_a}
						// This case happens if the acceptor has already promised to accept a
						// proposal with a higher n value for this sequence.

						// For now, I am just skipping this peer.
						// Later, if there isn't a majority for propose phase, then the proposer will sleep for a random hundred multiple of Milliseconds
						//log.Printf("Paxos.RunPaxosProtocol(%v): propose for (seq: %v, N: %v) rejected from: %v\n", px.me, seq, N_counter, peer)
						continue
					}
				}

				if ok == false {
					//log.Printf("Paxos.RunPaxosProtocol(%v): call() for (seq: %v, N: %v) propose failed to: %v\n", px.me, seq, N_counter, peer)
					continue
				}
			}
		}

		// Check at this point to see if a majority have sent propose_ok
		numPeers := len(px.peers)
		countOk := 0
		highest_N := -1
		for _, reply := range prepare_replies {
			if reply.Ok == true {
				countOk = countOk + 1
			}

			if reply.Ok == false {
				if reply.N > highest_N {
					highest_N = reply.N
				}
			}
		}

		if countOk > (numPeers/2) {
			//log.Printf("Paxos.RunPaxosProtocol(%v): majority accepted for (seq: %v, N: %v) propose.\n", px.me, seq, N_counter)
			// Choose the correct v' and then send accept to all servers
			// v' will either be:
			// 1. The v_a with the highest n_a
			// 2. The v passed to this function

			//Read through all ok_replies
			highest_n_a := -1
			var v_proposal interface{}
			for _, reply := range prepare_replies {
				if reply.Ok == true && reply.N_a > highest_n_a {
					v_proposal = reply.V_a
				}
			}

			if v_proposal == nil {
				v_proposal = v
			}

			//log.Printf("Paxos.RunPaxosProtocol(%v): (seq: %v, N: %v, v_proposal: %v)\n\n", px.me, seq, N_counter, v_proposal)

			// Now send all the accept messages
			args := AcceptArgs{seq, N_counter, v_proposal, px.me}
			accept_ok_replies := make(map[string]AcceptReply)

			for index, peer := range px.peers {
				reply := AcceptReply{}

				if index == px.me {
					//log.Printf("Paxos.RunPaxosProtocol(%v): Index is me, calling LocalAccept() for (seq: %v, N: %v) immediately.\n", px.me, seq, N_counter)
					px.LocalAccept(&args, &reply)

					if reply.Ok == true {
						accept_ok_replies[peer] = AcceptReply{reply.Sequence, reply.Ok, reply.From, reply.N}
						//log.Printf("Paxos.RunPaxosProtocol(%v): accept for (seq: %v, N: %v) accepted from: %v\n", px.me, seq, N_counter, peer)
					}

					if reply.Ok == false {
						// This case happens if the acceptor's n_p value is higher than the
						// n value for this sequence.

						// For now, I am just skipping this peer.
						// Later, if there isn't a majority for accept phase, then the proposer will sleep for a random hundred multiple of Milliseconds.
						//log.Printf("Paxos.RunPaxosProtocol(%v): accept for (seq: %v, N: %v) rejected from: %v\n", px.me, seq, N_counter, peer)
						continue
					}
				}

				if index != px.me {
					//log.Printf("Paxos.RunPaxosProtocol(%v): Calling AcceptRPC(%v) for (seq: %v, N: %v).\n", px.me, index, seq, N_counter)
					ok := call(peer, "Paxos.AcceptRPC", &args, &reply)

					if ok == true {
						if reply.Ok == true {
							//log.Printf("Paxos.RunPaxosProtocol(%v): accept for (seq: %v, N: %v) accepted from: %v\n", px.me, seq, N_counter, peer)
							accept_ok_replies[peer] = AcceptReply{reply.Sequence, reply.Ok, reply.From, reply.N}
						}

						if reply.Ok == false {
							//log.Printf("Paxos.RunPaxosProtocol(%v): accept for (seq: %v, N: %v) rejected from: %v\n", px.me, seq, N_counter, peer)
							continue
						}
					}

					if ok == false {
						//log.Printf("Paxos.RunPaxosProtocol(%v): call() for (seq: %v, N: %v) accept failed to: %v\n", px.me, seq, N_counter, peer)
						continue
					}
				}
			}

			// Check at this point to see if a majority have sent accept_ok
			numPeers := len(px.peers)
			if len(accept_ok_replies) > (numPeers/2) {
				// Send decided(v) to all
				//log.Printf("Paxos.RunPaxosProtocol(%v): majority accepted for (seq: %v, N: %v) accept.\n", px.me, seq, N_counter)

				sender_chain := make(map[string]bool)
				px.DecidedPropagate(seq, N_counter, v_proposal, sender_chain)

				agreementDecided = true
				break
			}

			if !(len(accept_ok_replies) > (numPeers/2)) {
				random_sleep_multiple := rand.Intn(10)
				//log.Printf("Paxos.RunPaxosProtocol(%v): no majority for (seq %v, N: %v) accept. Running loop again after sleeping %v milliseconds.\n", px.me, seq, N_counter, 100*random_sleep_multiple)
				time.Sleep(time.Duration(100 * random_sleep_multiple) * time.Millisecond)
				N_counter = N_counter + 1
				continue
			}
		}

		if !(countOk > (numPeers/2)) {
			// This continue is for the outer for loop (for agreementDecided == false)
			random_sleep_multiple := rand.Intn(10)
			//log.Printf("Paxos.RunPaxosProtocol(%v): no majority for (seq: %v, N: %v) propose. Running loop again after sleeping %v milliseconds.\n", px.me, seq, N_counter, 100*random_sleep_multiple)
			N_counter = highest_N + 1
			time.Sleep(time.Duration(100 * random_sleep_multiple) * time.Millisecond)
			continue
		}
	}
}

func (px *Paxos) MakeDecidedCall(peer string, seq int, args *DecidedArgs, reply *DecidedReply) {
	ok := false

	for ok == false {
		log.Printf("Paxos.MakeDecidedCall(%v) About to call Decided for (seq: %v, N: %v) for: %v\n", px.me, seq, args.N, peer)
		ok = call(peer, "Paxos.DecidedRPC", &args, &reply)

		if ok == true {
			if reply.Ok == true {
				log.Printf("Paxos.MakeDecidedCall(%v) Decided for (seq: %v, N: %v) accepted by: %v\n", px.me, seq, args.N, reply.From)
			}

			if reply.Ok == false {
				log.Printf("Paxos.MakeDecidedCall(%v) Decided for (seq: %v, N: %v) rejected (because its N < N_p of recipient) by: %v\n", px.me, seq, args.N, reply.From)
			}
		}

		if ok == false {
			log.Printf("Paxos.RunPaxosProtocol(%v): call() for (seq: %v, N: %v) decided failed to: %v\n", px.me, seq, args.N, peer)
			log.Printf("Paxos.RunPaxosProtocol(%v): Will retry decision (seq: %v, N: %v) send to: %v\n", px.me, seq, args.N, peer)
			time.Sleep(50 * time.Millisecond)

			continue
		}
	}
}

func (px *Paxos) LocalPrepare(args *PrepareArgs, reply *PrepareReply) error {
	//log.Printf("Paxos.LocalPrepare(%v): (seq: %v, N: %v) Received PrepareArgs: %v\n", px.me, args.Sequence, args.N, *args)
	//log.Printf("Paxos.LocalPrepare(%v): (seq: %v, N: %v) About to acquire lock at beginning of method.\n", px.me, args.Sequence, args.N)
	seq := args.Sequence
	px.mu.Lock()
	_, sequenceFound := px.agreementInstances[args.Sequence]
	// New agreement instance
	if sequenceFound == false {
		//log.Printf("Paxos.LocalPrepare(%v): New: (seq: %v, N: %v)\n", px.me, args.Sequence, args.N)
		newAgreementInstance := AgreementInstance{}
		newAgreementInstance.Sequence = args.Sequence
		newAgreementInstance.Status = Pending
		newAgreementInstance.N_p = -1
		newAgreementInstance.N_a = -1
		newAgreementInstance.V_a = nil
		px.agreementInstances[args.Sequence] = &newAgreementInstance
	}

	N_p := px.agreementInstances[seq].N_p

	if args.N > N_p {
		px.agreementInstances[seq].N_p = args.N
		// Don't set the status to Pending if the Status were already Decided
		// Just because a new proposal comes in, it doesn't mean that the Status should be set to Pending
		// because there is already a previously decided value.
		// So wait until DecidedRPC is called for this peer to update its value for the seq
		if px.agreementInstances[seq].Status != Decided {
			px.agreementInstances[seq].Status = Pending
		}

		reply.Sequence = seq
		reply.Ok = true
		reply.From = px.me
		reply.N = args.N
		reply.N_a = px.agreementInstances[seq].N_a
		reply.V_a = px.agreementInstances[seq].V_a

		//log.Printf("Paxos.LocalPrepare(%v): Sent back prepare_accept to (px: %v) for (seq: %v, N: %v): %v\n", px.me, args.From, seq, args.N, *reply)

	}

	if !(args.N > N_p) {
		//log.Printf("Paxos.LocalPrepare(%v): Sent back prepare_reject to (px: %v) for (seq: %v, N: %v): %v\n", px.me, args.From, seq, args.N, *reply)
		reply.Sequence = seq
		reply.Ok = false
		reply.From = px.me
		reply.N = px.agreementInstances[seq].N_p
	}

	//agreement := px.agreementInstances[seq]
	//log.Printf("Paxos.LocalPrepare(%v): (seq: %v, N: %v) State of agreement: %v\n\n", px.me, seq, args.N, agreement)
	px.mu.Unlock()
	//log.Printf("Paxos.LocalPrepare(%v): (seq: %v, N: %v) Released lock from beginning of method.\n", px.me, seq, args.N)

	return nil
}

func (px *Paxos) PrepareRPC(args *PrepareArgs, reply *PrepareReply) error {
	err := px.LocalPrepare(args, reply)
	return err
}

func (px *Paxos) LocalAccept(args *AcceptArgs, reply *AcceptReply) error {
	//log.Printf("Paxos.LocalAccept(%v): (seq: %v, N: %v) Received AcceptArgs: %v\n", px.me, args.Sequence, args.N, *args)
	sequence := args.Sequence
	//log.Printf("Paxos.LocalAccept(%v): (seq: %v, N: %v) About to acquire lock at beginning of method.\n", px.me, sequence, args.N)
	px.mu.Lock()
	agreementInstance, found := px.agreementInstances[sequence]
	if found == false {
		reply.Ok = false
		reply.From = px.me
		//log.Printf("Paxos.LocalAccept(%v): Agreement Instance for (seq: %v, N: %v) not found in agreementInstances.\n", px.me, sequence, args.N)
		px.mu.Unlock()
		//log.Printf("Paxos.LocalAccept(%v): (seq: %v, N: %v) Released lock at beginning of method because instance not found.\n", px.me, sequence, args.N)
		return nil
	}

	if args.N >= agreementInstance.N_p {
		px.agreementInstances[sequence].N_p = args.N
		px.agreementInstances[sequence].N_a = args.N
		px.agreementInstances[sequence].V_a = args.V
		// Don't set the status to Pending if the Status were already Decided
		// Just because a new proposal comes in, it doesn't mean that the Status should be set to Pending
		// because there is already a previously decided value.
		// So wait until DecidedRPC is called for this peer to update its value for the seq
		if px.agreementInstances[sequence].Status != Decided {
			px.agreementInstances[sequence].Status = Pending
		}

		reply.Sequence = args.Sequence
		reply.Ok = true
		reply.From = px.me
		reply.N = args.N

		//log.Printf("Paxos.LocalAccept(%v): Sent back accept_ok to (px: %v) for (seq: %v, N: %v): %v\n", px.me, args.From, sequence, args.N, *reply)
	}

	if !(args.N >= agreementInstance.N_p) {
		reply.Ok = false
		reply.From = px.me
		//log.Printf("Paxos.LocalAccept(%v): Sent back accept_reject to (px: %v) for (seq: %v, N: %v): %v\n", px.me, args.From, sequence, args.N, *reply)
	}

	//agreement := px.agreementInstances[sequence]
	//log.Printf("Paxos.LocalAccept(%v): (seq: %v, N: %v) State of agreement: %v\n\n", px.me, sequence, args.N, agreement)
	px.mu.Unlock()
	//log.Printf("Paxos.LocalAccept(%v): (seq: %v, N: %v) Released lock from beginning of method.\n", px.me, sequence, args.N)

	return nil
}

func (px *Paxos) AcceptRPC(args *AcceptArgs, reply *AcceptReply) error {
	err := px.LocalAccept(args, reply)
	return err
}

func (px *Paxos) LocalDecided(args *DecidedArgs, reply *DecidedReply) error {
	//log.Printf("Paxos.LocalDecided(%v): (seq: %v, N: %v) Received DecidedArgs: %v\n", px.me, args.Sequence, args.N, *args)
	sequence := args.Sequence
	//log.Printf("Paxos.LocalDecided(%v): (seq: %v, N: %v) About to acquire lock at beginning of method.\n", px.me, sequence, args.N)
	px.mu.Lock()
	_, found := px.agreementInstances[sequence]

	if found == false {
		// This case would happen if a machine comes back online and hasn't been part of the propose or accept phases
		// So tell this server to make a new agreement instance with status Decided and the correct V_a value
		newAgreementInstance := new(AgreementInstance)
		newAgreementInstance.Sequence = args.Sequence
		newAgreementInstance.Status = Decided
		// N_p and N_a need to be set to N because the machine must have come back online and therefore does not have N_p or N_a values for this sequence
		newAgreementInstance.N_p = args.N
		newAgreementInstance.N_a = args.N
		newAgreementInstance.V_a = args.V
		px.agreementInstances[sequence] = newAgreementInstance

		reply.Sequence = sequence
		reply.Ok = true
		reply.From = px.me
		reply.N = args.N
		reply.V = px.agreementInstances[sequence].V_a
		//log.Printf("Paxos.LocalDecided(%v): Sent back decided_ok to (px: %v) for (seq: %v, N: %v): %v\n", px.me, args.From, sequence, args.N, *reply)
	}

	if found == true {
		if args.N >= px.agreementInstances[sequence].N_p {
			px.agreementInstances[sequence].Status = Decided
			px.agreementInstances[sequence].V_a = args.V
			px.agreementInstances[sequence].N_a = args.N

			reply.Sequence = sequence
			reply.Ok = true
			reply.From = px.me
			reply.N = args.N
			reply.V = px.agreementInstances[sequence].V_a
			//log.Printf("Paxos.LocalDecided(%v): Sent back decided_ok to (px: %v) for (seq: %v, N: %v): %v\n", px.me, args.From, sequence, args.N, *reply)
		}

		if !(args.N >= px.agreementInstances[sequence].N_p) {
			reply.Ok = false
			//log.Printf("Paxos.LocalDecided(%v): Sent back decided_reject to (px: %v) for (seq: %v, N: %v): %v\n", px.me, args.From, sequence, args.N, *reply)
		}
	}

	//log.Printf("Paxos.LocalDecided(%v): (seq: %v, N: %v) decided with (value: %v).\n\n", px.me, args.Sequence, args.N, args.V)
	//agreement := px.agreementInstances[sequence]
	//log.Printf("Paxos.LocalDecided(%v): (seq: %v, N: %v) State of agreement: %v\n\n", px.me, sequence, args.N, agreement)

	// Handle propagation of Decided values to other peers to handle network partitions
	_, in_sender_chain := args.Sender_chain[px.peers[px.me]]

	if in_sender_chain == false {
		sender_chain := args.Sender_chain
		sender_chain[px.peers[px.me]] = true

		px.mu.Unlock()
		//log.Printf("Paxos.LocalDecided(%v) (seq: %v, n_value: %v, value: %v) Releasing lock from beginning of method to allow DonePropagate() to continue.\n", px.me, args.Sequence, args.N, args.V)
		px.DecidedPropagate(args.Sequence, args.N, args.V, sender_chain)

		// After completing the above call to DecidedPropagate(), will need to reacquire lock
		// Otherwise, don't need to reacquire lock
		//log.Printf("Paxos.LocalDecided(%v): (seq: %v, n_value: %v, value: %v) About to re-acquire lock from beginning of method to allow LocalDone() to continue.\n", px.me, args.Sequence, args.N, args.V)
		px.mu.Lock()
	}

	px.mu.Unlock()
	//log.Printf("Paxos.LocalDecided(%v): (seq: %v, n_value: %v, value: %v) Releasing lock from beginning of method.\n", px.me, args.Sequence, args.N, args.V)

	return nil
}

func (px *Paxos) DecidedRPC(args *DecidedArgs, reply *DecidedReply) error {
	err := px.LocalDecided(args, reply)
	return err
}

func (px *Paxos) DecidedPropagate(seq int, n_value int, value interface{}, sender_chain map[string]bool) {
	// Contact all other peers and tell them that a value has been decided for this seq

	args := DecidedArgs{}
	args.Sequence = seq
	args.N = n_value
	args.V = value
	args.From = px.me

	// Construct the list of peers to contact by not contacting anyone in sender_chain
	send_list := make([]string, 0)
	for _, peer := range px.peers {
		// If peer is not in sender_chain, then add it to send_list
		_, found := sender_chain[peer]
		if found == false {
			send_list = append(send_list, peer)
		}
	}

	sender_chain[px.peers[px.me]] = true
	args.Sender_chain = sender_chain
	//log.Printf("Paxos.DecidedPropagate(%v): send_list: (%v)\n", px.me, send_list)
	//log.Printf("Paxos.DecidedPropagate(%v): sender_chain: (%v)\n", px.me, args.Sender_chain)

	for _, peer := range send_list {
		reply := DecidedReply{}

		if peer == px.peers[px.me] {
			//log.Printf("Paxos.DecidedPropagate(%v): Index is me, calling LocalDecided() for (seq: %v, n_value: %v, value: %v) immediately.\n", px.me, seq, args.N, args.V)
			px.LocalDecided(&args, &reply)
		}

		if peer != px.peers[px.me] {
			//log.Printf("Paxos.RunPaxosProtocol(%v): Calling MakeDecidedCall(%v) for (seq: %v, n_value: %v, value: %v).\n", px.me, index, seq, args.N, args.V)
			go px.MakeDecidedCall(peer, seq, &args, &reply)
		}
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Don't need to acquire lock because Min() already does that
	//log.Printf("Paxos.Start(%v): Received start request for (seq: %v).\n", px.me, seq)
	min := px.Min()

	if seq < min {
		//log.Printf("Paxos.Start(%v): Ignoring start call because (seq: %v) < (min: %v).\n", px.me, seq, min)
		return
	}

	status, _ := px.Status(seq)
	if status == Decided {
		return
	}

	//log.Printf("Paxos.Start(%v): New sequence initiated: (seq: %v, v: %v)\n", px.me, seq, v)
	go px.RunPaxosProtocol(seq, v)
}


func (px *Paxos) Done(seq int) {
	sender_chain := make(map[string]bool)
	px.DonePropagate(seq, sender_chain)
}
//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) DonePropagate(seq int, sender_chain map[string]bool) {
	// Contact all other peers and tell them that you are done with all instances <= seq

	args := DoneArgs{}
	args.Whoami = px.peers[px.me]
	args.Seq_X = seq
	args.Source = px.peers[px.me]

	// Construct the list of peers to contact by not contacting anyone in sender_chain
	send_list := make([]string, 0)
	for _, peer := range px.peers {
		// If peer is not in sender_chain, then add it to send_list
		_, found := sender_chain[peer]
		if found == false {
			send_list = append(send_list, peer)
		}
	}

	sender_chain[px.peers[px.me]] = true
	args.Sender_chain = sender_chain
	//log.Printf("Paxos.DonePropagate(%v): send_list: (%v)\n", px.me, send_list)
	//log.Printf("Paxos.DonePropagate(%v): sender_chain: (%v)\n", px.me, args.Sender_chain)
	for _, peer := range send_list {
		reply := DoneReply{}

		if peer == px.peers[px.me] {
			//log.Printf("Paxos.DonePropagate(%v): Index is me, calling LocalDone() for (seq: %v) immediately.\n", px.me, seq)
			px.LocalDone(&args, &reply)
		}

		if peer != px.peers[px.me] {
			//log.Printf("Paxos.DonePropagate(%v): About to call Done(seq: %v) for peer: %v\n", px.me, seq, peer)
			ok := call(peer, "Paxos.DoneRPC", &args, &reply)
			if ok == true {
				//log.Printf("Paxos.DonePropagate(%v): Done(seq: %v) successfully sent to %v\n", px.me, seq, peer)

				if reply.Ok == true {
					//log.Printf("Paxos.DonePropagate(%v) (seq: %v) About to acquire lock to read and update peerDoneVals.\n", px.me, seq)
					px.mu.Lock()
					_, ok := px.peerDoneVals[reply.Whoami]
					if ok == true {
						// Peer exists as key in peerDoneVals
						// Make sure this Seq_X is larger than what is currently stored
						if reply.Seq_X > px.peerDoneVals[reply.Whoami] {
							px.peerDoneVals[reply.Whoami] = reply.Seq_X
						}
					}

					if ok == false {
						// Don't store a doneVal of -1 into peerDoneVals
						// Any server that has never called Done(seq) will have doneVal = -1
						if args.Seq_X > -1 {
							px.peerDoneVals[reply.Whoami] = reply.Seq_X
						}
					}

					px.mu.Unlock()
					//log.Printf("Paxos.DonePropagate(%v) (seq: %v) Released lock to read and update peerDoneVals.\n", px.me, seq)
				}
			}

			if ok == false {
				//log.Printf("Paxos.DonePropagate(%v): Call for Done(seq: %v) failed to: %v\n", px.me, seq, peer)
			}
		}
	}
}

func (px *Paxos) LocalDone(args *DoneArgs, reply *DoneReply) error {
	//log.Printf("Paxos.LocalDone(%v): Received DoneArgs: %v\n\n", px.me, *args)
	//log.Printf("Paxos.LocalDone(%v): (seq: %v) About to acquire lock at beginning of method.\n", px.me, args.Seq_X)
	px.mu.Lock()

	_, ok := px.peerDoneVals[args.Whoami]
	if ok == true {
		// Peer exists as key in peerDoneVals
		// Make sure this Seq_X value is larger than what is currently stored
		if args.Seq_X > px.peerDoneVals[args.Whoami] {
			px.peerDoneVals[args.Whoami] = args.Seq_X
			//log.Printf("Paxos.LocalDone(%v): Updated peerDoneVals: %v\n\n", px.me, px.peerDoneVals)
		}
	}

	if ok == false {
		// Don't store a doneVal of -1 into peerDoneVals
		// Any server that has never called Done(seq) will have doneVal = -1
		if args.Seq_X > -1 {
			px.peerDoneVals[args.Whoami] = args.Seq_X
			//log.Printf("Paxos.LocalDone(%v): Added new peer to peerDoneVals: %v\n\n", px.me, px.peerDoneVals)
		}
	}

	// Send back reply even if the peer is myself

	reply.Ok = true
	reply.Whoami = px.peers[px.me]
	if args.Source == px.peers[px.me] {
		reply.Seq_X = args.Seq_X
	}
	if args.Source != px.peers[px.me] {
		reply.Seq_X = px.doneVal
	}

	// Call Done(my px.doneVal, sender_chain) for all other peers
	_, in_sender_chain := args.Sender_chain[px.peers[px.me]]
	if in_sender_chain == false {
		sender_chain := args.Sender_chain
		sender_chain[px.peers[px.me]] = true
		myDoneVal := px.doneVal
		px.mu.Unlock()
		//log.Printf("Paxos.LocalDone(%v): (seq: %v) Releasing lock from beginning of method to allow DonePropagate() to continue.\n", px.me, myDoneVal)
		px.DonePropagate(myDoneVal, sender_chain)

		// After completing the above call to DonePropagate(), will need to reacquire lock
		// Otherwise, don't need to reacquire lock
		//log.Printf("Paxos.LocalDone(%v): (seq: %v) About to re-acquire lock from beginning of method to allow LocalDone() to continue.\n", px.me, myDoneVal)
		px.mu.Lock()
	}

	px.DeleteOldInstances()

	px.mu.Unlock()
	//log.Printf("Paxos.LocalDone(%v) (seq: %v) Releasing lock from beginning of method.\n", px.me, args.Seq_X)
	return nil
}

func (px *Paxos) DeleteOldInstances() {
	// Servers that have never called Done() will have doneVal = -1
	num_done_vals_stored := 0
	for _, peer := range px.peers {
		_, ok := px.peerDoneVals[peer]
		if ok == true {
			num_done_vals_stored = num_done_vals_stored + 1
		}
	}

	if num_done_vals_stored == len(px.peers) {
		// Each Paxos peer will have a Done value from each other peer
		// It should find the minimum, and then discard all sequences with sequence numbers <= that minimum
		// The Min() method returns that minimum + 1

		// I'm just setting an initial value for minDoneVal
		minDoneVal := px.peerDoneVals[px.peers[px.me]]
		for _, done_val := range px.peerDoneVals {
			if (done_val != -1) && (done_val < minDoneVal) {
				minDoneVal = done_val
			}
		}

		// Now set px's done_val:
		px.doneVal = minDoneVal

		// Now set all agreementInstances that have seq <= doneVal to Forgotten status

		sequencesToDelete := make([]int, 0)
		for seq, instance := range px.agreementInstances {
			if seq <= px.doneVal {
				instance.Status = Forgotten
				sequencesToDelete = append(sequencesToDelete, seq)
			}
		}
		//log.Printf("Paxos.LocalDone(%v): All agreement instances with seq <= doneVal: %v set to Forgotten status.\n", px.me, px.doneVal)

		//log.Printf("Paxos.LocalDone(%v): (doneVal: %v).\n\n", px.me, px.doneVal)

		// Now delete memory associated with any Forgotten instance
		for sequence := range sequencesToDelete {
			delete(px.agreementInstances, sequence)
		}
	}
}

func (px *Paxos) DoneRPC(args *DoneArgs, reply *DoneReply) error {
	err := px.LocalDone(args, reply)
	return err
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	//log.Printf("Paxos.Max(%v): About to acquire lock at beginning of method.\n", px.me)
	px.mu.Lock()
	highestSequenceSoFar := -1
	for sequence, _ := range px.agreementInstances {
		if sequence > highestSequenceSoFar {
			highestSequenceSoFar = sequence
		}
	}

	if (len(px.agreementInstances)) == 0 {
		// This case can happen if all agreement instances are deleted because everyone's
		// Done values are up to date. So the server deletes all agreementInstances
		highestSequenceSoFar = px.doneVal
	}
	px.mu.Unlock()
	//log.Printf("Paxos.Max(%v): Releasing lock at beginning of method.\n", px.me)
	return highestSequenceSoFar
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	//log.Printf("Paxos.Min(%v): About to acquire lock at beginning of method.\n", px.me)
	px.mu.Lock()
	//px.DeleteOldInstances()
	doneVal := px.doneVal
	px.mu.Unlock()
	//log.Printf("Paxos.Min(%v): Releasing lock from beginning of method.\n", px.me)
	if doneVal == -1 {
		return 0
	}

	return doneVal + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	fate := Fate(Pending)
	var v_a interface{}

	//log.Printf("Paxos.Status(%v): (seq: %v) About to acquire lock at beginning of method.\n", px.me, seq)
	px.mu.Lock()
	agreementInstance, ok := px.agreementInstances[seq]
	if ok == true {
		status := agreementInstance.Status
		if status == Pending {
			fate = Pending
		}

		if status == Decided {
			fate = Decided
			v_a = agreementInstance.V_a
		}
	}

	if ok == false {
		// If the key is not found, then we have marked it as Forgotten and deleted the associated data
		// So set fate to Forgotten
		fate = Forgotten
	}

	//px.DeleteOldInstances()
	px.mu.Unlock()
	//log.Printf("Paxos.Status(%v): (seq: %v) Releasing lock at beginning of method.\n", px.me, seq)
	return fate, v_a
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	px.agreementInstances = make(map[int]*AgreementInstance)
	px.peerDoneVals = make(map[string]int)
	px.doneVal = -1

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}

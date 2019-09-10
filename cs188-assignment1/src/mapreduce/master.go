package mapreduce

import "container/list"
import "fmt"
import "sort"

type WorkerInfo struct {
	address string
}

type JobResult struct{
	num int
	success bool
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	workerChannel := make(chan string)
	jobResults := make(chan JobResult)
	go registerWorkers(mr, workerChannel)

	jobNum := 0
	for jobNum < mr.nMap {
		currWorker := <- workerChannel
		go assignJob(mr, currWorker, jobNum, workerChannel, Map, mr.nReduce, jobResults)
		jobNum += 1
	}
	verifyCompletion(jobResults, mr.nMap-1, mr, workerChannel, Map, mr.nReduce)
	jobNum = 0
	for jobNum < mr.nReduce {
		currWorker := <- workerChannel
		go assignJob(mr, currWorker, jobNum, workerChannel, Reduce, mr.nMap, jobResults)
		jobNum += 1
	}
	verifyCompletion(jobResults, mr.nReduce-1, mr, workerChannel, Reduce, mr.nMap)
	return mr.KillWorkers()
}

func registerWorkers(mr *MapReduce, workerChannel chan string){
	for {
		workerAddress := <- mr.registerChannel
		workerChannel <- workerAddress
	}
}

func assignJob(mr *MapReduce, workerAddress string, jobNum int, workerChannel chan string, jobType JobType, numOtherPhase int, jobResults chan JobResult){
	var reply DoJobReply
	ok := call(workerAddress, "Worker.DoJob", DoJobArgs{mr.file, jobType, jobNum, numOtherPhase}, &reply)
	if ok == true {
		go func(){jobResults <- JobResult{jobNum, true}}()
		//mr.Workers[workerAddress] = &WorkerInfo{workerAddress}
		workerChannel <- workerAddress
	} else {
		jobResults <- JobResult{jobNum, false}
	}
}

func verifyCompletion(jobResults chan JobResult, numJobs int, mr *MapReduce, workerChannel chan string, jobType JobType, numOtherPhase int){
	jobNum := 0
	outOfOrderDoneJobs := make([]int, 0)
	for jobNum < numJobs {
		nextJob := <- jobResults
		if(nextJob.success == false){
			currWorker := <- workerChannel
			go assignJob(mr, currWorker, nextJob.num, workerChannel, jobType, numOtherPhase, jobResults)
			continue
		}
		if nextJob.num == jobNum {
			jobNum = jobNum + 1
		} else {
			outOfOrderDoneJobs = append(outOfOrderDoneJobs, nextJob.num)
			sort.Ints(outOfOrderDoneJobs)
		}
		for len(outOfOrderDoneJobs) > 0 && jobNum == outOfOrderDoneJobs[0] {
			jobNum = jobNum + 1
			outOfOrderDoneJobs = outOfOrderDoneJobs[1:]
		}
	}
}

package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	files []string

	reduceNum int

	mapStep int

	mapFinishCount int

	reduceStep int

	reduceFinishCount int

	mutex sync.Mutex

	mapTasks []string
}

func uuidGen() string {
	out, err := exec.Command("/usr/bin/uuidgen").Output()
	if err != nil {
		fmt.Printf("gen uuid error!")
	}
	return strings.TrimSpace(string(out))
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) JobRequest(args *JobRequestArgs, reply *JobRequestReply) error {
	if args.TaskType == MapTask {
		c.mutex.Lock()
		if len(args.LastTaskUUID) > 0 && args.LastTaskStatus == TaskSuccess {
			c.mapFinishCount = c.mapFinishCount + 1
			if c.mapFinishCount == len(c.files) {
				fmt.Printf("All [%v] Map Task Finished!\n", len(c.mapTasks))
			}
		}
		if c.mapFinishCount == len(c.files) {
			reply.TaskType = FinishTask
		} else {
			if c.mapStep < len(c.files) {
				reply.FileNames = []string{c.files[c.mapStep]}
				reply.TaskType = MapTask
				reply.ReduceNum = c.reduceNum
				reply.TaskUUID = uuidGen()
				c.mapTasks = append(c.mapTasks, reply.TaskUUID)
				c.mapStep = c.mapStep + 1
			} else {
				reply.TaskType = WaitTask
			}
		}
		c.mutex.Unlock()
	} else if args.TaskType == ReduceTask {
		c.mutex.Lock()
		if len(args.LastTaskUUID) > 0 && args.LastTaskStatus == TaskSuccess {
			c.reduceFinishCount = c.reduceFinishCount + 1
			if c.reduceFinishCount == c.reduceNum {
				fmt.Printf("All [%v] Reduce Task Finished!\n", c.reduceFinishCount)
			}
		}
		if c.reduceFinishCount == c.reduceNum {
			reply.TaskType = FinishTask
		} else {
			if c.reduceStep < c.reduceNum {
				reply.FileNames = []string{}
				for _, taskUUID := range c.mapTasks {
					outFileName := fmt.Sprintf("%s_%v.json", taskUUID, c.reduceStep)
					reply.FileNames = append(reply.FileNames, outFileName)
				}
				reply.ReduceNum = c.reduceNum
				reply.TaskType = ReduceTask
				reply.TaskUUID = uuidGen()
				c.reduceStep = c.reduceStep + 1
			} else {
				reply.TaskType = WaitTask
			}
		}
		c.mutex.Unlock()
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	if c.reduceFinishCount == c.reduceNum {
		ret = true
	}
	c.mutex.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.reduceNum = nReduce
	c.mapStep = 0
	c.reduceStep = 0
	c.mapTasks = []string{}
	c.reduceFinishCount = 0

	c.server()
	return &c
}

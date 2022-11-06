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
	"time"
)

type TaskRunInfo struct {
	StartTime int64
	EndTime   int64
	TaskUUID  string
	TaskNo    int
}

type MapTaskRunInfo struct {
	TaskRunInfo
	File string
}

type ReduceTaskRunInfo struct {
	TaskRunInfo
	Partition int
}

type Coordinator struct {
	// Your definitions here.
	mapTaskInfos []MapTaskRunInfo

	reduceTaskInfos []ReduceTaskRunInfo

	mutex sync.Mutex
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
		if args.LastTaskNo >= 0 && args.LastTaskUUID == c.mapTaskInfos[args.LastTaskNo].TaskUUID {
			c.mapTaskInfos[args.LastTaskNo].EndTime = time.Now().Unix()
		}
		finishTaskNum := 0
		for _, task := range c.mapTaskInfos {
			// fmt.Printf("task :%v, start: %v, end: %v\n", task.TaskNo, task.StartTime, task.EndTime)
			if task.StartTime > 0 && task.EndTime >= task.StartTime {
				finishTaskNum = finishTaskNum + 1
			}
			// task not started or task timeout
			timeNow := time.Now().Unix()
			if task.StartTime < 0 || (task.EndTime < 0 && (timeNow-task.StartTime) > 5) {
				// fmt.Printf("task :%v, start %v, end: %v, now: %v\n", task.TaskNo, task.StartTime, task.EndTime, timeNow)
				reply.FileNames = []string{task.File}
				reply.TaskType = MapTask
				reply.ReduceNum = len(c.reduceTaskInfos)
				reply.TaskNo = task.TaskNo
				reply.TaskUUID = uuidGen()
				c.mapTaskInfos[task.TaskNo].TaskUUID = reply.TaskUUID
				c.mapTaskInfos[task.TaskNo].StartTime = timeNow
				break
			}
		}
		if len(reply.TaskUUID) == 0 {
			if finishTaskNum == len(c.mapTaskInfos) {
				reply.TaskType = FinishTask
			} else {
				reply.TaskType = WaitTask
			}
		}
		c.mutex.Unlock()
	} else if args.TaskType == ReduceTask {
		c.mutex.Lock()
		if args.LastTaskNo >= 0 && args.LastTaskUUID == c.reduceTaskInfos[args.LastTaskNo].TaskUUID {
			c.reduceTaskInfos[args.LastTaskNo].EndTime = time.Now().Unix()
		}
		finishTaskNum := 0
		for _, task := range c.reduceTaskInfos {
			if task.StartTime > 0 && task.EndTime >= task.StartTime {
				finishTaskNum = finishTaskNum + 1
			}
			// task not started or task timeout
			timeNow := time.Now().Unix()
			if task.StartTime < 0 || (task.EndTime < 0 && (timeNow-task.StartTime) > 5) {
				reply.FileNames = []string{}
				for _, mapTask := range c.mapTaskInfos {
					outFileName := fmt.Sprintf("%s_%v.json", mapTask.TaskUUID, task.TaskNo)
					reply.FileNames = append(reply.FileNames, outFileName)
				}
				reply.TaskType = ReduceTask
				reply.ReduceNum = len(c.reduceTaskInfos)
				reply.TaskNo = task.TaskNo
				reply.TaskUUID = uuidGen()
				c.reduceTaskInfos[task.TaskNo].TaskUUID = reply.TaskUUID
				c.reduceTaskInfos[task.TaskNo].StartTime = timeNow
				break
			}
		}
		if len(reply.TaskUUID) == 0 {
			if finishTaskNum == len(c.reduceTaskInfos) {
				reply.TaskType = FinishTask
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
	ret := true

	// Your code here.
	c.mutex.Lock()
	for _, task := range c.reduceTaskInfos {
		if task.EndTime < 0 {
			ret = false
		}
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
	// c.files = files
	c.mapTaskInfos = []MapTaskRunInfo{}
	for i := 0; i < len(files); i++ {
		task := MapTaskRunInfo{}
		task.StartTime = -1
		task.EndTime = -1
		task.TaskNo = i
		task.File = files[i]
		c.mapTaskInfos = append(c.mapTaskInfos, task)
	}

	c.reduceTaskInfos = []ReduceTaskRunInfo{}
	for i := 0; i < nReduce; i++ {
		task := ReduceTaskRunInfo{}
		task.StartTime = -1
		task.EndTime = -1
		task.Partition = i
		task.TaskNo = i
		c.reduceTaskInfos = append(c.reduceTaskInfos, task)
	}
	c.server()
	return &c
}

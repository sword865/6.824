package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
const (
	MapTask    int = iota
	ReduceTask int = iota
	WaitTask   int = iota
	FinishTask int = iota
)

const (
	TaskSuccess int = iota
	TaskFailed  int = iota
	TaskUnKnow  int = iota
)

type JobRequestArgs struct {
	TaskType       int
	LastTaskUUID   string
	LastTaskStatus int
}

type JobRequestReply struct {
	FileNames []string
	ReduceNum int
	TaskType  int
	TaskUUID  string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

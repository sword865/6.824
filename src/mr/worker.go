package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func hashToPartition(s string, value int) uint32 {
	return uint32(ihash(s) % value)
}

func runMapStage(mapf func(string, string) []KeyValue) {
	lastTaskUUID := ""
	lastJobStatus := TaskUnKnow
	for {
		task := RequestJob(MapTask, lastTaskUUID, lastJobStatus)
		if task.TaskType == MapTask {
			result := map[uint32][]KeyValue{}
			for _, fileName := range task.FileNames {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", fileName)
				}
				file.Close()
				kva := mapf(fileName, string(content))
				for _, kv := range kva {
					idx := hashToPartition(kv.Key, task.ReduceNum)
					result[idx] = append(result[idx], kv)
				}
			}
			//dump result to out_put
			for idx, kva := range result {
				outFileName := fmt.Sprintf("%s_%v.json", task.TaskUUID, idx)
				file, _ := os.Create(outFileName)
				enc := json.NewEncoder(file)
				err := enc.Encode(&kva)
				if err != nil {
					fmt.Printf("output failed!")
				}
				file.Close()
			}
			lastTaskUUID = task.TaskUUID
			lastJobStatus = TaskSuccess
			// fmt.Printf("Map Task %s finished\n", task.TaskUUID)
		} else if task.TaskType == WaitTask {
			lastTaskUUID = ""
			lastJobStatus = TaskSuccess
			time.Sleep(time.Second)
		} else if task.TaskType == FinishTask {
			break
		} else {
			fmt.Printf("Bad Task: can not process task %v in map stage!\n", task.TaskType)
		}
	}
}

func runReduceStage(reducef func(string, []string) string) {
	lastTaskUUID := ""
	lastJobStatus := TaskUnKnow
	for {
		task := RequestJob(ReduceTask, lastTaskUUID, lastJobStatus)
		if task.TaskType == ReduceTask {
			if len(task.FileNames) == 0 {
				fmt.Printf("empty reduce job: %s\n", task.TaskUUID)
				continue
			}
			intermediate := []KeyValue{}
			for _, fileName := range task.FileNames {
				file, err := os.Open(fileName)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					log.Fatalf("cannot open %v", fileName)
				}
				if err == nil {
					var kva []KeyValue
					dec := json.NewDecoder(file)
					if err := dec.Decode(&kva); err != nil {
						break
					}
					intermediate = append(intermediate, kva...)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))

			reduceIdx := strings.Split(strings.Split(task.FileNames[0], ".")[0], "_")[1]

			outFileName := fmt.Sprintf("mr-out-%s.txt", reduceIdx)
			outFile, _ := os.Create(outFileName)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}

				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			lastTaskUUID = task.TaskUUID
			lastJobStatus = TaskSuccess
			// fmt.Printf("Reduce Task %s finished\n", task.TaskUUID)
		} else if task.TaskType == WaitTask {
			lastTaskUUID = ""
			lastJobStatus = TaskSuccess
		} else if task.TaskType == FinishTask {
			break
		} else {
			fmt.Printf("Bad Task: can not process task %v in map stage!\n", task.TaskType)
		}
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	fmt.Printf("Worker running Map Stage\n")
	runMapStage(mapf)
	fmt.Printf("Worker finish Map Stage\n")

	fmt.Printf("Worker running Reduce Stage\n")
	runReduceStage(reducef)
	fmt.Printf("Worker finish Reduce Stage\n")

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestJob(taskType int, lastTaskUUID string, lastJobStatus int) JobRequestReply {

	// declare an argument structure.
	args := JobRequestArgs{TaskType: taskType, LastTaskUUID: lastTaskUUID, LastTaskStatus: lastJobStatus}

	// fill in the argument(s).

	// declare a reply structure.
	reply := JobRequestReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.JobRequest", &args, &reply)
	if ok {
		if reply.TaskType == MapTask {
			fmt.Printf("reply.file %s for Map\n", strings.Join(reply.FileNames, ", "))
		} else if reply.TaskType == ReduceTask {
			reduceIdx := "-1"
			if len(reply.FileNames) > 0 {
				reduceIdx = strings.Split(strings.Split(reply.FileNames[0], ".")[0], "_")[1]
			}
			fmt.Printf("process reduce files %s\n", reduceIdx)
		}
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

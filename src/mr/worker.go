package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		hasTask := callHasMoreTask()
		if !hasTask {
			log.Printf("No more task, worker exit\n")
			return
		}

		task := callGetTask()
		if task == nil {
			return
		}
		// log.Printf("Get task %v", task)

		switch task.TaskType {
		case MapTask:
			doMapTask(task, mapf)
		case ReduceTask:
			doReduceTask(task, reducef)
		}

		callTaskDone(task)
		//time.Sleep(1 * time.Second)
	}
}

// doMapTask 用于执行Map任务
func doMapTask(task *TaskInfo, mapf func(string, string) []KeyValue) {
	intermediateFile, err := os.Create(task.IntermediateFilePath)
	if err != nil {
		log.Fatalf("cannot create %v", task.IntermediateFilePath)
	}
	defer intermediateFile.Close()

	file, err := os.OpenFile(task.MapFilePath, os.O_RDONLY, 0)
	if err != nil {
		log.Fatalf("cannot open %v", task.MapFilePath)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.MapFilePath)
	}

	kva := mapf(task.MapFilePath, string(content))
	enc := json.NewEncoder(intermediateFile)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}
}

// doReduceTask 用于执行Reduce任务
func doReduceTask(task *TaskInfo, reducef func(string, []string) string) {
	finalTaskPath := "mr-out-" + strconv.Itoa(task.TaskId)
	finalTaskFile, err := os.Create(finalTaskPath)
	if err != nil {
		log.Fatalf("cannot create %v", finalTaskPath)
	}
	defer finalTaskFile.Close()

	intermediate := make(map[string][]string)

	for i := 0; i < task.MapTaskCount; i++ {
		intermediateFilePath := "mr-tmp-" + strconv.Itoa(i)
		file, err := os.OpenFile(intermediateFilePath, os.O_RDONLY, 0)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFilePath)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			keyHash := ihash(kv.Key) % task.MapTaskCount
			if keyHash != task.TaskId {
				continue // 这个任务不需要处理
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}
	}

	// 输出结果
	for key, values := range intermediate {
		output := reducef(key, values)
		fmt.Fprintf(finalTaskFile, "%v %v\n", key, output)
	}
}

// callGetTask 用于向coordinator请求任务
func callGetTask() *TaskInfo {
	args := GetTaskArgs{}
	reply := TaskInfo{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply
	} else {
		log.Printf("call failed!\n")
	}

	return nil
}

// callTaskDone 用于向coordinator汇报任务完成
func callTaskDone(task *TaskInfo) {
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &task, &reply)
	if !ok {
		log.Printf("call failed!\n")
	}
}

// callHasMoreTask 用于向coordinator询问是否还有任务
func callHasMoreTask() bool {
	//log.Printf("Ask for more task\n")
	args := GetTaskArgs{}
	reply := GetTask_Wait
	ok := call("Coordinator.HasMoreTask", &args, &reply)
	if ok {
		if reply == GetTask_Wait {
			time.Sleep(1 * time.Second)
			return callHasMoreTask()
		}
		return reply == GetTask_HasTask
	} else {
		log.Printf("call failed!\n")
	}

	return false
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

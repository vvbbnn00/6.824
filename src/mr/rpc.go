package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type GetTaskArgs struct{}

type GetTaskReply int

const (
	GetTask_HasTask = iota
	GetTask_Wait
	GetTask_Finished
)

type HeartBeatArgs struct {
	Task TaskInfo
}

type HeartBeatReply struct{}
type TaskDoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

import (
	"log"
	"strconv"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int
type TaskType int
type ThreadMessageType int
type CoordinatorPhase int

const (
	TaskTimeOut = 10 * time.Second // 任务超时时间
)

const (
	MapPhase CoordinatorPhase = iota
	ReducePhase
	Finished
)

const (
	Unstarted TaskStatus = iota
	InProgress
	Completed
)

const (
	MapTask TaskType = iota
	ReduceTask
)

type TaskInfo struct {
	TaskId               int
	TaskType             TaskType
	IntermediateFilePath string // 任务的中间文件路径
	MapFilePath          string // Map任务需要处理的文件路径
	Status               TaskStatus
	StartTime            time.Time
	MapTaskCount         int // Map任务的数量
}

const (
	WorkerGetTask ThreadMessageType = iota
	WorkerHeartBeat
	WorkerTaskDone
	WorkerTaskTimeOut
)

// ThreadMessage 用于线程之间传递消息
type ThreadMessage struct {
	MsgType ThreadMessageType
	Task    TaskInfo
}

type Coordinator struct {
	// Map任务相关
	MapTasks []TaskInfo // Map任务的状态信息

	// Reduce任务相关
	ReduceTasks []TaskInfo // Reduce任务的状态信息
	NReduce     int        // Reduce任务的总数

	// 通过Channel来进行线程之间的通信
	MessageChan chan ThreadMessage

	CoordinatorPhase CoordinatorPhase // 当前协调器的阶段
}

// chooseMapTask 选择一个Map任务
func (c *Coordinator) chooseMapTask() *TaskInfo {
	for i := 0; i < len(c.MapTasks); i++ {
		if c.MapTasks[i].Status == Unstarted {
			return &c.MapTasks[i]
		}
	}
	return nil
}

// chooseReduceTask 选择一个Reduce任务
func (c *Coordinator) chooseReduceTask() *TaskInfo {
	for i := 0; i < len(c.ReduceTasks); i++ {
		if c.ReduceTasks[i].Status == Unstarted {
			return &c.ReduceTasks[i]
		}
	}
	return nil
}

// checkTaskTimeout 发起一个定时函数，检查任务是否超时
func (c *Coordinator) checkTaskTimeout(info *TaskInfo) {
	time.Sleep(TaskTimeOut)
	// 向 MessageChan 发送消息
	c.MessageChan <- ThreadMessage{MsgType: WorkerTaskTimeOut, Task: *info}
}

// chooseTask 选择一个任务
func (c *Coordinator) chooseTask() *TaskInfo {
	task := &TaskInfo{}
	if c.CoordinatorPhase == MapPhase {
		task = c.chooseMapTask()
	} else {
		task = c.chooseReduceTask()
	}

	if task != nil {
		task.StartTime = time.Now()
		task.Status = InProgress
		go c.checkTaskTimeout(task)
	}

	return task
}

// updatePhase 更新阶段，判断是否需要切换阶段
func (c *Coordinator) updatePhase() {
	// 如果当前阶段是Map阶段，判断是否所有的Map任务都已经完成
	if c.CoordinatorPhase == MapPhase {
		for i := 0; i < len(c.MapTasks); i++ {
			if c.MapTasks[i].Status != Completed {
				return
			}
		}
		c.CoordinatorPhase = ReducePhase
	}

	// 如果当前阶段是Reduce阶段，判断是否所有的Reduce任务都已经完成
	if c.CoordinatorPhase == ReducePhase {
		for i := 0; i < len(c.ReduceTasks); i++ {
			if c.ReduceTasks[i].Status != Completed {
				return
			}
		}
		c.CoordinatorPhase = Finished
	}
}

// GetTask 暴露给Worker的RPC接口
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *TaskInfo) error {
	// 向 MessageChan 发送消息
	c.MessageChan <- ThreadMessage{MsgType: WorkerGetTask}
	msg := <-c.MessageChan
	*reply = msg.Task
	return nil
}

// HeartBeat 暴露给Worker的RPC接口
func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	// 向 MessageChan 发送消息
	c.MessageChan <- ThreadMessage{MsgType: WorkerHeartBeat, Task: args.Task}
	return nil
}

// TaskDone 暴露给Worker的RPC接口
func (c *Coordinator) TaskDone(args *TaskInfo, reply *TaskDoneReply) error {
	// 向 MessageChan 发送消息
	c.MessageChan <- ThreadMessage{MsgType: WorkerTaskDone, Task: *args}
	*reply = TaskDoneReply{}
	return nil
}

// HasMoreTask 暴露给Worker的RPC接口
func (c *Coordinator) HasMoreTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.Done() {
		*reply = GetTask_Finished
		return nil
	}

	var task *TaskInfo

	switch c.CoordinatorPhase {
	case MapPhase:
		task = c.chooseMapTask()
	case ReducePhase:
		task = c.chooseReduceTask()
	default:
		task = nil
	}
	if task != nil {
		*reply = GetTask_HasTask
	} else {
		*reply = GetTask_Wait
	}
	return nil
}

// schedule 调度任务
func (c *Coordinator) schedule() {
	for {
		select {
		case msg := <-c.MessageChan:
			switch msg.MsgType {
			case WorkerGetTask: // 处理任务分配
				task := c.chooseTask()
				if task != nil {
					// log.Printf("[GetTask] Assign Task %d to Worker", task.TaskId)
					msg.Task = *task
					c.MessageChan <- msg
				}
			case WorkerTaskDone: // 处理任务完成
				// log.Printf("[TaskDone] Task %d is done", msg.Task.TaskId)
				taskId := msg.Task.TaskId
				if msg.Task.TaskType == MapTask {
					c.MapTasks[taskId].Status = Completed
				} else {
					c.ReduceTasks[taskId].Status = Completed
				}
				c.updatePhase()
			case WorkerTaskTimeOut: // 检查任务是否超时
				task := msg.Task
				taskId := msg.Task.TaskId
				if task.Status == Completed {
					break // 如果任务已经完成，则不需要处理
				}
				// 如果任务没有完成，则认为任务失败
				log.Printf("[TaskTimeOut] Task %d is time out", task.TaskId)
				// 若任务失败，则将任务状态置为Unstarted
				if msg.Task.TaskType == MapTask {
					c.MapTasks[taskId].Status = Unstarted
				} else {
					c.ReduceTasks[taskId].Status = Unstarted
				}
			default:
				log.Printf("Unknown message type")
			}
		}
	}
}

// start a thread that listens for RPCs from worker.go
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.CoordinatorPhase == Finished
}

// MakeCoordinator is to create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.NReduce = nReduce

	// 此处认为任务已经完成了分片，所以可以直接来分配任务
	for _, file := range files {
		task := TaskInfo{
			MapFilePath:          file,
			TaskType:             MapTask,
			Status:               Unstarted,
			TaskId:               len(c.MapTasks),
			IntermediateFilePath: "mr-tmp-" + strconv.Itoa(len(c.MapTasks)),
		}
		c.MapTasks = append(c.MapTasks, task)
	}

	// Reduce任务的初始化
	for i := 0; i < nReduce; i++ {
		task := TaskInfo{
			TaskType:             ReduceTask,
			Status:               Unstarted,
			TaskId:               i,
			IntermediateFilePath: "mr-tmp-" + strconv.Itoa(i),
			MapTaskCount:         len(c.MapTasks),
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}

	c.MessageChan = make(chan ThreadMessage)
	c.server()
	go c.schedule()

	return &c
}

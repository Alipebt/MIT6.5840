package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	files             map[string]FileStatus
	intermediateFiles map[string]FileStatus
	tasks             []Task

	workerID uint32
	nReduce  int

	phase Phase

	mu sync.Mutex
}
type Task struct {
	TaskType    TaskType
	Id          uint32
	Status      TaskStatus
	ProcessFile []string
	Result      []string

	timer time.Time
}

type TaskType int

const (
	UnknowTaskType TaskType = iota
	MapTask
	ReduceTask
)

type FileStatus int

const (
	UnknownFile FileStatus = iota
	FileUnassigned
	FileAssigned
	FileCompleted
)

type TaskStatus int

const (
	UnknowTaskStatus TaskStatus = iota
	TaskReady
	TaskWorking
	TaskCompleted
	TaskFailed
)

type Phase int

const (
	UnknownPhase  Phase = iota
	PhaseMap            // Map阶段
	PhaseReduce         // Reduce阶段
	PhaseComplete       // 完成阶段
)

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RPCHandler(args *Args, reply *Reply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	c.HeartBeat()

	task := args.Task
	switch task.TaskType {
	case UnknowTaskType:
		reprocess := false
		for _, item := range c.tasks {
			if item.Status == TaskFailed {
				reprocess = true
				reply.Task = Task{
					TaskType:    item.TaskType,
					Id:          item.Id,
					ProcessFile: item.ProcessFile,
					Status:      TaskWorking,
					timer:       time.Now(),
				}
				reply.NReduce = c.nReduce
				if item.TaskType == MapTask {
					for _, filename := range item.ProcessFile {
						c.files[filename] = FileAssigned
					}
				} else if item.TaskType == ReduceTask {
					for _, filename := range item.ProcessFile {
						c.intermediateFiles[filename] = FileAssigned
					}
				}
			}
		}
		if reprocess == false {
			if c.phase == PhaseMap {
				var assigned []string
				for filename, status := range c.files {
					if status == FileUnassigned {
						assigned = append(assigned, filename)
						c.files[filename] = FileAssigned
						break
					}
				}
				reply.Task = Task{
					TaskType:    MapTask,
					Id:          c.workerID,
					ProcessFile: assigned,
					Status:      TaskWorking,
					timer:       time.Now(),
				}
				reply.NReduce = c.nReduce
				if assigned == nil {
					reply.Task.TaskType = UnknowTaskType
					reply.Task.Status = UnknowTaskStatus
				} else {
					c.workerID++
					c.tasks = append(c.tasks, reply.Task)
				}
			} else if c.phase == PhaseReduce {
				var assigned []string
				reducerID := -1
				for filename, status := range c.intermediateFiles {
					tail, _ := strconv.Atoi(filename[len(filename)-1:])
					if status == FileUnassigned && reducerID == -1 {
						assigned = append(assigned, filename)
						c.intermediateFiles[filename] = FileAssigned
						reducerID = tail
					} else if status == FileUnassigned && tail == reducerID {
						assigned = append(assigned, filename)
						c.intermediateFiles[filename] = FileAssigned
					}
				}

				reply.Task = Task{
					TaskType:    ReduceTask,
					Id:          c.workerID,
					ProcessFile: assigned,
					Status:      TaskWorking,
					timer:       time.Now(),
				}
				if assigned == nil {
					reply.Task.TaskType = UnknowTaskType
					reply.Task.Status = UnknowTaskStatus
				} else {
					c.workerID++
					c.tasks = append(c.tasks, reply.Task)
				}
			}
		}
	case MapTask:
		if task.Status == TaskCompleted && c.phase == PhaseMap {
			c.UpdateTask(task)
			for _, item := range task.ProcessFile {
				c.files[item] = FileCompleted
			}
			for _, item := range task.Result {
				c.intermediateFiles[item] = FileUnassigned
			}
		}
		c.phase = c.GetPhase()
	case ReduceTask:
		if task.Status == TaskCompleted && c.phase == PhaseReduce {
			c.UpdateTask(task)
			for _, item := range task.ProcessFile {
				c.intermediateFiles[item] = FileCompleted
			}
		}
		c.phase = c.GetPhase()
	}

	return nil
}

func (c *Coordinator) UpdateTask(task Task) {
	for i, item := range c.tasks {
		if task.TaskType == item.TaskType &&
			task.Id == item.Id {
			c.tasks[i] = task
		}
	}
}

func (c *Coordinator) GetPhase() Phase {
	phase := PhaseComplete
	for _, status := range c.intermediateFiles {
		if status != FileCompleted {
			phase = PhaseReduce
		}
	}
	for _, status := range c.files {
		if status != FileCompleted {
			phase = PhaseMap
		}
	}
	return phase
}

// HeartBeat keep the task alive
func (c *Coordinator) HeartBeat() {
	now := time.Now()
	for i, item := range c.tasks {
		delay := item.timer.Add(10 * time.Second)
		if now.Before(delay) || item.Status == TaskCompleted {
			continue
		}
		c.tasks[i].Status = TaskFailed
		if item.TaskType == MapTask {
			for _, filename := range item.ProcessFile {
				if c.files[filename] != FileCompleted {
					c.files[filename] = FileUnassigned
				}
			}
		} else if item.TaskType == ReduceTask {
			for _, filename := range item.ProcessFile {
				if c.intermediateFiles[filename] != FileCompleted {
					c.intermediateFiles[filename] = FileUnassigned
				}
			}
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.phase == PhaseComplete {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = make(map[string]FileStatus)
	for _, filename := range files {
		c.files[filename] = FileUnassigned
	}
	c.intermediateFiles = make(map[string]FileStatus)
	c.workerID = 0
	c.nReduce = nReduce
	c.phase = PhaseMap

	c.server()
	return &c
}

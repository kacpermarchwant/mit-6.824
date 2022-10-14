package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Coordinator struct {
	// Your definitions here.
	nReduce int

	mapTasks               map[string]TaskState
	mapTasksCount          int
	completedMapTasksCount int
	nextMapTaskId          int

	intermediateFiles         [][]string
	completedPartitionsOfFile map[string]int

	reduceTasks               map[int]TaskState
	reduceTasksCount          int
	completedReduceTasksCount int

	mutex sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *EmptyArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.mapTasksLeft() {
		for file, state := range c.mapTasks {
			if state == Idle {
				reply.TaskType = MapTask
				reply.File = file
				reply.MapTaskId = c.getNextTaskMapId()
				reply.NReduce = c.nReduce

				c.mapTasks[file] = InProgress
				c.scheduleMapWorkerTimeout(file)

				return nil
			}
		}
	} else if c.reduceTasksLeft() {
		for idx, state := range c.reduceTasks {
			if state == Idle {
				reply.TaskType = ReduceTask
				reply.PartitionIdx = idx
				reply.IntermediateFiles = c.intermediateFiles[idx]

				c.reduceTasks[idx] = InProgress
				c.scheduleReduceWorkerTimeout(idx)

				return nil
			}
		}
	}

	reply.TaskType = None
	return nil
}

func (c *Coordinator) mapTasksLeft() bool {
	if c.mapTasksCount > c.completedMapTasksCount {
		return true
	} else {
		return false
	}
}

func (c *Coordinator) reduceTasksLeft() bool {
	if c.reduceTasksCount > c.completedReduceTasksCount {
		return true
	} else {
		return false
	}
}

func (c *Coordinator) getNextTaskMapId() int {
	nextId := c.nextMapTaskId
	c.nextMapTaskId++

	return nextId
}

func (c *Coordinator) scheduleMapWorkerTimeout(file string) {
	go c.timeoutMapWorker(file)
}

func (c *Coordinator) scheduleReduceWorkerTimeout(idx int) {
	go c.timeoutReduceWorker(idx)
}

func (c *Coordinator) timeoutMapWorker(file string) {
	timer := time.NewTimer(10 * time.Second)

	for {
		select {
		case <-timer.C:
			c.mutex.Lock()
			defer c.mutex.Unlock()

			if c.mapTasks[file] != Completed {
				c.mapTasks[file] = Idle
				return
			} else {
				return
			}
		}
	}
}

func (c *Coordinator) timeoutReduceWorker(idx int) {
	timer := time.NewTimer(10 * time.Second)

	for {
		select {
		case <-timer.C:
			c.mutex.Lock()
			defer c.mutex.Unlock()

			if c.reduceTasks[idx] != Completed {
				c.reduceTasks[idx] = Idle
				return
			} else {
				return
			}
		}
	}
}

func (c *Coordinator) RegisterNewIntermediateFile(args *RegisterNewIntermediateFileArgs, reply *EmptyReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.intermediateFiles[args.PartitionIdx] = append(c.intermediateFiles[args.PartitionIdx], args.IntermediateFile)
	c.completedPartitionsOfFile[args.OriginalFile] += 1

	if c.completedPartitionsOfFile[args.OriginalFile] == c.nReduce {
		c.mapTasks[args.OriginalFile] = Completed
		c.completedMapTasksCount += 1
	}

	return nil
}

func (c *Coordinator) RegisterCompletedReduceTask(args *RegisterCompletedReduceTaskArgs, reply *EmptyReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.reduceTasks[args.PartitionIdx] = Completed
	c.completedReduceTasksCount += 1

	return nil
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
	c.mutex.RLock()
	defer c.mutex.RLocker().Unlock()

	return !c.reduceTasksLeft()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.mapTasks = make(map[string]TaskState, len(files))
	c.reduceTasks = make(map[int]TaskState, nReduce)
	c.intermediateFiles = make([][]string, nReduce)
	c.completedPartitionsOfFile = make(map[string]int)

	for _, file := range files {
		c.mapTasks[file] = Idle
		c.mapTasksCount += 1
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Idle
		c.reduceTasksCount += 1
	}

	c.server()
	return &c
}

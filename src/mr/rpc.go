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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	None
)

type EmptyArgs struct{}

type RegisterNewIntermediateFileArgs struct {
	IntermediateFile string
	OriginalFile     string
	PartitionIdx     int
}

type EmptyReply struct{}

type GetTaskReply struct {
	TaskType          TaskType
	File              string
	MapTaskId         int
	NReduce           int
	PartitionIdx      int
	IntermediateFiles []string
}

type RegisterCompletedReduceTaskArgs struct {
	PartitionIdx int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

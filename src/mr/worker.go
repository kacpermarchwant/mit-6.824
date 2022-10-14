package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

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

	// Your worker implementation here.
	for {
		args := EmptyArgs{}
		reply := GetTaskReply{}

		call("Coordinator.GetTask", &args, &reply)

		switch reply.TaskType {
		case MapTask:
			applyMapFunction(reply.File, reply.MapTaskId, reply.NReduce, mapf)
		case ReduceTask:
			applyReduceFunction(reply.IntermediateFiles, reply.PartitionIdx, reducef)
		case None:
			continue
		}
	}
}

// mapf => k1 -> v1 -> list(k2, v2)
func applyMapFunction(file string, mapTaskId int, nReduce int, mapf func(string, string) []KeyValue) {
	openedFile, err := os.Open(file)

	if err != nil {
		log.Fatalf("applyMapFunction: cannot open %v", file)
	}

	defer openedFile.Close()

	fileContent, err := ioutil.ReadAll(openedFile)

	if err != nil {
		log.Fatalf("applyMapFunction: cannot read %v", file)
	}

	intermediateKeyValuePairs := mapf(file, string(fileContent))

	createIntermediateFiles(intermediateKeyValuePairs, file, mapTaskId, nReduce)
}

func createIntermediateFiles(intermediateKeyValuePairs []KeyValue, originalFile string, mapTaskId int, nReduce int) {
	bufferedPairs := make([][]KeyValue, nReduce)

	for _, kv := range intermediateKeyValuePairs {
		partition := ihash(kv.Key) % nReduce
		bufferedPairs[partition] = append(bufferedPairs[partition], kv)
	}

	for partition, kvList := range bufferedPairs {
		tempFilename := "temp-" + strconv.Itoa(mapTaskId) + "-" + strconv.Itoa(partition)
		intermediateFile, err := os.Create(tempFilename)

		if err != nil {
			log.Fatalf("createIntermediateFiles: cannot create %v", tempFilename)
		}

		defer intermediateFile.Close()

		encoder := json.NewEncoder(intermediateFile)

		for _, kv := range kvList {
			err := encoder.Encode(&kv)

			if err != nil {
				log.Fatal("createIntermediateFiles: cannot encode kv", err)
			}
		}

		intermedaiteFilename := "mr-" + strconv.Itoa(mapTaskId) + "-" + strconv.Itoa(partition)

		os.Rename(tempFilename, intermedaiteFilename)

		call("Coordinator.RegisterNewIntermediateFile", &RegisterNewIntermediateFileArgs{IntermediateFile: intermedaiteFilename, OriginalFile: originalFile, PartitionIdx: partition}, &EmptyReply{})
	}
}

func applyReduceFunction(intermediateFiles []string, partitionIdx int, reducef func(string, []string) string) {
	intermediateFileContent := []KeyValue{}

	for _, file := range intermediateFiles {

		openedFile, err := os.Open(file)

		if err != nil {
			log.Fatalf("applyReduceFunction: cannot open %v", file)
		}

		defer openedFile.Close()

		dec := json.NewDecoder(openedFile)

		for {
			var kv KeyValue

			err := dec.Decode(&kv)

			if err != nil {
				break
			}

			intermediateFileContent = append(intermediateFileContent, kv)
		}
	}

	intermediateFileContentGroupedByKey := make(map[string][]string)

	for _, kv := range intermediateFileContent {
		intermediateFileContentGroupedByKey[kv.Key] = append(intermediateFileContentGroupedByKey[kv.Key], kv.Value)
	}

	tempFilename := "temp-" + strconv.Itoa(partitionIdx)
	outputFile, err := os.Create(tempFilename)

	if err != nil {
		log.Fatalf("applyReduceFunction: cannot open %v", tempFilename)
	}

	for key, values := range intermediateFileContentGroupedByKey {
		output := reducef(key, values)

		fmt.Fprintf(outputFile, "%v %v\n", key, output)
	}

	outputFile.Close()

	outputFilename := "mr-out-" + strconv.Itoa(partitionIdx)
	os.Rename(tempFilename, outputFilename)

	call("Coordinator.RegisterCompletedReduceTask", &RegisterCompletedReduceTaskArgs{PartitionIdx: partitionIdx}, &EmptyReply{})
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

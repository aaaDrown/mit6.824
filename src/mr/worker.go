package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	fmt.Printf("do map task%v\n", response)
	var intermediate []KeyValue
	filename := response.Filename

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// 通过io工具包获取conten,作为mapf的参数
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// map返回一组KV结构体数组
	intermediate = mapf(filename, string(content))

	//initialize and loop over []KeyValue
	rn := response.ReducerNum
	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)
	for _, kv := range intermediate {
		HashedKV[(ihash(kv.Key))%rn] = append(HashedKV[(ihash(kv.Key))%rn], kv)
	}

	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}

}

// GetTask 获取任务（需要知道是Map任务，还是Reduce）
func GetTask() Task {

	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}

// callDone Call RPC to mark the task as completed
func callDone(args *Task) Task {
	
	reply := Task{}
	ok := call("Coordinator.MarkFinished", args, &reply)

	if ok {
		fmt.Printf("get reply %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	keepFlag := true
	for keepFlag {
		task := GetTask()
		fmt.Printf("get task %v\n", task)
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				callDone(&task)
			}

		case WaittingTask:
			{
				fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				fmt.Println("Task about :[", task.TaskId, "] is terminated...")
				keepFlag = false
			}

		}
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

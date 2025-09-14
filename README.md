## MIT 6.5840 Lab

---



README.md中给出的是部分代码，完整代码请参考并对照[这里](https://github.com/Alipebt/MIT6.5840)



---



### Lab1  MapReduce

`MapReaduce`的主要实现分为两部分，`worker`和`coordinator`。

`coordinator`是协调多个`worker`的协调器，只有一个实例。`MapReduce`处理过程分为两个阶段，`Map`阶段和`Reduce`阶段。`Reduce`任务只有在`Map`任务完全结束后才会被分配和执行，否则就会出现键值对重复、缺失等问题。

`Map`会将原始输入的文件名及其内容映射为一个键值对，即`<文件名，文件内容>`，然后通过`Map`函数使其转变为键值对集合，即多个`<key，value>`，然后将中间数据交给`Reduce`处理，获取所有键值对并排序，然后将相同键的值合并在一起，即`<key，[value list]>`，最后通过调用`Reduce`函数来获取处理后的`[Value list]`。

对于该Lab，其中间文件命名为`mr-(mapid)-(reduceid)`，结果文件命名为`mr-out-(reduceid)`。

一个`worker`就相当于一个工作节点，所以说`worker`是循环获取`coordinator`分配的任务的。

此时我们需要一个`Task`结构体来表示所分配的任务。

```go
// Coordinater 
type Task struct {
	TaskType     TaskType	//  任务类型
	Id         uint32	  // ID
	Status      TaskStatus	// 任务状态 
	ProcessFile   []string	  // 分配的任务
	Result      []string	 // 处理结果

	timer time.Time			 // 用于u记录任务时间（心跳）
}

type TaskType int

const (
	UnknowTaskType TaskType = iota
	MapTask			// Map任务
	ReduceTask		// Reduce任务
)

type TaskStatus int

const (
	UnknowTaskStatus TaskStatus = iota
	TaskReady		// 任务就绪
	TaskWorking		// 任务工作中
	TaskCompleted	// 任务完成
	TaskFailed		// 任务失败（超时）
)

```

以及使`RPC`能够传递`Task`：

```go
// rpc
type Args struct {
	Task Task
}

type Reply struct {
	NReduce int		// Reduce任务数量
	Task    Task
}

```



当一个`worker`向`coordinator`发送`RPC`请求以获取任务之前，可以先将任务类型标为`未知`，代表需要获取任务。

```go
// Worker
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		time.Sleep(100 * time.Millisecond)

		args := Args{}
		reply := Reply{}

		args.Task.TaskType = UnknowTaskType
		ok := call("Coordinator.RPCHandler", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
			return
		}
		if reply.Task.TaskType == MapTask {
			// Map
			DoMap(mapf, &args, &reply)
		} else if reply.Task.TaskType == ReduceTask {
			// Reduce
			DoReduce(reducef, &args, &reply)
		} else if reply.Task.TaskType == UnknowTaskType {
		}
	}
}
```

而`Coordinater`在收到获取请求后，根据系统现在所处的处理阶段为`worker`分配一个`Map`或`Reduce`任务。并且为了可以维护数据、任务以及各种状态，`Coordinater`的结构作以下定义：

```go
type Coordinator struct {
	files             map[string]FileStatus  // 原始数据及状态
	intermediateFiles     map[string]FileStatus	 // Map产生的中间数据及状态
	tasks             []Task				// 任务列表

	workerID uint32		// worker 的序号
	nReduce  int	  // Reduce任务数

	phase Phase			// 阶段，Map或Reduce

	mu sync.Mutex		// worker调用Coordinater的处理函数是并发的
}

type FileStatus int

const (
	UnknownFile FileStatus = iota
	FileUnassigned		// 文件未分配
	FileAssigned		// 文件已分配
	FileCompleted		// 文件处理完成
)

type Phase int

const (
	UnknownPhase  Phase = iota
	PhaseMap            // Map阶段
	PhaseReduce         // Reduce阶段
	PhaseComplete       // 完成阶段
)

```



我们先来看`Map`任务，它的原始输入为Lab中给出的文件，一个文件就是一块切分好的数据，一个`Map`只需分配一个文件即可。

```go
// 选取一个未被分配的任务（文件）
var assigned []string
for filename, status := range c.files {
	if status == FileUnassigned {
		assigned = append(assigned, filename)
		c.files[filename] = FileAssigned
		break
	}
}
// 数据写入响应结构体
reply.Task = Task{
	TaskType:    MapTask,
	Id:         c.workerID,
	ProcessFile:   assigned,
	Status:      TaskWorking,
	timer:       time.Now(),
}
reply.NReduce = c.nReduce
if assigned == nil {
    // 未分配需处理的文件则退回任务
	reply.Task.TaskType = UnknowTaskType
	eply.Task.Status = UnknowTaskStatus
} else {
    // 分配后将任务信息添加到任务列表里，并将工作序号加一
	c.workerID++
	c.tasks = append(c.tasks, reply.Task)
}
```

接下来`worker`收到了`Map`的任务分配，并执行。首先是获取所分配文件的内容，直接将其通过`map`函数转换为键值对集合，由于一个`Map`任务会生成多个文件（数据块），其命名为`mr-%v-%v`，第一个`%v`是当前`workerID`，第二个`%v`是按照变量`nReduce`，将`key`映射到相同`Reduce`的`reduceID`。

任务结束后，`worker`将任务完成信息传输给`Coordinater`。

```go
// func DoMap(mapf func(string, string) []KeyValue, args *Args, reply *Reply) {}

// 读取数据，并记录所有处理后的结果
	for _, filename := range task.ProcessFile {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

       // kva 是键值对的集合
		kva := mapf(filename, string(content))
       // 中间数据，记录了所有文件中获取到的键值对
		intermediate = append(intermediate, kva...)
	}

// 生成中间文件名称及内容的映射关系
	for i := 0; i < len(intermediate); i++ {

		kv := KeyValue{}
		kv.Key = intermediate[i].Key
		kv.Value = intermediate[i].Value

       // 保证多个相同的key能够被正确的映射在同一个reduce任务里
		reducerID := ihash(kv.Key) % reply.NReduce
		name := fmt.Sprintf("mr-%v-%v", reply.Task.Id, reducerID)
       // <文件名，键值对集合>
		file2result[name] = append(file2result[name], kv)

       // 可能会多次生成相同名称的中间文件，防止其重复录入
		record := false
		for _, item := range task.Result {
			if name == item {
				record = true
			}
		}
		if record == false {
			task.Result = append(task.Result, name)
		}
	}


// 持久化
// 使用temp文件重命名为所需的文件，以防止同一文件的并发读写
	for name, kva := range file2result {

		tmpfile, err := os.CreateTemp("./", "temp_*")
		if err != nil {
			log.Fatal(err)
		}

		enc := json.NewEncoder(tmpfile)
		for _, kv := range kva {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		tmpfile.Close()

		err = os.Rename(tmpfile.Name(), name)
		if err != nil {
			log.Fatal(err)
		}
		os.Remove(tmpfile.Name())
	}

// 任务完成
	task.Status = TaskCompleted
	args.Task = task
	ok := call("Coordinator.RPCHandler", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return
	}
```

在`Coordinater`接收到任务完成的请求后，会将原始数据文件标记为`已完成`，并将中间文件记录下来并标记为`未分配`，以供分配给`Reduce`任务。

```go
// 注：c.phase == PhaseMap 是为了防止一个map任务被多次分配后，
//    在reduce阶段又重新刷新中间文件为未分配
if task.Status == TaskCompleted && c.phase == PhaseMap {
	c.UpdateTask(task)
	or _, item := range task.ProcessFile {
       // 分配的原始文件标记为完成
		c.files[item] = FileCompleted
	}
	or _, item := range task.Result {
       // 将生成的中间文件标记为未分配
		c.intermediateFiles[item] = FileUnassigned
	}
}
// 获取并更新当前阶段
c.phase = c.GetPhase()
```

在进入`Reduce`阶段后，`Coordinater`分配的任务将不再是`Map`任务，而是`Reduce`任务。

```go
var assigned []string
reducerID := -1
// 为一个reduce任务分配相同后缀的文件
for filename, status := range c.intermediateFiles {
	tail, _ := strconv.Atoi(filename[len(filename)-1:])
	if status == FileUnassigned && reducerID == -1 {
		assigned = append(assigned, filename)
		c.intermediateFiles[filename] = FileAssigned
		reducerID = tail
	} else if status == FileUnassigned && tail == reducerID {
		ssigned = append(assigned, filename)
		c.intermediateFiles[filename] = FileAssigned
	}
}
```

在`worker`中执行`reduce`任务。

```go
// func DoReduce(reducef func(string, []string) string, args *Args, reply *Reply) 
	file2result := map[string]ByKey{}
	intermediate := []KeyValue{}

	task := reply.Task

	for _, filename := range task.ProcessFile {
		file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
       // 获取分配到的中间文件中所有的键值对
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	// 进行排序，使相同的key放在一起
	sort.Sort(ByKey(intermediate))

	for i := 0; i < len(intermediate); {
		j := i + 1
       // 记录同一个key的value数量
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}
		// 将相同key的value合并在一起
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		kv := KeyValue{}
		kv.Key = intermediate[i].Key
       // 调用reduce函数
		kv.Value = reducef(kv.Key, values)

       // 最终生成的文件名
		name := fmt.Sprintf("mr-out-%v", task.Id)
       // <文件名，内容>
		file2result[name] = append(file2result[name], kv)
		task.Result = append(task.Result, name)

		i = j
	}

// 持久化

// 结束任务时向Coordinater发送请求

```

`Coordinater`中收到`reduce`结束的请求后，记录中间文件状态以及更新阶段。

```go
		if task.Status == TaskCompleted && c.phase == PhaseReduce {
			c.UpdateTask(task)
			for _, item := range task.ProcessFile {
				c.intermediateFiles[item] = FileCompleted
			}
		}
		c.phase = c.GetPhase()
```

至此，在阶段为`已完成`后，结束`Coordinater`。

```go
func (c *Coordinator) Done() bool {
	ret := false

	if c.phase == PhaseComplete {
		ret = true
	}

	return ret
}
```

现在只是完成了系统在不会出错的情况下的`MapReduce`任务，而如果某些`worker`出现错误，我们则需要回溯任务，并重新执行。

首先在Lab1中所需要实现的解决方法是：如果一个任务超过十秒未响应，则认定其为`故失效

所以我们可以实现一个简单的心跳机制，在有`worker`向`Coordinater`发送请求后，遍历记录中的任务序列，并获取其时间，与现在时间做对比，然后决定保留此任务或回溯此任务。回溯任务时需要将各个文件的`已分配`记录改为`未分配`。

```go
// Coordinater
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
```

在`Coordinater`分配任务时，优先分配失效的任务，保证其`workerID`不会变化，否则生成的中间文件以及最终文件会有重复（若标为`失效`的其实依然在工作）。

```go
		reprocess := false
	    // 寻找是否有失效任务
		for _, item := range c.tasks {
			if item.Status == TaskFailed {
				reprocess = true
             // 直接将失效任务信息当作新任务发送给worker，
             // 使其处理的内容以及生成的内容保持一致
				reply.Task = Task{
					TaskType:    item.TaskType,
					Id:          item.Id,
					ProcessFile: item.ProcessFile,
					Status:      TaskWorking,
					timer:       time.Now(),
				}
				reply.NReduce = c.nReduce
             // 更新文件状态
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
    		// 分配新任务
		}
```

最后在`Coordinater`的处理函数中加一个大大的锁就好了（对高并发不友好，但是懒得去一点点加锁了）


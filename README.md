## MIT 6.5840 Lab

---



README.md中给出的是部分代码，完整代码请参考并对照[这里](https://github.com/Alipebt/MIT6.5840)



---



### Lab 1: MapReduce

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

---

## Lab 2: Key/Value Server

这个Lab很简单，就省略了

---

## Lab 3: Raft

测试方案参考：[这里](https://github.com/warr99/mit6.824-lab)

### Part 3A:  leader election

`Leader`选举是`Raft`一致性算法的核心机制，确保分布式系统中始终有一个明确的领导者来协调操作。当`Follower`节点在指定时间内未收到`Leader`的心跳消息时，会触发选举过程。

我们使用Go语言的`time.Ticker`来实现两个关键的定时任务：

- 选举超时定时器：监控Leader心跳，超时后触发选举
- 心跳定时器：Leader定期发送心跳维持领导地位

`func (rf *Raft) ticker()`中首先需要初始化这两个`Ticker`，并在结束后关闭`Ticker`。使用`select{case}`来接收超时，然后执行相应的处理函数。在每次选举超时后，再次随机生成一个超时时间，防止所有服务器在同一刻发生超时选举导致脑裂。

```go
func (rf *Raft) ticker() {
	// 初始化Ticker
	for rf.killed() == false {
		select {
		case <-electionTicker.C:
			Election()
          resetElectionTicker()
		case <-heartBeatTicker.C:
			HeartBeat()
		}
	}
}
```

`Election`函数会使当前服务器节点执行选举任务。首先将自己的`term+1`，节点状态改为`Candidater`，然后通过`RPC`向其他所有服务器发送`请求投票`。需要注意的是，由于`网络IO`会长时间阻塞进程，会影响到节点消息的收发，所以我们需要启用协程，每一个协程都会向一个固定的节点发送投票请求并等待回应，一旦收到回应则判断已收到的票数是否能够使自己成为Leader或是否投票失败`（拒绝的票数大于服务器数的一半）`。此外在`网络IO`的过程中需要先释放锁，结束后再加锁，防止影响其他关键操作的执行。

在`Election`过程中一旦收到了比自己的`term`更高的节点的消息或该节点收到现有`Leader`的心跳`，则立马更新term并变为Follower。

```go
// Election()
	for i, _ = range rf.peers {
		go func(server int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// ......
			rf.mu.Unlock()
			ok := rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()
			// ......
          // 判断是否更变状态
          if {
           }
		}(i)
	}
```

在`HeartBeat()`中，也类似选举超时的处理方式，为每一个节点单独使用协程来进行通信，并根据返回的数据判断是否需要更新`term`和节点状态。其发送的心跳在接收方只需要判断任期是否不比自己小，是则重置选举超时，不是则不做任何处理。最后将当前节点的`term`作为返回值返回给`Leader`。

在投票处理函数中则主要根据以下来规则来执行：

- `Candidate`的`term`小于当前节点的`term`，则拒绝投票
- `Candidater`的`term`等于当前节点的`term`，但当前节点已投过票，则拒绝投票
- `Candidater`的`term`等于当前节点的`term`，且当前节点未投票或者已投给`Candidater`，则同意投票
- `Candidater`的`term`大于当前节点的`term`，更新当前节点`term`和状态，并且同意投票

### Part 3B: log

完成这部分首先要注意以下几点：

- `Start()`是客户端所调用的函数，并注意其返回值。`Raft`中的所有`Log`都是从这获取的
- 成功`commit log`后需要使用`rf.applyCh <- applyMsg`向客户端进行回复
- `log[0]`可以初始化为空`log`，使下标从1开始，简化算法的实现逻辑
- 心跳和日志复制共用一个函数：`AppendEntries()`，其区别为心跳相当于复制空日志

先从`Start()`函数开始。在`Leader`中，`Start()`会先将客户端发来的内容存入本地`log`中，然后返回该`log`的下标，用来检测是否这个`log`在系统中达成一致。

```go
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.status == Leader
	if !isLeader {
		return -1, term, isLeader
	}

	entry := Log{
		Command: command,
		Term:    rf.currentTerm,
	}

	rf.logs = append(rf.logs, entry)
    // 已知节点已复制的最新日志的下标。
    // 此处为设置自己已知自己的最新日志。用于后续判断commitIndex
	rf.matchIndex[rf.me] = len(rf.logs) - 1

    // 返回此log下标
	return len(rf.logs) - 1, term, isLeader
}

```

现在本地有了新日志了，需要对整个系统中的节点进行日志复制。在`Leader`发送心跳时，根据`nextIndex`判断是否存在新日志需要发送。

```go
type AppendEntriesReply struct {
	Term    int
	Success bool

	// 冲突log对应的term
	XTerm int
	// 第一条Term==XTerm的log
	XIndex int
	// log长度
	XLen int
}
```



当收到添加成功的回复时，更改`Leader`的`nextIndex`和`matchIndex`。其中`nextIndex`是要向各节点添加的下一个`log`的下标，`matchIndex`是已知各节点成功复制的最新`Log`的下标。

当收到失败的回复时，则根据以下逻辑填充所需的数据：

- `XTerm`：这个是`Follower`中与`Leader`冲突的`Log`对应的任期号。如果`Follower`在对应位置的任期号不匹配，它会拒绝`Leader`的`AppendEntries`消息，并将自己的任期号放在`XTerm`中。如果`Follower`在对应位置没有`Log`，那么这里会返回` -1`。
- `XIndex`：这个是`Follower`中，对应任期号为`XTerm`的第一条`Log`条目的槽位号。
- `XLen`：当前`Follower`的`Log`长度

```go
func (rf *Raft) handleAppendLog(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	isSuccess := false
	if rf.status == Leader {
		return false
	}
	// 如果领导人的任期小于接收者的当前任期
	if args.Term < rf.currentTerm {
		return false
	}
	// 找不到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目
	if rf.getLog(args.PrevLogIndex).Term == -1 {
		// index冲突(无此index)
		reply.XTerm = -1
	} else if rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		// Term冲突
		reply.XTerm = rf.getLog(args.PrevLogIndex).Term
	} else {
		isSuccess = true
	}
	// term为XTerm的第一条log, 当XTerm=-1时,XIndex一直为0
	for index, log := range rf.logs {
		if log.Term == reply.XTerm {
			reply.XIndex = index
			break
		}
	}
	// 如果一个已经存在的条目和刚刚接收到的日志条目发生了冲突（因为索引相同，任期不同），
	// 那么就删除这个已经存在的条目以及它之后的所有条目
	// 追加日志中尚未存在的任何新条目
	// L:   0   1   2   3   4
	// F:	0   1   2   5
	//				p   n
	// E:             3   4
	// args.Entries != nil排除心跳
	if isSuccess && args.Entries != nil {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
	}
	reply.XLen = len(rf.logs)
	// 已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引
	if isSuccess && args.LeaderCommit > rf.commitIndex {
		//  leaderCommit 或者是 上一个新条目的索引 取两者的最小值
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
	}
	return isSuccess
}
```

在`Follower`写入`reply`后，再根据`LeaerCommit`更新自己的`commitIndex`。然后`Leader`处理接收到的信息

```go
if reply.Success {
	// 成功接收log
	// 由于发送了所有的新log，如果成功了则该node的下一个log位置为log长度
	rf.nextIndex[server] = reply.XLen
	rf.matchIndex[server] = reply.XLen - 1
} else {
	// 未成功接收，则一定返回有XTerm,Xindex,Xlen
	if reply.XTerm != -1 {
		// -1表示PrevLogIndex位置发生冲突，即nextIndex-1或len(log)位置。表示要插入的位置的前一个位置冲突
		// leader检查自身是否有Xterm的Term
		iXTerm := -1
		// 倒序遍历，提取最后一个Term=XTerm的index
		for iXTerm = len(rf.logs) - 1; iXTerm >= 0; iXTerm-- {
			if rf.logs[iXTerm].Term == reply.XTerm {
				break
			}
		}
		if iXTerm != -1 {
			// 存在，设nextIndex为indexLastXTerm下一个。
			// 表示peer在XTerm期间的所有日志都已存在(对于peer，XTerm为最新Term)
			// L:	[0   1   2   3   4]
			// t:    1   1   1   2   2
			//                      n=5
			// F: 	[0   1   2   3   4]
			// t:	 1   1   1   1   1
			//                n=3
			rf.nextIndex[server] = iXTerm + 1
		} else {
			// 不存在，设nextIndex为reply.XIndex
			// L:	[0   1   2   3   4]
			// t:	 1   1   1   2   2
			// 						     n=5
			// F:	[0   1   2   3   4]
			// t:	 1   1   1   1   1
			//					 n=3
			rf.nextIndex[server] = reply.XIndex
		}
	} else {
		// PrevLogIndex位置不存在log
		// L:	[0   1   2   3   4]
		// t:	 1   1   1   2   2
		// 				        p=4 n=5
		// F:	[0   1   2   3]
		// t:	 1   1   1   1
		//					     n=4

		rf.nextIndex[server] = reply.XLen
	}
}
```

在`Leader`为所有节点处理结束后，根据各个节点状态来判断是否该`log`达成了共识

```go
// 过半票决，成功为大多数添加日志
for N := rf.commitIndex + 1; N < len(rf.logs); N++ {
	agreement := 0
	for _, matchIndex := range rf.matchIndex {
		if matchIndex >= N && rf.logs[N].Term == rf.currentTerm {
			agreement++
		}
	}
	if agreement > len(rf.peers)/2 {
		rf.commitIndex = N
	}
}
```

然后每个心跳期间也同时进行判断是否将某个`log`应用到状态机（达成一致后消息回传）

```go
func (rf *Raft) commitMsg() {
	if rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		applyMsg := rf.createApplyMsg()
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()
	}
}
```

此外，由于选举过程中，拥有最新已提交的日志的节点才允许成为`Leader`，所以现在需要进行选举约束，这里的限制是，节点只能向满足下面条件之一的候选人投出赞成票：

- 候选人最后一条Log条目的任期号**大于**本地最后一条Log条目的任期号；

- 或者，候选人最后一条Log条目的任期号**等于**本地最后一条Log条目的任期号，且候选人的Log记录长度**大于等于**本地Log记录的长度

```go
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果term < currentTerm返回 false
	// 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.updateNodeWithTerm(args.Term)
		reply.VoteGranted = true
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.getLog(-1).Term {
			// 候选人最后一条Log条目的任期号大于本地最后一条Log条目的任期号；
			reply.VoteGranted = true
		} else if args.LastLogTerm == rf.getLog(-1).Term && args.LastLogIndex >= len(rf.logs)-1 {
			// 或者，候选人最后一条Log条目的任期号等于本地最后一条Log条目的任期号，
          // 且候选人的Log记录长度大于等于本地Log记录的长度
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.resetElectionTicker()
	}
}
```


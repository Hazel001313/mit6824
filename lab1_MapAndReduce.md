## 1 Worker的实现

```go
func Worker(mapf func(string, string) []KeyValue,
    reducef func(string, []string) string) {
    for {
        task := rpc_request_task() //send RPC request for a task
        switch task.Tasktype {
        case MapTask:
            if domap(mapf, &task) == nil {
                rpc_report_task(&task, true)
            } else {
                rpc_report_task(&task, false)
            }
        case ReduceTask:
            if doreduce(reducef, &task) == nil {
                rpc_report_task(&task, true)
            } else {
                rpc_report_task(&task, false)
            }
        case StopTask:
            return
        case WaitTask:
            time.Sleep(time.Second)
        }
    }
```

​		根据从Coordinator取到的任务类型做相应的任务。注意：在做map和reduce任务时，为保证写文件的原子性，先用ioutil.TempFile生成相应的临时文件，再用os.Rename完成原子性替换



## 2 Coordinator的实现

### 2.1 基于channel解决并发问题

```go
func (c *Coordinator) schedule() {
    c.init_maptasks()
    for {
        select {
        case msg := <-c.requestCh:
            ...
            msg.ok <- struct{}{}
        case msg := <-c.reportCh:
            ...
            msg.ok <- struct{}{}
        case msg := <-c.timingCh:
            ...
            msg.ok <- struct{}{}
        case msg := <-c.statusCh:
            msg.status <- c.condition
        }
    }
}
```

​		requestCh传递Worker索要任务的消息；reportCh传递Worker报告任务完成情况的消息; timingCh传递定时器消息; statusCh传递查询Coordinator状态的消息。对Coordinator数据的全部操作均在schedule()内完成。

###　2.2 与worker的交互

初始化map任务：

```go
func (c *Coordinator) init_maptasks() {
    for i, file := range c.mapfiles {
        newtask := &TaskInfo{
            Taskno:   i,
            Tasktype: MapTask,
            Filename: []string{file},
            Nmap:     c.nmap,
            Nreduce:  c.nreduce,
        }
        c.tasks <- newtask
        c.metainfos[i] = &TaskMetainfo{
            taskinfo:      newtask,
            taskcondition: TaskWaiting,
        }
    }
}
```

在任务队列中添加新map任务，更新各map任务的元信息



处理worker发送过来的索要任务/报告任务处理情况的rpc请求：

```go
func (c *Coordinator) Request(args *TaskArgs, reply *TaskReply) error {
    msg := &requestMsg{
        args:  args,
        reply: reply,
        ok:    make(chan struct{}),
    }
    c.requestCh <- msg
    <-msg.ok
    return nil
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
    msg := &reportMsg{
        args:  args,
        reply: reply,
        ok:    make(chan struct{}),
    }
    c.reportCh <- msg
    <-msg.ok
    return nil
}
```

msg打包传递给schedule()协程统一处理



Coordinator状态的转移Map-》Reduce-》Alldone：

```go
func (c *Coordinator) checkcondition() {
    for _, metainfo := range c.metainfos {
        if metainfo.taskcondition != TaskDone {
            return
        }
    }
    switch c.condition {
    case Map:
        c.init_reducetasks()
        c.condition = Reduce
    case Reduce:
        c.condition = AllDone
    }
}
```

每次收到worker传来的任务完成消息后，checkcondition()检查当下各任务的状态，在当下所有任务都完成的情况下转换Coordinator的状态。在Map状态转换为Reduce状态时init_reducetasks初始化reduce任务。



### 2.3 定时器：重新schedule超时任务

在发送任务给Worker时，设置10秒的定时器

```go
case msg := <-c.requestCh:
            if len(c.tasks) > 0 {
                task := <-c.tasks
                *msg.reply = TaskReply{Taskinfo: *task}
                c.metainfos[task.Taskno].taskcondition = TaskWorking
                c.metainfos[task.Taskno].starttime = time.Now()
                c.metainfos[task.Taskno].tasktimer = time.NewTimer(10 * time.Second)
                go c.timing(task, c.metainfos[task.Taskno].tasktimer)
            }
            ...
            msg.ok <- struct{}{}
```

10秒后timing发送消息给schedule

```go
func (c *Coordinator) timing(task *TaskInfo, timer *time.Timer) {
    <-timer.C
    msg := &timingMsg{
        task: task,
        ok:   make(chan struct{}),
    }
    c.timingCh <- msg
    <-msg.ok
}
```

schedule检查，如果任务顺利完成，则定时器取消计时，否则重新将任务加入队列中

```go
case msg := <-c.reportCh:
            reportmsg := msg.args
            if !reportmsg.Done {
                c.restart(&reportmsg.Taskinfo)
            } else {
                c.metainfos[reportmsg.Taskinfo.Taskno].taskcondition = TaskDone
                c.metainfos[reportmsg.Taskinfo.Taskno].tasktimer.Reset(0 * time.Second)
                c.checkcondition()
            }
            msg.ok <- struct{}{}
```

schedule 检查，如果经检查定时器对应的任务超时，则重新将任务加入到队列中

```go
case msg := <-c.timingCh:
            task := msg.task
            if c.metainfos[task.Taskno].taskcondition == TaskWorking && time.Since(c.metainfos[task.Taskno].starttime) > 10*time.Second {
                c.restart(task)
            }
            msg.ok <- struct{}{}
```



## 参考的R语言学习资料 

golang 并发 chan https://studygolang.com/articles/6106go

语言并发与并行学习笔记 https://blog.csdn.net/kjfcpua/article/details/18265441 

golang线程与通道 https://studygolang.com/articles/3311golang

栈逃逸 https://blog.csdn.net/weixin_44065217/article/details/122482368golang

并发：channel还是锁 https://cloud.tencent.com/developer/article/1412497
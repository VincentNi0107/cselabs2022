# Note of Lab1
![layers](./assets/lab2a.png)
这个lab是今年新出的，其实有点僵硬。一开始测试脚本有问题，导致我浪费了一下午debug。。。 :(

由于这一部分都是串行操作，不存在transaction之间的action交叉，所以WAL的很多东西是用不到的，比如tx_id,action_id,前向指针以及persister里的mtx锁。预测一波lab2b是文件系统的多线程读写，这些到时候再写，这个lab里就偷懒了。
## Part1 Persistency
实现持久化储存：
我采用的方法是在extent_server层给每一个create,put和remove操作创建一个log，写入checkpoint文件中（直接把这个文件当成模拟磁盘了）。这样做其实后面part2还挺方便的。

建议在chfs_command类里加一条string属性用来存储三种指令的信息，然后加一个serialize和deserialize方法实现chfs_command类和字符串之奶奶的转换，方便文件读写。
## Part2 All-or-nothing
Guidance里说”Note that in practice, system crash may also occur when a log entry is being written to the log file. But in this lab we consider each log operation as an atomic operation, so you do not have to worry about this issue.“其实这里的log也是没有Atomicity保证的，stop.sh有可能会打断write函数的磁盘写操作，但在我的实际测试中没有发生过这种事。我是基于每一个write操作的Atomicity写的，不然会复杂很多。


在chfs_client层里每个操作先begin,执行完commit，都写进checkpoint里。每次重启restore的时候忽略所有没有commit的transaction。我part1采用的持久化方法是个存redo log，所以这里非常方便。
## Part 3: Checkpoint
因为我没用log文件 所以不用做Checkpoint操作。：）

就partA看来这个lab设计的不算很好，不如去年的[map-reduce](https://ipads.se.sjtu.edu.cn/courses/cse/2021/labs/lab2.html),作为对WAL的训练也不如往届的[handson1](https://ipads.se.sjtu.edu.cn/courses/cse/2021/handson/handson-1.html)和[handson2](https://ipads.se.sjtu.edu.cn/courses/cse/2021/handson/handson-2.html)。
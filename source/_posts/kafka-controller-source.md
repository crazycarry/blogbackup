---
title: Kafka controller架构分析
date: 2018-04-04 17:42:52
tags: 
     - 源码分析
     - 消息中间件
     - 大数据
     - 架构设计
categories: kafka
---
kafka在0.8版本前没有提供Partition的Replication机制，一旦Broker宕机，其上的所有Partition就都无法提供服务，而Partition又没有备份数据，数据的可用性就大大降低了。所以0.8后提供了Replication机制来保证Broker的failover。由于Partition有多个副本，为了保证多个副本之间的数据同步，有多种方案：
* 1.所有副本之间是无中心结构的，可同时读写数据，需要保证多个副本之间数据的同步 
* 2.在所有副本中选择一个Leader，生产者和消费者只和Leader副本交互，其他follower副本从Leader同步数据
<!-- more -->
## Replication

### 数据同步
kafka在0.8版本前没有提供Partition的Replication机制，一旦Broker宕机，其上的所有Partition就都无法提供服务，而Partition又没有备份数据，数据的可用性就大大降低了。所以0.8后提供了Replication机制来保证Broker的failover。由于Partition有多个副本，为了保证多个副本之间的数据同步，有多种方案：

* 1.所有副本之间是无中心结构的，可同时读写数据，需要保证多个副本之间数据的同步
* 2.在所有副本中选择一个Leader，生产者和消费者只和Leader副本交互，其他follower副本从Leader同步数据



第一种方案看起来可以对客户端请求进行负载均衡，但是由于要在多个副本之间互相同步数据，数据的一致性和有序性难以保证。而第二种方案看起来客户端连接的节点会少点，而且其他副本同步数据可能没有那么及时，但是在正常情况下，客户端只需要和Leader一个副本通信即可，而其他follower只需要和Leader同步数据。假设有一个Partition有5个副本，4个follower只需要各自和leader建立一条链路通信，而对于第一种方案，5个副本之间要两两通信，确保Partition的每个副本的数据都是一致的。所以第一种方案虽然提供了客户端的负载均衡，但是对于服务端的设计带来比较大的复杂性，而第二种方案虽然限制了客户端只能连接Partition的Leader Replica，但这种简洁的设计使得服务端更加健壮。

[![](http://img.blog.csdn.net/20160310090207079 "k_replica_design")](http://img.blog.csdn.net/20160310090207079 "k_replica_design")

### 同步策略

存在Replication的目的是为了在Leader发生故障时，follower副本能够代替Leader副本继续工作，即新的Leader必须拥有原来的Leader提交过的所有消息，那么在任何时刻follower要保证和Leader比起来总是最新的，或者说follower要和leader的数据始终保持同步。

有两种策略保证副本和leader是同步的，`primary-backup replication`和`quorum-based replication`。这两种模式下都会选举一个副本作为leader，其他的副本都作为follower。所有的写请求都会经过leader，然后leader会将写传播给follower（kafka因为使用pull模式，所以是follower从leader拉取数据，不过总的来说，目的都是将leader的数据同步给所有的follower）。

使用基于**quorum**的复制方式（也叫做**Majority Vote**，少数服从多数），leader需要等待副本集合中大多数的写操作完成（大多数的概念是指一半以上）。如果一些副本当掉了，副本组的大小也不会发生变化，这就会导致写操作无法写到失败的副本上，就无法满足半数以上的限制条件。这种模式下，假设有2N+1个副本，在提交之前要确保有N+1个副本复制完消息，为了保证正确选出新的Leader，失败的副本数不能超过N个。因为在剩下的任意N+1个Replica里，至少有一个Replica包含有最新的所有消息。这种策略的缺点是：为了保证Leader选举的正常进行，它所能容忍的失败的follower个数比较少，如果要容忍N个follower挂掉，必须要有2N+1个以上的副本。

[![](http://img.blog.csdn.net/20160309085146309 "k_major_vote")](http://img.blog.csdn.net/20160309085146309 "k_major_vote")

使用**主备**模式复制，leader会等待组（所有的副本组成的集合）中所有副本写成功后才返回应答给客户端。如果其中一个副本当掉了，leader会将它从组中删除掉，并继续向剩余的副本写数据。一个失败的副本在恢复之后如果能赶上Leader，leader就会允许它重新加入组中。kafka中这个组的概念就是Leader副本的ISR集合。这个ISR里的所有副本都跟上了Leader，只有ISR里的成员才有被选为Leader的可能。这种模式下对于N+1个副本，一个Partition能在保证不丢失已经提交的消息的前提下容忍N个副本的失败（只要有一个副本没有失败，其他失败了都没有关系）。比较Majority Vote和ISR，为了容忍N个副本的失败，两者在**提交前需要等待**的副本数量是一样的（ISR中有N+1个副本才可以容忍N个副本失败，而Majority Vote副本总数=2N+1，N+1个副本成功，才能容忍N个副本失败，所以需要等待的副本都是N+1个。这里的等待一般是将请求发送到N+1个副本后，要等待这些副本应答后才表示成功提交），但是ISR需要的总的副本数几乎是Majority Vote的一半（假设ISR中所有副本都能跟上Leader，则一共也之后N+1个副本，而Majority Vote则需要2N+1个副本）。

### leader选举

Leader选举本质上是一个分布式锁，有两种方式实现基于ZooKeeper的分布式锁：

* 1.节点名称唯一性：多个客户端创建一个节点，只有成功创建节点的客户端才能获得锁
* 2.临时顺序节点：所有客户端在某个目录下创建自己的临时顺序节点，只有序号最小的才获得锁

Majority Vote的选举策略和ZooKeeper中的Zab选举是类似的，实际上ZooKeeper内部本身就实现了少数服从多数的选举策略。kafka中对于Partition的leader副本的选举采用了第一种方法：为Partition分配副本，指定一个ZNode临时节点，第一个成功创建节点的副本就是Leader节点，其他副本会在这个ZNode节点上注册Watcher监听器，一旦Leader宕机，对应的临时节点就会被自动删除，这时注册在该节点上的所有Follower都会收到监听器事件，它们都会尝试创建该节点，只有创建成功的那个follower才会成为Leader（ZooKeeper保证对于一个节点只有一个客户端能创建成功），其他follower继续重新注册监听事件。

### 副本放置策略

kafka将一个逻辑意义的topic分成多个物理意义上的partition，每个partition是这个topic的一部分消息，将partition分布在多个brokers上，可以达到负载均衡的目的。除了Partition的分配，每个Partition都有Replcias，所以也要考虑Replica的分配策略，因为不能把一个Partition的所有Replicas都放在同一台机器上。一般我们希望将所有的Partition能够均匀地分布在集群中所有的Brokers上。Kafka分配Replica的算法如下：

* 将所有存活的N个Brokers和待分配的Partition排序
* 将第i个Partition分配到第(i mod n)个Broker上，这个Partition的第一个Replica存在于这个分配的Broker上，并且会作为partition的优先副本
* 将第i个Partition的第j个Replica分配到第((i + j) mod n)个Broker上

假设集群一共有4个brokers，一个topic有4个partition，每个Partition有3个副本。下图是每个Broker上的副本分配情况。

[![](http://img.blog.csdn.net/20160224133413052 "k_partition")](http://img.blog.csdn.net/20160224133413052 "k_partition")

### Follower failure

如果Follower Replica失败超过一定时间后，Leader会将这个失败的follower从ISR中移除（follower没有发送fetch请求）。由于ISR保存的是所有全部赶得上Leader的follower replicas，失败的follower肯定是赶不上了。虽然ISR现在少了一个，但是并不会引起的数据的丢失，ISR中剩余的replicas会继续同步数据（只要ISR中有一个follower，就不会丢失数据，实际上leader replica也是在ISR中的，所以即使所有的follower都挂掉了，只要leader没有问题，也不会出现问题的）。

当失败的follower恢复时，它首先将自己的日志截断到上次checkpointed时刻的HW（checkpoint是每个broker节点都有的）。因为checkpoint记录的是所有Partition的hw offset。当follower失败时，checkpoint中关于这个Partition的HW就不会再更新了。而这个时候存储的HW信息和follower partition replica的offset并不一定是一致的。比如这个follower获取消息比较快，但是ISR中有其他follower复制消息比较慢，这样Leader并不会很快地更新HW，这个快的follower的hw也不会更新（leader广播hw给follower）。这种情况下，这个follower日志的offset是比hw要大的。所以在它恢复之后，要将比hw多的部分截掉，然后继续从leader拉取消息（跟平时一样）。实际上，ISR中的每个follower日志的offset一定是比hw大的。因为只有ISR中所有follower都复制完消息，leader才会增加hw，而每个Replica复制消息后，都会增加自己的offset。也就是说有可能有些follower复制完了，而有另外一些follower还没有复制完，那么hw是不会增加的。

[![](http://img.blog.csdn.net/20160310090248232 "k_replica_hw")](http://img.blog.csdn.net/20160310090248232 "k_replica_hw")

Kafka的Replica实现到目前0.9版本为止，分别经历了三个阶段：

| 阶段 | 主要做法 | 优缺点 |
| :-: | :-: | :-: |
| 1 | zookeeper，很多监听器 | 脑裂，羊群效应，ZK集群负载过重 |
| 2 | one brain，zk queue | 将Replcia改变的状态事件用ZK队列实现 |
| 3 | state machine，controller, direct rpc | 直接RPC更快，状态机统一处理 |

### zk path & listeners

下表汇总了zk中和partition相关的节点，对于不同版本，路径可能不同，不过要存储的信息都是类似的：

| zookeeper path | type and creator | value |
| :-: | :-: | :-: |
| *v1* |
| /brokers/ids/[broker_id]–>host:port | 临时节点,admin | 所有存活的Brokers的信息 |
| /brokers/topics/[topic]/[partition_id]/**replicas**–>{broker_id …} | admin | 每个Partition当前分配的Replicas(AR) |
| brokers/topics/[topic]/[partition_id]/**leader**–>broker_id | 临时节点,leader | 这个partition的当前leader副本 |
| /brokers/topics/[topic]/[partition_id]/**ISR**–>{broker_id, …} | leader | 保持和leader同步的replica编号 |
| *v2* |
| /brokers/topics/[topic]/[partition_id]/**leaderAndISR**–>   {leader:broker_id,ISR:{broker1..}} | controller,leader | Partition的Leader和ISR |
| *v3* |
| /brokers/topics/**[topic]**–>{part1: [broker1, broker2]…} | admin | topic中所有Partition的AR |
| /brokers/topics/[topic]/[partition_id]/**state**–>   {leader:broker_id,ISR:{broker1..}} | controller,leader | Partition的状态信息类似leaderAndISR |
| /controller –> {brokerid} | controller | 当前集群的controller节点 |
| *common* |
| /admin/partitions_reassigned/[topic]/[partition_id]–>{broker_id …} | admin | 重新分配partition |
| /admin/partitions_add/[topic]/[partition_id]–>{broker_id …} | admin | 新添加partition对应的AR |
| /admin/partitions_remove/[topic]/[partition_id] | admin | 删除topic中已经存在的partition |

在v2版本中，Partition重新分配的监听器只注册在leader的Broker上，而Leader和State变化的监听器注册在所有Broker上。一个Partition的多个Replica会分布在多个Broker上，只有在Leader Replica的Broker上才注册Partition-Reassigned监听器。而Leader和State变化对于Partition的所有副本都要能够感知，所以一个Partition所有Replica分布的Broker都要注册Leader-change和State-change监听器。

[![](http://img.blog.csdn.net/20160310104717825 "k_replica_listeners")](http://img.blog.csdn.net/20160310104717825 "k_replica_listeners")

### Broker,Partition和Replica的事件流

Partition的Replica存储在Broker上，并且和ZooKeeper互相交互，主要完成这些工作：

* 1.Broker启动读取ZK中Partition的所有AR
* 2.每个Replica判断Leader是否存在，不存在则进入选举阶段，存在则使自己成为Follower
* 3.每个Replica都会在leader上注册监听器，当Leader发生变化时，所有Replica会开始选举Leader
* 4.选举Leader时，最先创建leader节点的Replica成为Leader，其他Replica再次成为Follower

[![](http://img.blog.csdn.net/20160310130841867 "k_p_r_b_z")](http://img.blog.csdn.net/20160310130841867 "k_p_r_b_z")

v1版本对于每个replica变化都会触发replicaStateChange调用。

[![](http://img.blog.csdn.net/20160310104453777 "k_replica_v1")](http://img.blog.csdn.net/20160310104453777 "k_replica_v1")

v1版本中，由于各种事件严重依赖ZooKeeper，存在着以下问题：

* 脑裂：虽然ZooKeeper能保证注册到节点上的所有监听器都会按顺序被触发，但并不能保证同一个时刻所有副本看到的状态是一样的，可能造成不同副本的响应不一致
* 羊群效应：如果宕机的那个Broker的Partition数量很多，会造成多个Watch被触发，引起集群内大量的调整
* 每个副本都要在ZK的Partition上注册Watcher，当集群内Partition数量很多时，会造成ZooKeeper负载过重

[![](http://img.blog.csdn.net/20160309092701811 "zk_listener_problem")](http://img.blog.csdn.net/20160309092701811 "zk_listener_problem")

v2版本不再为replicas注册Replica变化的事件，而是放到broker级别的状态变化事件。而且v2的startReplica和v1的replicaStateChange功能是相似的，都是完成replicas发生变化时判断是否需要选举或成为follower。

[![](http://img.blog.csdn.net/20160310104507199 "k_replica_v2")](http://img.blog.csdn.net/20160310104507199 "k_replica_v2")

所有的brokers只对状态变化事件进行响应，而且状态变化事件也只由Leader决定，状态变化是通过onPartitionsReassigned写到zk的`/brokers/state/[broker_id]`的请求事件队列触发的，而这个事件只注册在Partition的Leader Replica上。也就是说follower状态的改变只基于partition的Leader发送请求时才会发生，如果leader没有对某个follower说要改变状态，则follower是不会有任何事件发生的，这种方式类似于事件驱动的模式（引入了一个中间的状态机）。而第一版中任何一个replica改变，都有可能导致所有其他follower都要做出相应（直接触发）。

[![](http://img.blog.csdn.net/20160310145330545 "k_replica2state")](http://img.blog.csdn.net/20160310145330545 "k_replica2state")

注意：replicas的状态变化通过队列的形式并不会立即被触发，而leader变化事件要能立即被触发，所以要给这个事件单独注册Leader-Change监听器，而不能统一都放在State-Change监听器里，否则的话，leader变化事件可能会淹没在replicas的状态变化事件里，而没有被及时地处理（Leade变化这个事件是大BOSS发话，要立即执行，replicas的状态变化是其他领导发话，要进行排期依次处理）。

在这两个版本中，Partition都有一个commitQ队列用来缓存生产请求，并且每个Partition都有一个commit线程负责将消息写到Leader的本地日志中，并等待ISR中所有副本复制。Follower会向Leader抓取数据来保持和Leader的消息同步。

[![](http://img.blog.csdn.net/20160311085103389 "k_produce_fetch")](http://img.blog.csdn.net/20160311085103389 "k_produce_fetch")

在Follower向Leader抓取数据时，要根据Follower的抓取进度判断是否应该把它加入ISR或从从ISR中移除：

```scala  
maybe_change_ISR() {
 // 如果follower太慢会被leader从ISR中移除,确保leader可以顺利提交消息尽管isr的副本数变少了
 find the smallest leo (leo_min) of every replica in ISR
 if ( leader.leo - leo_min > MAX_BYTES_LAG || 
 the replica with leo_min hasn't updated leo for more than MAX_TIME_LAG)
 newISR = ISR - replica with leo_min
  
 //如果follower赶上了leader,加入到ISR中
 for each replica r not in ISR
 if ( r.leo - leo_min 
 newISR = ISR + r
  
 update the LeaderAndISR path in ZK with newISR
 if (update in ZK successful) {
 leader.partition.ISR = new ISR
 }
}

```



## Controller要解决的问题

Replication的v3版本采用Controller实现，相比v2版本的主要不同点是：

* Leader的变化从监听器改为由Controller管理
* 控制器负责检测Broker的失败，并为每个受影响的Partition选举新的Leader
* 控制器会将每个Leader的变化事件发送给受影响的每个Broker
* 控制器和Broker之间的通信采用直接的RPC，而不是通过ZK队列

虽然因为引入了Controller，需要实现Controller的failover。但它的优点有：

* 因为Leader管理被更加集中地管理，比较容易调试问题
* Leader变化针对ZK的读写可以批量操作，减少在failover过程中端到端的延迟
* 更少的ZooKeeper监听器
* 使用直接RPC协议相比队列实现的ZK，能够更加高效地在节点之间通信

从整个集群的所有Brokers中选举出一个Controller，它主要负责：

* Partition的Leader变化事件
* 新创建或删除一个topic
* 重新分配Partition
* 管理分区的状态机和副本的状态机

当控制器完成决策之后（决定了Partition新的Leader和ISR），它会将这个决策持久化到ZK中（将LeaderAndISR写入到ZK节点），并且向所有受到影响的Brokers通过直接RPC的形式直接发送新的决策（发送LeaderAndISRRequest）。这些决策（持久化到ZK中的数据）是真理之源，它们会被客户端用来路由请求（客户端只跟决策信息里的Leader交互），并且每个Broker启动的时候会用来恢复它的状态（Partition分配到Broker上，说明Broker现在拥有了分配到的Partition）。当Broker启动之后，它会接收控制器通过直接RPC的形式发送过来的最新的决策。

每个KafkaServer中都会创建一个KafkaController对象，但是集群中只允许存在一个Leader Controller，这是通过ZooKeeper选举出来的：第一个成功创建zk节点的那个Controller会成为Leader，其他的Controller会一直存在，但是并不会发挥作用。只有当原先的Controller挂掉后，才会选举出新的Controller。**集群中所有Brokers选举一个Controller**和**Partition中所有Replicas选举一个Leader**是类似的。有点类似于Hadoop-1.x中的SecondaryNameNode：同时启动NameNode和SecondaryNameNode，当NameNode挂掉后，SecondaryNameNode会代替成为NameNode角色。在系统运行过程中，SecondaryNameNode要同步NameNode的数据，这样在NameNode发生故障时，SecondaryNameNode切换为NameNode时数据并不会丢失或落后太多。Hadoop-2.x的HA使用了ZKFailoverController有两个Master：Active和Standby，工作原理和SNN是类似的，只不过Active Master将信息写入共享存储，Standby从共享存储中读取信息，保持与Active Master的同步，从而减少故障时的切换时间。不过KafkaController的failover机制并不会在系统运行过程中将其他所有Controller实例的数据和Leader同步，而是在Leader发生故障时重新选举，并且重新恢复数据。

原先在没有Controller时，Leader或者Replica的变化都是通过监听器完成的，现在引入Controller之后，不仅要处理Broker的failover，也要处理Controller的failover。Broker和Controller的failover也是通过注册在ZK上的Watcher监听器完成的，所有的Brokers监听一个Controller，Controller也监听所有的Controller，两者互相关注。

[![](http://img.blog.csdn.net/20160311085124561 "k_controller_failover")](http://img.blog.csdn.net/20160311085124561 "k_controller_failover")

### Broker Failover：on_broker_change

Broker失败，Controller注册在/brokers/ids的Watcher会触发调用onBrokerFailure。失败的Broker上的Replica可能是某个Partition的Leader也可能是某个Partition的follower。如果是Partition的Leader，那么Controller要确保有其他Broker成为这个Partition的Leader，如果是Partition的Follower，虽然不需要重新选举Leader，但是有可能Partition的ISR会变化。所以Controller首先会读取ZK中的ISR/AR选举新的Leader和ISR，然后把这个信息先保存到ZK中，最后把包含了新Leader和ISR的LeaderAndISR指令发送给受到影响的Brokers。收到指令的Broker如果Controller命令它成为某个Partition的Leader，那么原先为Follower副本的Replica就会becomeLeader。下面举个例子说明这个过程：

* Partition1有三个副本：Replica1，Replica2，Replica3。其中Replica1是Leader，ISR=[1,2,3]
* Replica1所在的Broker1挂掉了（现在Partition没有了Leader），Controller触发onBrokerFailure
* Controller读取ZK的leaderAndISR，选举出新的leader和ISR。Leader=Replica2，ISR=[2,3]
* Controller将最新的leaderAndISR写到ZK中（leader=Replica2，ISR=[2,3]）
* Controller将最新的leaderAndISR构造成LeaderAndISRRequest发送给Broker2，Broker3
* Replica2所在的Broker2收到指令，因为最新的leader指示Replica2是Leader，所以Replica2成为Partition1的Leader
* Replica3所在的Broker3收到指令，Replica3仍然是follower，并且在ISR中，becomeFollower

Broker失败后Controller端的处理步骤如下：

1. 从ZK中读取现存的brokers
2. broker宕机，会引起partition的Leader或ISR变化，获取在在这个broker上的partitions：set_p
3. 对set_p的每个Partition P
    3.1 从ZK的leaderAndISR节点读取P的当前ISR
    3.2 决定P的新Leader和新ISR（优先级分别是ISR中存活的broker，AR中任意存活的作为Leader）
    3.3 将P最新的leader，ISR，epoch回写到ZK的leaderAndISR节点
4. 将set_p中每个Partition的LeaderAndISR指令（包括最新的leaderAndISR数据）发送给受到影响的brokers

最后一步采用发送指令的方式实际上是一种RPC请求，之前的版本中采用的是在ZK的leader节点注册监听器来监控leader的变化，不过我们已经看到了这种方式会使得ZK的负载过重，而使用RPC的方式可以在不依赖ZK的情况下同样可以处理leader的变化。

`如何决定P的新Leader和新ISR？`

假设AR=[1,2,3,4,5]，ISR=[1,2,3]，但是存活的Brokers=[2,3,5]。选择Leader的方式是ISR中目前存活的Brokers，比如目前存活的Broker是[2,3,4]，所以ISR中的副本1是不能作为Leader的，也不会再作为ISR了。Leader的选举是选举目前还存活的[2,3]中的一个，ISR的选举是选举在ISR中当前仍然存活的Broker=[2,3]。所以最后Leader=2，ISR=[2,3]。

假设AR=[1,2,3,4,5]，ISR=[1,2,3]，但是存活的Brokers=[4,6,7]。因为ISR中没有一个Broker在当前处于存活状态，所以只能退而求其次从AR中选择，幸运的是AR中的4目前是存活的，所以Leader=4，ISR=[4]。由于4不再ISR中，所以这种情况有可能会造成数据丢失，因为只有选举处于ISR中的，才不会丢失数据，但是现在ISR中的没有一个存活，所以也只好选择有可能丢失的Broekr，总比找不到任何的Broker要好吧。

`什么叫做受到影响的brokers？`

Partition有多个Replicas，Replica是分布在Broker上的物理表示，所以一个Broker上受到影响的Replica的Partition肯定还有其他副本分布在其他Broker上，所有含有宕机Broker的Partition的节点都是受到影响的brokers。假如Broker1上有三个Partition，这些Partition有些是Leader（p1）有些则是Follower（p2，p3），如果是Leader，则受影响的brokers要负责选出这个Replica对应的Partition的新Leader；如果是follower，也有可能影响了Partition的ISR，所以Leader要负责更新ISR。

[![](http://img.blog.csdn.net/20160311084942405 "k_affected_brokers")](http://img.blog.csdn.net/20160311084942405 "k_affected_brokers")

### 创建或删除topics：on_topic_change
1. 新创建一个topic，会同时指定Partition的个数，每个Partition都有AR信息写到ZK中
2. 为新创建的set_p 每个Partition P初始化leader：
    2.1 选择AR中的一个存活的Broker作为新Leader，ISR=AR
    2.2 将新Leader和ISR写到ZK的leaderAndISR节点
3. 发送LeaderAndISRCommand给受到影响的brokers
4. 如果是删除一个topic，则发送StopReplicaCommand给受影响的brokers

Controller会在/brokers/topics上注册Watcher，所以有新topic创建/删除时，Controller会通过Watch得到新创建/删除的Topic的Partition/Replica分配。对于新创建的Topic，分配给Partition的AR中所有的Replica都还没有数据，可认为它们都是同步的，也即都在ISR中（ISR=AR），任意一个Replica都可作为Leader。

创建或删除topic的过程和onBrokerFailure类似都要经过三个步骤：1) 选举Leader和ISR；2) 将leaderAndISR写到ZK中；3) 将最新leaderAndISR的LeaderAndISR指令发送给受到影响的Brokers。这是因为brokerChange导致Partition的Leader或者ISR发生变化，而新创建topic时，根本就没有Leader和ISR，所以两者都需要为Partition选择Leader和ISR。

[![](http://img.blog.csdn.net/20160313104935879 "k_broker_topic_change3")](http://img.blog.csdn.net/20160313104935879 "k_broker_topic_change3")

### Broker处理Controller发送的Command**

`on_LeaderAndISRCommand`

1. 读取命令中的set_P
2. 对每个partition P
    2.1 如果P在本地不存在，调用startReplica()创建一个新的Replia
    2.2 指令要求这个Broker成为P的新Leader，调用becomeLeader
    2.3 指令要求这个Broker作为Leader l的follower，调用becomeFollower
3. 如果指令中有INIT标记，则删除不在set_p中的所有本地partitions

对于新创建的Partition，由于Replica都还没有在Broker上被创建，所以有可能指令中Partition的本地文件不存在，就需要新创建一个Replica。而如果是Broker失败发送过来的LeaderAndISRCommand，则一般受到影响接收LeaderAndISRCommand指定的Broker是有存在Replica的，那么指令里就会要求这个Broker上的副本要么成为Leader，要么成为Follower。比如上面A的图例中，Broker1宕机，其上的p1副本是Leader，现在集群中p1没有leader了，因此指令就会要求broker2的p1副本由原先的follower要转变为Leader了。

becomeLeader指的是当前接收LeaderAndISRCommand指令的Broker，原先是一个follower，现在要转变为Leader。由于作为Follower期间，它会从Leader抓取数据，而现在Leader不在了，所以首先要停止抓取数据线程。follower转变为Leader之后，要负责读写数据，所以要启动提交线程负责将消息存储到本地日志文件中。

```scala  
becomeLeader(r: Replica, command) {
 stop the ReplicaFetcherThread to the old leader
 //after this, no more messages from the old leader can be appended to r
 r.partition.ISR = command.ISR
 r.isLeader = true //enables reads/writes to this partition on this broker
 start a commit thread on r
}

```
注意有可能新leader的HW会比之前的leader的HW要落后，这是因为新leader有可能是ISR，也有可能是AR中的replica。而原先作为Follower的replica，它的HW只会在向Leader发送抓取请求时，Leader在抓取响应中除了返回消息也会附带自己的HW给follower，Follower收到消息和HW后，才会更新自己的replica的HW，这中间有一定的时间间隔会导致Follower的HW会比Leader的HW要低。因此Follower在转变为Leader之后，它的HW是有可能比老的Leader的HW要低的。如果在leader角色转变之后，一个消费者客户端请求的offset可能比新的Leader的HW要大（因为消费者最多只消费到Leader的HW位置，但是消费者并不关心Leader到底有没有变化，所以如果旧的Leader的HW=10，那么客户端就可以消费到offset=10这个位置，而Leader发生转变后，HW可能降低为9，而这个时候客户端继续发送offset=10，就有可能比Leader的HW要大了！）。这种情况下，如果消费者要消费Leader的HW到LEO之间的数据，Broker会返回空的集合，而如果消费者请求的offset比LEO还要大，就会抛出OffsetOutofRangeException（LEO表示的是日志的最新位置，HW比LEO要小，客户端只能消费到HW位置，更不可能消费到LEO了）。

对于要转变为Follower的replica，原先如果是Leader的话，则要停止提交线程，由于当前Replica的leader可能会发生变化，所以在开始时要停止抓取线程，在最后要新创建到Replica最新leader的抓取线程，这中间还要截断日志到Replica的HW位置。

```scala 
becomeFollower(r: Replica) {
 stop the ReplicaFetcherThread to the old leader 
 r.isLeader = false 
  //disables reads/writes to this partition on this broker
 stop the commit thread, if any
 truncate the log to r.hw
 start a new ReplicaFetcherThread to the current leader of r, from offset r.leo
}

```
startReplica表示启动一个Replica，如果不存在Partition目录，则创建。并启动Replica的HW checkpoint线程，我们已经知道了Follower的HW是通过发送抓取请求，接收应答中包含了Leader的HW，设置为Follower Replica的HW（而Leader的HW又是由ISR提交来决定的，所以说ISR决定了HW能够增加，而Follower的HW则来自于Leader的HW）。

```scala  
startReplica(r: Replica) {
 create the partition directory locally, if not present
 start the HW checkpoint thread for r
}

```


下表是Broker收到LeaderAndISR指令后的动作，角色变化，以及线程的变化：

| action | state transition | thread |
| :-: | :-: | :-: |
| startReplica | new replica | start HW checkpoint |
| becomeFollower | leader to follower | stop commit thread,start fetch thread |
| becoleLeader | follower to leader | stop fetch thread,start commit thread |

`on_StopReplicaCommand`

Replica既然可以start，也可以被stop掉，即通过StopReplicaCommand指令要求Broker停止掉Replica。

1. 从指令中读取partitions集合
2. 对每个partition P的每个Replica ，调用stopReplica
    2.1 停止和r关联的抓取线程（当然针对的是Follower Replica）
    2.2 停止r的HW checkpoint线程
    2.3 删除partition目录

### Controller Failover：on_controller_failover

由于Controller是从Brokers中选举出来的，所以Controller所在的节点也会作为Partition的存储节点的。当Controller挂掉后，Controller本身作为Broker也会触发新的Controller调用on_broker_change。但是在还没有选举出新的Controller之前，挂掉的Broker的on_broker_change不会被新的Controller调用（因为根本就没有可用的Controller）。所以对于挂掉的Controller节点，最紧迫的任务是首先选举出新的Controller，然后再由新的Controller触发挂掉的那个Controller的on_broker_change。

[![](http://img.blog.csdn.net/20160313104859597 "k_onControllerFailover")](http://img.blog.csdn.net/20160313104859597 "k_onControllerFailover")

还有一点Broker失败和Controller失败是不同的：Broker的failover是在Controller端处理的，因为我们知道Broker挂掉了，Controller负责在挂掉Controller和受影响的Broker之间更新数据（将新的leaderAndISR发送给受影响的Broker）。而Controller的failover则是在Broker处理的（成功创建Controller的那一个Broker）。实际上理解起来也很简单，任何角色A会在ZK上注册节点信息，并有另外的角色B负责监听，当角色A挂掉后，会由节点的监听器B处理A的failover。

因为Controller是集群中各种事件状态变化的中央控制器，比如Controller负责将LeaderAndISRCommand/StopReplicaCommand指令发送给集群中的brokers。因为Controller存在的作用就是负责处理某个Broker的事件变化，转换为请求，发送给其他的Broker，而Broker之间是不需要直接通信的。因此如果Controller这个大脑发生故障了，那么就等于各个Broker之间无法通信了。举个例子公司中开发某个项目并不需要所有人都围坐在一起从方案到需求到设计，通常在不同阶段有不同的人开不同的会议，不过每个会议都要求有一个项目组长（Controller）参与。在需求会议，项目组长收集需求人员的需求（源事件），项目组长整理需求后，在设计会议上，他会召集开发人员进行设计评审并分配不同的任务给不同的开发人员（由Controller分配任务给不同的Broker），可是有一天项目组长请假了，需求人员和开发人员就不知道如何通信了，因为他们之间的媒介无法连接了。那么怎么办呢，可以引入一个共享存储ZooKeeper：Controller将状态改变的事件都存储在ZooKeeper上确保数据不会丢失，这样即使Controller挂掉了，在新的Controller掌权之后，也能够从ZooKeeper中读取出所有事件，这种方式跟v2版本的Replica设计中ZK的state change队列（存储的是每个broker的state change requests）是类似的。但是真的需要为此再引入ZKQueue吗？实际上只要Controller节点保存的数据是无状态的，那么切换新的Controller之后能够恢复之前的集群状态信息就无需引入ZK队列。

1. 创建`/controller->当前broker_id`的ZK节点，如果不成功则返回，表示竞选失败
2. 读取ZK中每个Partition的leaderAndISR节点数据
3. 对每个Partition都发送LeaderAndISRCommand（带上INIT标记位）给相关的Brokers
4. 触发调用on_broker_change()
5. 对没有leader的Partition，调用init_leaders(set_p)，这里跟创建topic时类似
6. 调用on_partitions_reassigned()重新分配Partition
7. 调用on_partitions_add()创建Partition
8. 调用on_partitons_remove()删除Partition

第一步创建/controller节点，监听了这个节点的所有brokers都想要创建，但是只有一个Broker节点才能创建成功。on_controller_failover表示之前的Controller已经挂掉了，要在监听的broker并且成功创建/controller的节点处理旧的controller的failover（故障转移处理）。Controller挂掉时，它之前创建的/controller会被自动删除掉（并不在这个方法里处理，而是由ZK在controller进程挂掉时自动删除），所以就让其他一直默默潜水的brokers有机会成为新的Controller。

### Broker Startup：on_broker_startup
由于broker不管是启动还是发生故障，都会操作ZK中的/brokers/ids节点，而Controller牢牢地监控了这个节点的变化情况。当Broker刚启动时（这里指的是failover中的startup，如果说启动一个全新的broker，它上面是没有Replica的，而failover是之前存在Replica，发生故障后恢复过来的startup上面仍然有Replica的），为了让它所拥有的Replica能够正常提供服务，需要startReplica，同时其上的每个Replica要么成为Leader，要么成为Follower。

[![](http://img.blog.csdn.net/20160311091731316 "k_on_broker")](http://img.blog.csdn.net/20160311091731316 "k_on_broker")

在on_broker_change里，Controller会将失败Broker上的Partition的指令发送给受影响的brokers（肯定不能发送给失败的Broker了），收到指令的brokers会对自己所拥有的Replica判断是否成为Leader或者Followr。而on_broker_start则是Controller直接处理刚刚启动的Broker。即on_broker_start是通过监听器直接处理，而on_broker_change是通过Controller发送命令给brokers处理，对于Broker而言都是针对Partition的Replica在Leader和Follower之间进行角色转换操作。

[![](http://img.blog.csdn.net/20160312092301167 "k_broker_change_start")](http://img.blog.csdn.net/20160312092301167 "k_broker_change_start")

1. 读取ZK中topic路径下每个topic所有Partition的AR（和partition路径的AR功能类似）
2. 读取ZK中分配给当前启动的Broker的每个Partition的leader和ISR（leaderAndISR）
3. 对分配给当前Broker的每个Replica
    3.1 start replica
    3.2 这个Broker是这个Partition的leader，becomeLeader
    3.3 这个Broker是这个Partition的follower，becomeFollower
4. 删除不再分配给当前Broker的本地Partitions

最后一步Broker在启动的时候需要删除不属于自己的partitions。这种场景可能是在这个broker当掉的时候，属于这个broker的topic被删除后又立马被重新创建。在删除topic的时候，所有broker都应该把这个topic的所有partition都删除掉，但是如果broker挂掉了，是无法进行任何操作的。在重新创建topic的时候，也不会将partition分配给当掉的broker，而只是分配partition给其他存活的broker。所以当掉的broker恢复正常后，应该把在删除topic时候的那些partition都删除掉。尽管又重新创建了topic，但是当掉broker上的partition是不能作为新创建的topic的partitions的，因为在当掉期间ZK中根本就不会记录属于当掉broker的partition分配信息。

### 讨论
`broker失败期间端到端的延迟`

1. broker关闭(关闭socket server后，需要关闭请求处理器和log)
2. controller上的broker监听器被触发
3. controller负责leadership变更，将新leader和isr发布到zk（每个受影响的partition都写一次zk）
4. 通过写到`ZKQueue`中通知每个broker leadership变化(每个broker写一次关于leader变化事件的zk)
5. 新leader等待isr中的follower成功连接上(Kafka RPC)
6. follower首先截断日志，然后开始从leader抓取数据

端到端的延迟指的是从Producer提交消息到这条消息被Consumer/Follower消费到的时间间隔。最耗时的是步骤三：对每个partition都要将新的leaderAndISR写一次zk。假设在broker发生故障时需要为10K个partition改变leader，每次zk写花费4ms，则总共会花费40s。不过可以使用zk中的multi()将所有Partition的写操作用批处理就只需要一次写。

步骤四中：controller会将最新的leadershipi变化事件通知给每个broker。在Replica的版本2中对于state change事件则采用队列的形式，对于leader的变化事件是通过监听器的方式通知注册在leader的所有broker节点。现在由于leader的管理交给了Controller，所以可以使用直接RPC代替监听器和队列。

`ZKQueue vs 直接RPC`

通过ZK完成Controller和brokers的通信是非常低效的，因为每次通信都需要2次ZK写（每个写操作花费2个RPC），一次监听器的触发（写之后数据改变触发监听器调用），一次ZK读（监听器要获取最新的写操作结果），所以对于一次通信就花费了6次RPC（2*2+1+1=6，RPC指的是在不同节点之间的通信，controller和brokers是在不同节点上，需要通过RPC通信才能读写数据）。

如果controler和所有的brokers之间为了能够直接通信，在Broker端实现一个admin RPC，那么每次通信只需要一个RPC。使用RPC意味着当一个broker当掉后，它可能会丢失一些来自Controller的命令，所以在broker启动时它需要读取zk中保存的状态信息来恢复自己的状态。

`如何处理多个leader?`

存在这样一种情况：有多个broker同时声称自己是某个partition的leader副本。比如broker A是partition的初始Leader，partition的ISR是{A,B,C}。然后broker A因为GC失去它在ZK中的注册信息。这时controller会认为broker A当掉了，并将partition的leader分配给了broker B，设置新的ISR为{B,C}，并写到ZK中（每次partition的leader发生变化，epoch也会增加）。在broker B成为leader的同时，broker A从GC中恢复过来，但是还没有收到controller发送的leadershipi变化指令。现在broker A和B都认为自己是leader，如果我们允许broker A和B能同时提交消息那就非常不幸了，因为所有副本之间的数据同步就会不一致了。不过当前的设计中实际上是不会允许出现这种情况的：**当broker B成为leader之后，broker A是无法再提交任何新的消息的**。

假设允许存在两个leader，生产者会同时往这两个leader写数据，但是能不能提交消息就类似于两阶段提交协议了：kafka的leader要能够提交一个消息的保证是ISR中的所有副本都复制成功。对于broker A为了保证能投提交消息m，它需要ISR中的每个副本（A,B,C）都要接收到消息m，而这个时候broker A仍然认为ISR是{A,B,C}，（这是broker A的一份本地拷贝，虽然这个时候ZK中的ISR已经被broker B改变了），但是ISR中的broker B是不会再接收到消息m的。因为在broker B成为Leader的时候，它会首先关掉到之前旧的broker A的数据抓取线程（broker B之前是follower，会向leader抓取数据，只有follower抓取数据，leader才能判断消息是否能提交），因为broker B的抓取线程被关闭了，broker A可能会认为B无法赶上Leader，既然因为B受到影响不能提交消息m，broker A干脆就想要把B从ISR中移除，这个时候broker A要将自己认为的最新的ISR写到ZK中。不过不幸的是broker A并不能完成这个操作：因为在写ZK的时候broker A会发现自己的epoch版本和ZK中的当前值并不匹配（broker B在选举为Leader之后会写到ZK中，并将epoch增加1，任何新的写操作的epoch都不能比当前epoch小），直到这个时刻，broker A才意识到它已经不再是partition的leader了（举个例子：一个团伙中大领导A突然消失了一段时间，众人在这段时间里选举出了新的领导B，并且向所有的成员都发话了，只有B的指令是管用的。在A回来时，没有人告诉他B已经成功上位，A还傻傻地认为自己还是最高BOSS，不过当他要发布指令的时候，发现每个人的指令只接受B的了，这时A才意识到MD自己已经被踢出局了）。

[![](http://img.blog.csdn.net/20160312113420173 "k_2leader")](http://img.blog.csdn.net/20160312113420173 "k_2leader")

`broker故障客户端如何路由`

controller首先将受影响的partitions的新leader写到zk的leaderAndISR节点，然后才会向brokers发送leader改变的命令，brokers收到命令后会对leader改变的事件作出响应。由于客户端请求会使用leaderAndISR的数据来连接leader所在的节点，客户端的请求被路由到新leader所在的broker节点，但如果那个broker还没有准备好成为leader，就存在一个时间窗口对于客户端而言是不可用的。HBase只在regionserver响应了打开或关闭region的命令之后才更新元数据（这个元数据也用来响应客户端的请求，即客户端可以读取哪些region），而kafka这里则是controller先更新元数据（写入leaderAndISR）后才发送命令给broker，因此可能元数据已经更新，但是broker还没收到命令，或者收到命令后还没准备好成为leader。

[![](http://img.blog.csdn.net/20160312132504155 "k_client_route")](http://img.blog.csdn.net/20160312132504155 "k_client_route")

所以可能你会认为不要让controller先更新leaderAndISR节点，而是发送指令给brokers，每个broker在收到指令并处理完成后才，让每个broker来更新这个节点的数据。不过这里让controller来更新leaderAndISR节点是有原因的：我们是依赖ZK的leaderAndISR节点来保持controller和partition的leader的同步。当controller选举出新的leader之后，它并不希望ISR（新leader的ISR）被当前的leader（旧的leader）改变。否则的话（假设ISR可以被旧leader改变），新选举出来的leader在接管正式成为leader之前可能会被当前leader从ISR中剔除出去。通过立即将新leader发布到leaderAndISR节点，controller能够防止当前leader更新ISR（选举新的leader在写到ZK时，epoch增加，而如果当前leader想要更新ISR比如将新选举的leader从ISR中剔除掉，因为epoch不匹配，所以当前leader就不再有机会更新ISR了）。

一种方式是使用额外的ZK路径比如ExternalView来路由客户端请求解决这种短暂的不可用窗口。controller只会在broker响应leader改变的指令之后才更新ExternalView。对所有的partitions使用一个ExternalView或者每个partition都使用一个ExternalView各有利弊。前者对ZK有更少的负载，但是可能会强制客户端触发不必要的重新平衡。

正常情况下，leadership改变的指令会很快地被执行，所以在转换的这段时间里，我们可以依赖于客户端的重试机制，相对而言客户端的代价更小。即使broker成为新的leader花费了太长的时间，controller也会得到超时通知并触发一条告警信息通知admin。

`leadership改变时，offset超过HW`

通常情况下，follower的HW总是落后于leader的HW。所以在leader变化时，消费者发送的offset中可能会落在新leader的HW和LEO之间（因为消费者的消费进度依赖于Leader的HW，旧Leader的HW比较高，而原先的follower作为新Leader，它的HW还是落后于旧Leader，所以消费者的offset虽然比旧Leader的HW低，但是有可能比新Leader的HW要大）。如果没有发生leader变化，服务端会返回OffsetOutOfRangeException给客户端。但这种情况下如果请求的offset介于HW和LEO之间，服务端会返回空消息集给消费者。

[![](http://img.blog.csdn.net/20160312135626735 "k_client_hw_out")](http://img.blog.csdn.net/20160312135626735 "k_client_hw_out")

## 小结
当前的KafkaController实现是多线程的控制器，并且模拟了状态机，它维护的状态有：

* 在每个节点上维护所有partitions的Replicas
* 所有partitions的Leaders

引起状态改变的输入事件有：

1. 注册在ZooKeeper上的监听器
    1) BrokerChangeListener（Broker挂掉）
    2) DeleteTopicListener（删除Topic）
    3) TopicChangeListener（更改Topic）
    4) AddPartitionsListener（为Topic新增加Partition）
    5) PartitionReassignedListener（Admin）
    6) PreferredReplicaElectionListener（Admin）
    7) ReassignedPartitionsIsrChangeListener
2. 到brokers的连接通道（controller被关闭）
3. 内部的调度任务（prefered leader选举）

## Ref

* [https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Controller+Internals](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Controller+Internals)



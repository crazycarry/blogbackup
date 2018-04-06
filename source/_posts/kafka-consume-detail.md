---
title: Kafka消费者重新实现的细节
date: 2018-04-04 19:20:41
tags: 
     - 源码分析
     - 消息中间件
     - 大数据
     - 架构设计
categories: kafka
---
上一章分析的消费者高级API使用ConsumerGroup的语义管理多个消费者，但是在消费者或者Partition发生变化时都需要rebalance，它的实现对ZooKeeper依赖比较严重，
由Kafka内置实现了失败检测和Rebalance(ZKRebalancerListener)，但是它存在羊群效应和脑裂的问题，客户端代码实现低级API也不能解决这个问题。如果将失败探测和Rebalance的逻辑放到一个高可用的中心Coordinator，这两个问题即可解决。同时还可大大减少Zookeeper的负载，有利于Kafka Broker的扩展(Broker也会作为协调节点的角色存在)。
<!-- more -->

协调节点在前面分析Consumer的Offset(fetchOffsets和commitOffsets)分析过GroupCoordinator的处理逻辑。不过新消费者KafkaConsumer有自己的协调者ConsumerCoordinator。高级API使用ZookeeperConsumerConnector，其中的offset相关fetch和commit API，以及数据抓取线程对于新消费者都需要重新实现，ConsumerCoordinator作为新消费者KafkaConsumer的一部分用java代码重新实现了这些API。服务端的GroupCoordinator对新旧API都适用的。

[![](http://img.blog.csdn.net/20160228101305540 "k_consumer_new_old")](http://img.blog.csdn.net/20160228101305540 "k_consumer_new_old")

ConsumerCoordinator是KafkaConsumer的一个成员变量，所以每个消费者都要自己的ConsumerCoordinator，消费者的ConsumerCoordintor只是和服务端的GroupCoordinator通信的介质，下文中提到的**协调者一般指的是服务端的GroupCoordinator**。每个KafkaServer都有一个GroupCoordinator实例，服务端的GroupCoordinator管理消费组成员和offset，它可以管理多个消费组（因为Broker本身即使存储一个topic的消息，也可以被不同的消费组订阅）。注意：组成员的状态管理（比如GroupMetadata）是在服务端的GroupCoordinator完成的，而不是由消费组的ConsumerCoordinator完成（因为消费者只能看到自己的，无法看到和自己同组的其他成员）。

### 消费组管理协议

在同一个消费组里多个consumer实例需要进行平衡操作。消费组会注册感兴趣的topics。这个消费组中的所有消费者会互相协调，每个消费者会互相拥有独一的partition集合。即同一个partition只会分配给消费组中的一个消费者。一个消费者可以有多个partition。当消费组成功平衡后，所有注册的topics的每个partition都会被唯一的消费者拥有(每个partition都会被分配给消费者去消费)。每个Broker节点会被选举为一部分消费组的协调节点，消费组的协调节点负责在组成员变化，或者注册的topics的partition变化时进行协调。协调节点同时负责在平衡操作时，将partition的所有权配置信息(partition分配给哪个消费者)在所有consumers之间进行交流。

**Consumer消费者的工作过程：**

* 1.在启动时或者协调节点故障转移时，消费者发送ConsumerMetadataRequest给bootstrap brokers列表中的任意一个brokers。在ConsumerMetadataResponse中，它接收消费者对应的消费组所属的协调节点的位置信息。
* 2.消费者连接协调节点，并发送HeartbeatRequest。如果返回的HeartbeatResponse中返回IllegalGeneration错误码，说明协调节点已经在初始化平衡。消费者就会停止抓取数据，提交offsets，发送JoinGroupRequest给协调节点。在JoinGroupResponse，它接收消费者应该拥有的topic-partitions列表以及当前消费组的新的generation编号。这个时候消费组管理已经完成，消费者就可以开始抓取数据，并为它拥有的partitions提交offsets。
* 3.如果HeartbeatResponse没有错误返回，消费者会从它上次拥有的partitions列表继续抓取数据，这个过程是不会被中断的。

[![](http://img.blog.csdn.net/20160228101325759 "k_consumer_flow")](http://img.blog.csdn.net/20160228101325759 "k_consumer_flow")

**Co-ordinator协调节点的工作过程：**

* 1.在稳定状态下，协调节点通过`故障检测协议`跟踪每个消费组中每个消费者的健康状况。
* 2.在选举和启动时，协调节点读取它管理的消费组列表，以及从ZK中读取每个消费组的成员信息。如果之前没有成员信息，它不会做任何动作。只有在同一个消费组的第一个消费者注册进来时，协调节点才开始工作(即开始加载消费组的消费者成员信息)。
* 3.当协调节点完全加载完它所负责的消费组列表的所有组成员之前，它会在以下几种请求的响应中返回CoordinatorStartupNotComplete错误码：HeartbeatRequest，OffsetCommitRequest，JoinGroupRequest。这样消费者就会过段时间重试(直到完全加载，没有错误码返回为止)。
* 4.在选举或启动时，协调节点会对消费组中的所有消费者进行故障检测。根据故障检测协议被协调节点标记为Dead的消费者会从消费组中移除，这个时候协调节点会为Dead的消费者所属的消费组触发一个平衡操作(消费者Dead之后，这个消费者拥有的partition需要平衡给其他消费者)。
* 5.当HeartbeatResponse返回IllegalGeneration错误码，就会触发平衡操作。一旦所有存活的消费者通过JoinGroupRequests重新注册到协调节点，协调节点会将最新的partition所有权信息在JoinGroupResponse的每个消费者之间通信(同步)，然后就完成了平衡操作。
* 6.协调节点会跟踪任何一个消费者已经注册的topics的topic-partition的变更。如果它检测到某个topic新增的partition，就会触发平衡操作。当创建一个新的topics也会触发平衡操作，因为消费者可以在topic被创建之前就注册它感兴趣的topics。

从上面两者的工作过程，我们大致知道了协调节点负责管理消费组中的消费者。而消费者会和协调节点通信。如果协调节点发生故障转移，则消费者需要寻找新的协调节点。如果协调节点检测到消费者发生了故障，则协调节点负责平衡操作。

### 故障检测协议

消费者在加入到消费组时，发送给协调者的JoinGroupRequest设置了session timeout。当消费者成功加入到消费组后，在消费者和协调者都会开始故障检测流程。消费者启动周期性的心跳(发送HeartbeatRequest)，每隔session.timeout.ms/heartbeat.frequency发送给协调者并等待响应。

* session.timeout 会话超时的最大时间，超过这个时间，消费者和协调者都会认为对方挂掉了
* heartbeart.frequency 心跳频率，时间除于次数表示每一次心跳的时间间隔，间隔越短越容易发生rebalance

如果协调者在session.timeout没有收到消费者的心跳请求，它会标记消费者为死亡状态。同样如果消费者在session.timeout内没有收到心跳响应，它会假设协调者挂掉了，消费者会启动重新发现协调者的流程(每个协调者只管理一部分消费者，一个消费者只被一个协调者管理，协调者是brokers中的一个)。

heartbeat.frequency(心跳频率)是消费者端的配置，它决定了消费者发送一次心跳给协调者的时间间隔。这个值是rebalance延迟的最低临界值，因为协调者是根据心跳响应通知消费者，进行rebalance操作的。(为什么心跳频率和rebalance有关，因为心跳和session.timeout有关，超时后会触发rebalance)。所以如果session.timeout.ms设置的非常大时，也要将心跳频率设置为相对有意义的比较大的值。当然也不能将心跳频率设置的太高(结果是心跳时间间隔很短)，导致brokers的负载太重了。

* 1.在接收到ConsumerMetadataResponse或JoinGroupResponse后，消费者周期性地发送HeartbeatRequest给协调者。
* 2.协调者在收到HeartbeatRequesst时，首先检查generation id，消费者编号和消费组。如果消费者指定了一个无效或过期的generation id，协调者会发送带有IllegalGeneration错误码的HeartbeatResponse给消费者。
* 3.如果协调者在session timeout没有收到消费者的心跳请求，标记消费者挂掉，并触发消费组的rebalance流程。
* 4.如果消费者在session timeout没有收到协调者的心跳响应，认为协调者失败，并触发重新发现协调者的流程。

当协调者发生故障时，消费者发现新的协调者的顺序可能发生在新的协调者完成故障处理(包括从zk中加载消费组元数据等)之前或之后。如果在完成故障处理之后才发现新的协调者，新的协调者就会像之前一样接收消费者的心跳请求。而如果是在之前，新的协调者则会拒绝消费者的心跳请求，会导致消费者重新发现协调者，并重新连接协调者。如果消费者太晚连接新的协调者，协调者可能会标记消费者挂掉了，消费者再次加入时，会认为这是一个新的消费者，并触发rebalance。

> 消费者发现新的协调者(co-ordinator re-discovery)，包括两个步骤，首先确定新的协调者，然后消费者连接协调者。如果新的协调者确定了，并且消费者成功连接上新协调者，这样消费者发送的心跳请求就会被新的协调者正常接收。但是如果新协调者已经确定，而消费者并没有连接上新的协调者，消费者发送的心跳请求并不会被接收：因为连接都还没有建立!

### 状态图

**消费者状态机**

[![](https://cwiki.apache.org/confluence/download/attachments/38570548/Consumer%20state%20diagram.jpg?version=10&modificationDate=1400109502000&api=v2 "consumer-state")](https://cwiki.apache.org/confluence/download/attachments/38570548/Consumer%20state%20diagram.jpg?version=10&modificationDate=1400109502000&api=v2 "consumer-state")

* `Down`：消费者进程挂掉了。
* `Start up & discover co-ordinator`：在这个状态时，消费者为所属的组发现协调者。消费者一旦发现协调者后就会发送JoinGroupRequest(没有consumer id信息，表示消费组)。如果同一组中的其他消费者指定了和当前消费者存在冲突的partition分配策略。当前消费者就可能接收到InconsistentPartitioningStrategy错误码的响应。如果策略名称不被Brokers识别，会收到UnknownPartitioningStrategy错误码。这种情况消费者无法加入到消费组。
* `Part of a group`：如果收到的JoinGroupResponse没有错误码，有consumer id以及为整个组生成的generation id。消费者就会成为组的一个成员。这个状态下，消费者会发送HeartbeatRequest，根据心跳响应结果的错误码，它可以继续在当前状态，或者移动到Stopped Consumption或者Rediscover co-ordinator的状态。
* `Re-discover co-ordinator`：这个状态下，消费者并没有停止消费，但是会发送GroupCoordinator来尝试重新发现协调者，并且等待响应，直到收到没有错误码的响应(响应结果中会返回新发现的协调者)。
* `Stopped consumption`：消费者停止消费消息，然后提交offset，直到重新加入消费组中才会继续开始消费消息。

**协调者状态机**

[![](https://cwiki.apache.org/confluence/download/attachments/38570548/Coordinator%20state%20diagram.jpg?version=5&modificationDate=1399498790000&api=v2 "coordinator-state")](https://cwiki.apache.org/confluence/download/attachments/38570548/Coordinator%20state%20diagram.jpg?version=5&modificationDate=1399498790000&api=v2 "coordinator-state")

* `Down`：协调者进程挂掉了
* `Catch up`：协调者被选举出来了，但还还没有开始提供服务
* `Ready`：新选举出来的协调者已经完成加载它所负责的消费组的组元数据
* `Prepare for rebalance`：协调者发送IllegalGeneration的心跳响应给组中的所有消费者，并等待消费者发送JoinGroupRequest
* `Rebalancing`：协调者当前generation中收到消费者的JoinGroupRequest，然后增加group的generation id，并且为请求的消费者分配consumer ids(下面说到分配过程)，以及完成partition的分配(将partiton分配给消费者)
* `Steady`：协调者接受每个消费组的所有消费者发送的OffsetCommitRequest和心跳信息。

### Consumer id的分配

* 1.消费者启动后，从协调者接收到的第一次JoinGroupResponse中有consumer id。从这里开始，消费者的每次心跳以及提交offset请求都必须要包含这个consumer id(作为一种标识，比如员工入职后分配了胸卡，你以后上班就都要佩戴胸卡了)。如果协调者收到的HeartbeatRequest和OffsetCommitRequest其中的consumer id和组中的任何一个consumer ids都不同，协调者就会在对应的响应信息中发送带有UnknownConsumer错误码的响应给发起请求的消费者。
* 2.协调者在成功rebalance时，会为消费者分配一个consumer id，返回在JoinGroupResponse中返回给消费者。消费者可以选择在接下来的JoinGroupRequest中包含这个id，直到消费者被关闭或者挂掉了。带上id的好处是可以降低rebalance操作的延迟，当rebalance触发时，协调者会等待在上一个generation id的所有消费者发送JoinGroupRequest。协调者定位一个消费者是通过它的consumer id。如果消费者选择不带consumer id的JoinGroupRequest，协调者只能等待完全的session timeout才能继续剩下的rebalance操作。这是因为没有办法将不带consumer id的JoinGroupRequest和一个不存在的consumer id的消费者映射起来(请求中没有带consumer id就没办法确定consumer id是否存在，因为无法比较)。而如果(每个)消费者发送的JoinGroupRequest带了consumer id，协调者就能立即确定这个消费者是不是存在，并且能在所有已知的消费者都发送JoinGroupRequest后，完成本次rebalance操作(而不需要等待session timeout才最终完成)。
* 3.协调者会在接收到一个消费组中所有存在的消费者发送了一个JoinGroupRequest之后开始分配consumer id。它会为JoinGroupRequest中没有consumer id的每个消费者分配新的group-uuid。前提是这样的消费者是刚刚启动的或者没有选择发送之前分配给它的consumer id。
* 4.如果消费者发送的JoinGroupRequest带了consumer id，但是不匹配当前组成员的ids，协调者会在JoinGroupResponse中返回UnknownConsumer错误码，避免这个消费者加入到不认识的消费组中。这也不会触发组中其他消费者的rebalance操作。

### 协议格式

对于每个消费组，协调者会存储以下信息：
1) 对每个存在的topic，可以有多个消费组订阅同一个topic(对应消息系统中的广播)
2) 对每个消费组，元数据如下：

* 消费组订阅的topics列表
* Group配置信息，包括session timeout等
* 组中每个消费者的元数据。消费者元数据包括主机名，consumer id
* 每个正在消费的topic partition的当前offsets
* Partition的ownership元数据，包括consumer到分配给消费者的partitions映射

[![](http://img.blog.csdn.net/20160224164856609 "k_protocol")](http://img.blog.csdn.net/20160224164856609 "k_protocol")

### 其他故障场景

**协调者故障或者到连接协调者失败**

* 1.协调者发生故障时，控制器会为受到影响的消费组子集选举出新的leader/协调者。作为成为offset-topic-partitions的leader，协调者从zookeeper中读取它负责的每个消费组的元数据。每个消费组的元数据包括了group的consumer ids，generation id，订阅的topics列表。在协调者从zk中读取所有的元数据之前，发送给消费者的心跳响应带有CoordinatorStartupNotComplete错误码。在这段时间如果消费者发送JoinGroupRequest是不合法的，此时返回消费者的错误码是IllegalProtocolState。
* 2。Broker发送UpdateMetadataRequest给Controller，它在接收到更新的group metadata之前，如果消费者发送了ConsumerMetadataRequest给这个Broker，响应结果会返回协调者过期的信息。这种情况下，消费者发送的心跳和offset提交就会收到错误为NotCoordinatorForGroup的响应结果。所以消费者应该退回重来，即重新发送ConsumerMetadataRequest(确保在update group metadata之后)。

**订阅的topics的partition变化**

* 1.消费组对应的协调者负责检测订阅的topics的partitions数量的变化，一旦partitions数量发生变化。
* 2.协调者标记消费组准备rebalance，此时如果消费者有心跳，返回IllegalGeneration错误码(因为即将新一轮的平衡)，同时消费者会停止抓取数据(平衡要开始了，大家不要拿数据)，并提交offset(先保存下状态)，然后发送JoinGroupRequest给协调者。
* 3.协调者等待这个组中所有的消费者都给它发送了JoinGroupRequest(大家都签到后才能开始哈)，然后会在zk中增加group的generation id(通知zk现在进入了一个新纪元)，计算新的partition分配(为每个人都重新分口粮)，最后在JoinGroupResponse中返回更新的partition分配信息，以及新的generation id(通知消费者完成了)。注意即使组成员没有变化，generation也会增加，即每次发生rebalance都会增加generation id(类似zk的epoch)。
* 4.消费者收到JoinGroupResponse，它会在本地存储generation id和自己的consumer id，然后为返回的重新分配到的partitions开始抓取数据。在这之后消费者发送给协调者的请求会使用这个新的generation id以及consumer id，这两个id都是上一次的JoinGroupResponse的返回信息。

**在rebalance时的offset提交**

上面我们看到消费组开始rebalance时，消费者会停止抓取数据，提交offset。其中提交offsets是为了保存状态信息。

* 1.如果消费者收到IllegalGeneration错误码(表示当前组正在rebalance)，它会在发送JoinGroupRequest给协调者之前停止抓取数据，并提交已经存在的offsets(发送JoinGroupRequest是rebalance的一部分工作，而停止抓取则是前提条件)。
* 2.协调者会检查OffsetCommitRequest中的generation id，如果请求中的generation id比协调者的值要高就会被拒绝。
* 3.协调者不允许消费者发送的OffsetCommitRequest中的generation ids比zk中当前组的generation id要旧。在rebalance时该约束没有问题，因为在所有消费者发送JoinGroupRequest之前，协调者不会增加zk中group的generation id。当协调者增加了generation id之后，在还没有发送JoinGroupResoponse之前，协调者并不期望收到OffsetCommitRequest(在当前最新的generation id里，因为还没有返回响应，组中任何消费者都不会发送最新generation的offset commit请求)。所以消费者发送的每个OffsetCommitRequest应该总是和协调者的当前generation id是匹配的。
* 4.当消费者遇到软件问题而失败，比如在协调者进行rebalance时，消费者发生了长时间的GC停顿，如果消费者停顿时间超过session timeout，协调者在session timeout时间内就不会收到消费者发送的JoinGroupRequest请求，会标记消费者挂掉。

**在rebalance时的heartbeats**

* 1.消费者每隔session.timeout.ms/heartbeat.frequency时间就周期性地发送心跳给协调者。如果消费者在心跳响应中收到IllegalGeneration错误码，它会停止抓取，然后提交offset，并向协调者发送JoinGroupRequest。在消费者收到JoinGroupResponse之前，它不会再向协调者发送任何的心跳请求。
* 2.设置更高的心跳频率可以确保更低延迟的rebalance操作(因为时间间隔变小，而rebalance是根据这个间隔而触发的)，因为协调者只有在HeartbeatResponse时才有可能触发消费者的rebalance操作(收到心跳响应后加入组就正式开始rebalance)。
* 3.当协调者收到消费者发送的JoinGroupRequest，在返回JoinGroupResponse给消费者之前，协调者会暂停对这个消费者的故障检测。当协调者把JoinGroupResponse发送出去时，就重新启动心跳计时器，如果在又一次的session timeout时间内没有收到这个消费者的心跳请求会标记这个消费者为Dead(即从JoinGroupResponse发送出去开始计时，在session timeout收到心跳请求才认为消费者正常)。协调者在rebalance时依赖于心跳而停止故障检测是由broker的socket server设计而决定的(协调者也是一个broker)。kafka只允许broker针对每个客户端一次只能读取或者处理一个未完成的请求(这是保证有序处理的简单做法)。这是为了防止对同一个客户端，消费者和broker同时处理心跳请求和join group请求。根据JoinGroupRequest来标记失败，防止协调者在rebalance操作时就将消费者标记为Dead。注意如果消费者在rebalance时遇到软件的问题而停顿，并不会阻碍rebalance操作的完成。如果消费者在发送JoinGroupRequest之前发生停顿，协调者会标记它Dead，然后完成rebalance操作，在新的generation中只包括其他的消费者(失联的那个消费者当然不会被包括在本次generation中了) 。如果消费者在发送JoinGroupRequest之后发生停顿，协调者在假设rebalance操作成功完成的情况下(这里generation包括了消费者)仍然会向它发送JoinGroupResponse，并且重新开始心跳计时器。如果消费者在session timeout之前就恢复了，它会和往常一样消费。如果在session timeout之后还处于停顿状态，它就会被协调者标记为Dead，然后又触发了一次rebalance操作。
* 4.协调者只在JoinGroupRequest中返回新的generation id和consumer id。一旦消费者接收到JoinGroupResponse，消费者在下一次发送HeartbeatRequest时附带上新的generation id和consumer id发送给协调者。

**在rebalance时的协调者故障**

rebalance操作会有多个阶段：

* 1.协调者收到rebalance的通知-可能在zk监视到topic/partition发生变化，新消费者注册，或者旧消费者挂掉。
* 2.协调者初始化rebalance操作，通过发送带有IllegalGeneration错误码的心跳响应给消费者(消费者发送了心跳请求)。
* 3.消费者发送JoinGroupRequest请求给协调者(在接收到心跳响应之后)。
* 4.协调者增加了zk中消费组的generation id，并在zk中写入新的partition ownership信息。
* 5.协调者发送JoinGroupResponse给消费者。

协调者可能在上面任何一个步骤失败，下面讨论了在每个步骤如果协调者失败了是怎么处理的。

* 1.协调者在步骤1失败：协调者在收到通知后，但是还没有机会做出反应就失败了，新的协调者为了完成故障处理需要有能力检测什么时候需要rebalance操作。(新)协调者会从zk中读取消费组的元数据，包括消费组订阅的topics列表以及之前的partition ownership。如果topics的数量或者订阅topics的partitions数量和之前的partition ownership决策(分配partition是一种决策)有出入，新的协调者就会认为需要为这个消费组开始进行一次rebalance操作。同样如果消费者连接到新的协调者和zk中group generation的元数据不同，协调者也会为这个消费组开始一次rebalance操作。
* 2.协调者在步骤2失败，它会发送带有错误码的HeartbeatResponse给一些消费者，但不是全部(挂掉之后当然无法在发送了)。和步骤1的失败类似，协调者会在失效备援(failover)后检测rebalance的需要并开始又一次rebalance操作(失败的协调者发生在它自己的rebalance时，而新的协调者接管后，也需要检测什么时候需要rebalance，所以它的rebalance叫做又一次)。 如果是因为一个消费者的失败而开始一次rebalance，但是消费者在协调者failover处理完成之前就恢复为正常状态，协调者不会又开始一次rebalance(如果消费者在session timeout后仍然没有恢复，协调者认为消费者dead，就又开始一次rebalance)。然而，如果只要有任意一个消费者向协调者发送一个JoinGroupRequest，协调者就会为整个消费组开始一次rebalance操作。
* 3.协调者在步骤3失败，它可能只会接收到消费组中部分consumers的JoinGroupRequest。在失效备援后，协调者可能会收到所有存活的消费者的HeartbeatRequest或者部分消费者的JoinGroupRequest。和步骤1类似，也会触发消费组的rebalance。
* 4.协调者在步骤4失败，它可能会在写入新的generation id和消费组成员到zk中后失败。generation id和成员信息是作为一个原子的zk写入操作。在失效备援后，消费者会发送旧的generation id的HeartbeatRequests给协调者。协调者比较消费者的心跳请求中的generation和zk不一致，就会返回错误码为IllegalGeneration的响应，让消费者重新发送JoinGroupRequest。所以在HeartbeatRequest和OffsetCommitRequest中附带generation id和consumer id是值得的。
* 5.协调者在步骤5失败，它可能会在发送JoinGroupResponse给消费组中的部分消费者后失败了。已经接收到JoinGroupResponse的消费者在要发送心跳或者提交offsets时会检测到失败的协调者。这时它会发现新的协调者，并向它以新的generation发送心跳。(新的协调者在这个时候会向消费者发送没有错误码的HeartbeatResponse。对于没有收到JoinGroupResponse的消费者也会发现新的协调者，并且向它发送JoinGroupRequest。这也同样会触发协调者为消费组触发rebalance操作。

**慢的消费者**

消费速度慢的消费者会被协调者从消费组中移除，比如协调者在session timeout时间内没有收到慢的消费者的心跳请求。典型的场景是如果消费者的消息处理速度比session timeout还要慢，会导致poll调用的时间间隔超过session timeout。由于心跳请求只会在poll调用时才会发送，这会导致协调者标记比较慢的消费者为Dead。协调者处理慢消费者的步骤：

* 1.如果协调者在session timeout没有收到心跳请求，它标记消费者dead，并且中断到消费者的socket连接。
* 2.同时协调者会将带有IllegalGeneration错误码的HeartbeatResponse发送给组中其他的消费组，并触发rebalance。
* 3.如果在协调者接收到其他任意一个消费者的HeartbeatRequest请求之前，慢的消费者先发送了HeartbeatRequest协调者会取消rebalance的尝试，并且返回没有错误码的HeartbeatResponse给慢的消费者(说明由慢状态渐渐好转了)
* 4.如果不是这种情况(其他消费者先发送心跳)，协调者继续rebalance，也向慢消费者发送IllegalGeneration错误码。
* 5.由于协调者只会等待存活的消费者的JoinGroupRequest，所以在它接收到其他消费者的join请求后，它说rebalance可以结束了。如果这时慢的消费者恰巧也发送了JoinGroupRequest(突然不慢了)，协调者会在当前generation里包括这个慢的消费者，如果除了这个慢的消费者外，协调者还没有发送一个JoinGroupResponse(是其他消费者都还没发送，还是什么情况?)。
* 6.如果协调者已经发送了JoinGroupResponse(向其他存活的消费者，而不是这个慢的消费者，因为慢的消费者才刚发送请求)，它会让这一轮的rebalance完成，然后又会紧接着触发下一次的rebalance(慢的消费者在这一轮上轮不上，得等到下一轮)。
* 7.如果当前这一轮的rebalance时间花的太长了，慢的消费者的JoinGroupResponse就会超时(因为慢的消费者只能等到其他消费者都接收完JoinGroupResponse之后，在第一轮rebalance结束之后，才会发送JoinGroupResponse给慢的消费者，而第一轮的rebalance耗费太长了，慢的消费者在session timeout内没有收到协调者发送的JoinGroupResponse而超时)，消费者会认为协调者发生故障，就会重新发现协调者，并向新的协调者发送JoinGroupRequest。

### Offsets和消费者位置

消费者可以定时自动地提交offset，或者手动控制什么时候提交offset。使用commitSync手动提交commitOffset，会阻塞调用线程，直到offsets成功被提交，或者在提交过程中发生错误。使用commitAsync则是非阻塞方式，会在成功提交或者失败时，触发OffsetCommitCallback回调函数的执行。

### 消费者组和主题订阅

当消费组发生自动重新分配(为partition分配consumer)时，消费者会通过ConsumerRebalanceListener被通知到。这样消费者就可以在监听器开始工作时做一些必要的应用程序处理逻辑，比如清除状态，手动提交offset。 同时消费组也可以通过assign(List)，将指定的partitions分配给消费者，这种方式需要关闭动态的partition分配。

### 新消费者示例

生产者向topic推送消息，消费者订阅topic，一旦topic有消息，消费者就会去拉数据。生产者的一条消息用ProducerRecord表示，消费者的批量消息是ConsumerRecords。生产消息时会指定消息的Key和Value，所以ConsumerRecord也有key和value(还有partition，offset其他属性)。

示例1：最简单的客户端消息消费

```java  
KafkaConsumer consumer = new KafkaConsumer<>(props);
consumer.subscribe(Collections.singletonList(this.topic));
ConsumerRecords records = consumer.poll(1000);
for (ConsumerRecord record : records) {
 System.out.println("Received message: (" + record.key() + 
 ", " + record.value() + ") at offset " + record.offset());
}

```



**自动提交offset**

利用Kafka的消费组提供的语义，可以管理Consumer的负载均衡和故障处理(offset存储在kafka，并自动提交offset)。Broker使用心跳的方式自动检测消费组中失败的消费者进程，消费者会定时地向集群发送ping(心跳)表示自己存活。只要消费者能够做这件事情(ping)，就说明它是存活的，它就会保留对分配给它的partition的消费的权利。如果消费者超过sessionTimeOut没有发送心跳就会被认为死亡，它的partitions就会分配给其他的线程。

示例2：自动提交offset，获取ConsumerRecord的offset

```java 
// 配置信息
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "test");
props.put("enable.auto.commit", "true");
props.put("auto.commit.interval.ms", "1000");
props.put("session.timeout.ms", "30000");

// 创建消费者实例, 并且订阅topic
KafkaConsumer consumer = new KafkaConsumer(props);
consumer.subscribe(Arrays.asList("foo", "bar"));

// 消费者消费消息
while (true) {
 ConsumerRecords records = consumer.poll(100);
 for (ConsumerRecord record : records)
 System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
}

```


**手动管理offset**

当消息的消费和其他处理逻辑耦合在一起时，只有处理逻辑完成后，才能认为这条消息被成功消费。在下面的示例中，我们消费了一批记录，并且在内存中暂时保存，当有足够的记录时插入到数据库中。如果像前面的示例允许自动提交offset，当消费者获取出消息时就认为消费了一批消息，而我们的处理逻辑在放到内存后，在插入数据库之前如果失败了，就会导致这批消息并没有保存到数据库中，却被消费掉了(丢失)。为了防止这种问题的出现，我们只有在对应的消息插入到数据库之后，才执行一次手动提交offset的工作。通过这种方式，我们可以精确地控制什么时候消息被认为成功地消费了。但是这却引起了另外的一个潜在的问题：在插入到数据库之后，在提交offset之前，客户端应用程序挂掉了，这样应用程序下次启动时，因为offset没有更新，消费者线程会从上次提交的offset开始继续消费消息，就会插入重复的数据(最近的一批)到数据库中。所以这种方式，对于kafka而言，只能保证消息”至少发送一次”，但不能保证”正好一次”(交给了客户端自己实现)。

* 1.commit offset=10
* 2.fetch from offset=10，get 5 msgs，offset=16
* 3.insert 5 msgs into db
* 4.client failed
* 5.still fetch from offset=10，get 5 msgs，offset=16
* 6.insert duplicated 5 msgs into db
* 7.commit offset=16
* 8.next time，fetch from offset=16

示例3：客户端手动管理offset的提交

```java  
props.put("enable.auto.commit", "false");  // 设置autoCommit为false

int commitInterval = 200;
List> buffer = new ArrayList>();
while (true) {
 ConsumerRecords records = consumer.poll(100);
 for (ConsumerRecord record : records) {
 buffer.add(record);
 if (buffer.size() >= commitInterval) {
 insertIntoDb(buffer);
 consumer.commitSync();
 buffer.clear();
 }
 }
}

```

**订阅指定的partition**

前面的示例我们订阅了感兴趣的topics，然后kafka会帮我们在这些topics公平地共享partitions。这种简单的负载均衡方式，能让客户端程序的多个实例(多个消费者进程)一起完成所有记录的处理工作。使用指定partition的方式，消费者只会分配到指定的partition，如果消费者挂掉后，并不会有负载均衡的工作，会将这个消费者的partitions分配给其他的消费者线程实例(相当于静态分配)。但是有几种场景是有意义的：

* 如果消费者逻辑维护了和这个Partition相关的一些本地状态(比如本地的KV存储)，就应该只从它维护的本地磁盘对应的partition获取记录
* 消费者线程本身就是HA的，如果它失败了，会重启(比如使用集群管理框架，就像YARN，Mesos，或者作为流处理框架的一部分)。这种情况也不需要kafka检测失败以及重新分配partition(因为失败后重启，还会消费之前所属的partition)。

在动态分配partition的场景下，消费者的加入和删除，都会导致partition的重新分配给其他的消费者。而静态分配partition下，如果消费者挂掉后，分配给这个消费者的partition并不会负载给其他消费者。静态分配partition的模式，消费者不是订阅主题，而是订阅指定的partition(当然partition也是由topic组成的)：

```java  
String topic = "foo";
TopicPartition partition0 = new TopicPartition(topic, 0);
TopicPartition partition1 = new TopicPartition(topic, 1);
consumer.assign(partition0);
consumer.assign(partition1);

```



consumer所指定的消费组仍然会用来提交offset(partition的offset是面向消费组的，而不是针对每个消费者，虽然partition是分配给消费者处理的，但如果offset记录在消费者上，当所属的消费者挂掉后，这个offset就会丢失掉了，所以应该记录在消费组上)。现在因为消费者固定分配了指定的partitions，只有指定了新的partitions，消费者的partitions集合才会变化，但仍然没有失败检测。注意：不可能为一个消费者实例同时混合订阅指定的partition(没有负载均衡)和订阅topic(有负载均衡)两种逻辑。

下面的示例consumer订阅了指定的topic和partitions，消费者在关闭之前会消费这些partitions到最近可用的消息。使用静态partition分配，就意味着自动放弃了消费组的管理功能。不过仍然要指定group.id来使用kafka的offset管理，但并不需要指定sessionTimeOut。因为只有使用group management时，在session超时后才会完成自动故障转移。

示例4：消费者订阅指定的partitions

```java
Properties props = new Properties();
props.put("metadata.broker.list", "localhost:9092");
props.put("group.id", "test");
props.put("enable.auto.commit", "true");
props.put("auto.commit.interval.ms", "10000");
KafkaConsumer consumer = new KafkaConsumer(props);

// subscribe to some partitions of topic foo
TopicPartition partition0 = new TopicPartition("foo", 0);
TopicPartition partition1 = new TopicPartition("foo", 1);
TopicPartition[] partitions = new TopicPartition[2];
partitions[0] = partition0;
partitions[1] = partition1;
consumer.subscribe(partitions);

// find the last committed offsets for partitions 0,1 of topic foo
Map lastCommittedOffsets = consumer.committed(partition0, partition1);
// seek to the last committed offsets to avoid duplicates
consumer.seek(lastCommittedOffsets);

// find the offsets of the latest available messages to know where to stop consumption
Map latestAvailableOffsets = 
 consumer.offsetsBeforeTime(-2, partition0, partition1);
boolean isRunning = true;
Map consumedOffsets = new HashMap();
while(isRunning) {
 Map records = consumer.poll(100, TimeUnit.MILLISECONDS);
 Map lastConsumedOffsets = process(records);
 consumedOffsets.putAll(lastConsumedOffsets);
 for(TopicPartition partition : partitions) {
 if(consumedOffsets.get(partition) >= latestAvailableOffsets.get(partition))
 isRunning = false;
 else
 isRunning = true;
 }
}
consumer.commit();
consumer.close();

```



**存储offset到kafka之外**

消费者客户端应用程序并不一定要求将kafka作为内置的offset存储。可以将offset存储在自己选择的其他存储系统中。常见的用法是应用程序将offset和消费的结果以原子性/事务的方式存储在同一个系统中，当然原子性并不一定是需要的。但是选择这种方式，可以确保消费的”完全原子性”，能够保证”正好一次”的语义，这比kafka默认提供的”至少一次”语义要强壮。

* 如果消费结果要保存到关系型数据库中，同时存储offset到数据库中，可以在一次事务中同时提交结果和offset。这种情况下，记录被消费，并成功存储，offset被更新表示事务成功。而事务失败时，结果不会存储，offset也不会被更新。
* 如果结果保存到本次存储，最好也将offset也一起保存到本地。

由于每条记录都有自己的offset，为了管理你自己的offset，需要做下面的几个工作：

* 配置enable.auto.commit=false，关闭自动提交offset
* 用每个ConsumerRecord的offset来保存你自己的position信息
* 在重启时，恢复consumer的position，调用seek(TopicPartition，long)

如果partition的分配也采用手动静态分配的方式，上面的步骤会简单很多。如果是自动分配partition，在partition变化时有一些额外的工作需要做。调用subscribe(List，ConsumerRebalanceListener)中的Listener就完成了这个额外的工作。当partitions从消费者去掉，消费者会在Listener的onPartitionRevoked()为这些partition提交offset(最后一次机会了)。当partitions分配给一个消费者，消费者会查找这些新的partition的offset(就比如上面被去掉的partition)，然后初始化(这是一个新创建的)消费者到查找出来的那个offset位置。这是在监听器的onPartitionsAssigned方法中。

ConsumerRebalanceListener另一个通用做法是在移动partition到其他消费者时，刷新应用程序为partitions维护的任何缓存。因为缓存是根据partition的数据构建的，一旦partition迁移到其他消费者实例，原先的缓存在当前应用程序就失效了，所以需要刷新。

示例5：消费者订阅指定的partitions，并且使用外部存储offset

```java
Properties props = new Properties();
props.put("metadata.broker.list", "localhost:9092");
KafkaConsumer consumer = new KafkaConsumer(props);

// subscribe to some partitions of topic foo
TopicPartition partition0 = new TopicPartition("foo", 0);
TopicPartition partition1 = new TopicPartition("foo", 1);
TopicPartition[] partitions = new TopicPartition[2];
partitions[0] = partition0;
partitions[1] = partition1;
consumer.subscribe(partitions);

// seek to the last committed offsets to avoid duplicates
Map lastCommittedOffsets = getLastCommittedOffsetsFromCustomStore();
consumer.seek(lastCommittedOffsets); 

// find the offsets of the latest available messages to know where to stop consumption
Map latestAvailableOffsets = 
 consumer.offsetsBeforeTime(-2, partition0, partition1);
boolean isRunning = true;
Map consumedOffsets = new HashMap();
while(isRunning) {
 Map records = consumer.poll(100, TimeUnit.MILLISECONDS);
 Map lastConsumedOffsets = process(records);
 consumedOffsets.putAll(lastConsumedOffsets);
 // commit offsets for partitions 0,1 for topic foo to custom store
 commitOffsetsToCustomStore(consumedOffsets);
 for(TopicPartition partition : partitions) {
 if(consumedOffsets.get(partition) >= latestAvailableOffsets.get(partition))
 isRunning = false;
 else isRunning = true;
 } 
} 
commitOffsetsToCustomStore(consumedOffsets); 
consumer.close();

```


**控制消费者的position**

在大多数情况下，消费者消费记录只是简单地从一开始到结束，并且定时地提交它的位置(不管是自动的还是手动的)。不过新的API也允许消费者手动控制它的位置，消费者可以在一个partition钟随意地往前或者往后移动位置。这就意味着消费者可以重新消费旧的记录(多次读取相同的记录)，或者直接跳到最近的记录，忽略掉中间的记录。

* 消费者可能落后太多，并不尝试抓取所有落后的记录，而是直接跳到最近的记录。对时间敏感的记录，这种处理方式也是有意义的。
* 对于需要维护本地状态的系统，消费者在启动时会初始化它的位置，无论本地状态保存的是什么。而且如果本地状态数据被破坏
    (比如磁盘损坏)，本地状态可以通过重新消费所有的数据，在新的机器上重建状态信息(假设kafka保存了足够的历史数据)。

kafka允许通过seek(TopicPartition，long)指定新的位置，或者seekToBeginning，seekToEnd定位到最早或最近的offset。下面的示例假设offsets保存在kafka中，并使用commit方法手动提交offset，如果消息消费失败，会重置consumer的offsets。注意seek重置offsets只对当前消费者起作用，它并不会触发consumer的rebalance，或者影响其他消费者的fetchOffsets。

示例6：消息消费失败时，重置offset

```java 
int commitInterval = 100;
int numRecords = 0;
boolean isRunning = true;
Map consumedOffsets = new HashMap();
while(isRunning) {
 Map records = consumer.poll(100, TimeUnit.MILLISECONDS);
 try {
 Map lastConsumedOffsets = process(records);
 consumedOffsets.putAll(lastConsumedOffsets);
 numRecords += records.size();
 // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
 if(numRecords % commitInterval == 0) consumer.commit();
 } catch(Exception e) {
 try {
 // rewind consumer's offsets for failed partitions
 // assume failedPartitions() returns the list of partitions for which the processing of the last batch of messages failed
 List failedPartitions = failedPartitions(); 
 Map offsetsToRewindTo = new HashMap();
 for(TopicPartition failedPartition : failedPartitions) {
 // rewind to the last consumed offset for the failed partition. Since process() failed for this partition, the consumed offset
 // should still be pointing to the last successfully processed offset and hence is the right offset to rewind consumption to.
 offsetsToRewindTo.put(failedPartition, consumedOffsets.get(failedPartition));
 }
 // seek to new offsets only for partitions that failed the last process()
 consumer.seek(offsetsToRewindTo);
 } catch(Exception e) {  break; } // rewind failed
 }
}
consumer.close();

```


上面的process方法假设接收一批消息，返回每个partition最近处理过的消息的offset(consumedOffset，不是nextOffset)。在消费一批数据之后，将consumedOffsets保存在内存中。当有异常发生时，循环failedPartitions的每个partition，从内存中获取出partition对应的consumedOffset，让消费者实例重新seek(参数可以是多个Partition到offset的映射)。

```java  
private Map process(Map records) {
 Map processedOffsets = new HashMap();
 for(Entry recordMetadata : records.entrySet()) {
 List recordsPerTopic = recordMetadata.getValue().records();
 for(int i = 0;i 
 ConsumerRecord record = recordsPerTopic.get(i);
 // process record
 processedOffsets.put(record.partition(), record.offset()); 
 }
 }
 return processedOffsets; 
}

```


示例7：对整个消费组倒回offsets

如果使用了kafka的group management(消费组管理功能具有consuers的自动负载均衡以及故障处理能力)，为每个消费者实例系统级地倒回offsets的准确位置是在ConsumerRebalanceListener回调函数里。 在consumer发生rebalance时，并且在消费消息之前，当consumer被分配到新的partitions集合后，会触发onPartitionAssigned回调函数的执行。在这里为consuer提供全新的倒回offset功能才是正确的。如果你能预知在当前的消费组管理中会一直重置consumer的offset，建议你总是配置consumer使用ConsumerRebalanceListener，并使用一个标志位用来判断是否启用offset的倒回逻辑功能。

倒回offset函数的作用是，在成功地消费了消息并且提交了offset之后，你发现了消息处理逻辑中存在的问题。这时你希望对整个消费组进行offset倒回，这还只是作为对处理逻辑修复的回滚操作的一部分工作。(消息处理逻辑存在问题，需要对已经消费的消息使用新的处理逻辑重新消费，所以需要回滚offset)这种情况下，你会为每个消费者实例开启倒回offset的配置标志位。并且依次滚动重启每个消费者实例。(消费逻辑存在问题，在修改消费者客户端代码后，必须要重启消费者进程才能以最新的逻辑消费消息)每次重启都会触发rebalance，最终所有的消费者实例都会对它们拥有的partitions倒回offsets。

```java
KafkaConsumer consumer = new KafkaConsumer(props,
 new ConsumerRebalanceListener() {
 boolean rewindOffsets = true;  // should be retrieved from external application config
 public void onPartitionsAssigned(Consumer consumer, TopicPartition...partitions) {
 Map latestCommittedOffsets = consumer.committed(partitions);
 if(rewindOffsets)
 Map newOffsets = rewindOffsets(latestCommittedOffsets, 100);
 consumer.seek(newOffsets);
 }
 public void onPartitionsRevoked(Consumer consumer, TopicPartition...partitions) {
 consumer.commit();
 }
 // this API rewinds every partition back by numberOfMessagesToRewindBackTo messages
 private Map rewindOffsets(Map currentOffsets,
 long numberOfMessagesToRewindBackTo) {
 Map newOffsets = new HashMap();
 for(Map.Entry offset : currentOffsets.entrySet()) 
 newOffsets.put(offset.getKey(), offset.getValue() - numberOfMessagesToRewindBackTo);
 return newOffsets;
 }
});
consumer.subscribe("foo", "bar");
//...同上调用了process消费消息,并保存到consumedOffsets内存中
consumer.close();

```

示例8：使用外部offset存储倒回offsets

由于将offset保存在外部存储系统中，消费者要倒回offset时，需要从自定义存储中读取offset提供给消费者。同样`onPartitionAssigned`回调函数也是将自定义存储的offsets提供给消费者的正确的地方。同时客户端代码还需要提供保存消费者的offsets到自定义存储系统中的方法(有读取就有存储)。因为`onPartitionsRevoked`会在消费者停止抓取数据之后，并partition的所有权更改之前调用。所以这里是为消费者拥有的partitions提交offsets的正确位置。

```java  
KafkaConsumer consumer = new KafkaConsumer(props,
 new ConsumerRebalanceListener() {
 // 从自定义存储中读取offset,让consumer重置offset
 public void onPartitionsAssigned(Consumer consumer, TopicPartition...partitions) {
 Map lastCommittedOffsets = getLastCommittedOffsets(partitions);
 consumer.seek(lastCommittedOffsets);
 }
 // 提交offset,保存offset带外部存储中
 public void onPartitionsRevoked(Consumer consumer, TopicPartition...partitions) {
 Map offsets = getLastConsumedOffsets(partitions);
 commitOffsetsToCustomStore(offsets); 
 }
 // following APIs should be implemented by the user for custom offset management
 private Map getLastCommittedOffsets(TopicPartition... partitions) {return null;}
 private Map getLastConsumedOffsets(TopicPartition... partitions) {return null;}
 private void commitOffsetsToCustomStore(Map offsets) {}
});
Map consumedOffsets = new HashMap();
while(isRunning) {
 Map records = consumer.poll(100, TimeUnit.MILLISECONDS);
 Map lastConsumedOffsets = process(records);
 consumedOffsets.putAll(lastConsumedOffsets);
 numRecords += records.size();
 // commit offsets for all partitions of topics foo, bar synchronously, owned by this consumer instance
 if(numRecords % commitInterval == 0) commitOffsetsToCustomStore(consumedOffsets);
}
consumer.close();

```


**消费流控制**

如果一个消费者要抓取多个分配的partitions，它会尝试同时消费所有partitions的消息，即这些partitions的优先级是相同的。但是在有些情况下，消费者要首先专注于对一部分partitions开足马力抓取数据，对其他partitions的抓取只有在优先级比较高的那些partitions只有很少数据，或者没有数据可以消费时(比较空闲的状态)，才去消费那些优先级比较低的partitions。典型的应用是流处理，比如处理器从两个topics抓取数据，并且在这两个流上运用join操作算子。当其中一个topic落后于另外一个流的消息太多，处理器应该要暂停抓取领先的流，而去抓取落后的流，让它赶上来(才能一起join)。另外一个场景是在消费者启动的时候，由于历史数据太多了，一时半会儿赶不上。而应用程序对于某些topics通常只需要得到最近的数据。所以对于这些topics会优先考虑抓取数据，而其他topics则会暂停(让出资源给优先级高的优先抓取，而资源共享会拖慢整体速度)。kafka支持动态的消息获取控制，pause会暂停获取某个partition的消息，而resume则恢复获取(在未来的某个时刻调用poll时)。

**多线程处理**

kafka的消费者(KafkaConsumer对象)并不是线程安全的。客户端代码需要自己确保多线程的访问是同步的。未同步的访问会抛出ConcurrentModificationException(比如对Map访问的同时又修改了Map也会报这个错)。 唯一例外的是wakeup方法(是线程安全的)：它可以被外部线程用来安全地中断一个进行中的操作。对于阻塞在wakeup方法上的线程会抛出WakeupException。可以被另外的线程用来作为关闭consumer的钩子。

```java  
public class KafkaConsumerRunner implements Runnable {
 private final AtomicBoolean closed = new AtomicBoolean(false);
 private final KafkaConsumer consumer;

 public void run() {
 try {
 consumer.subscribe("topic");
 while (!closed.get()) {
 ConsumerRecords records = consumer.poll(10000);
 // 处理新的记录
 }
 } catch (WakeupException e) {
 if (!closed.get()) throw e; //如果关闭了忽略异常
 } finally {
 consumer.close();
 }
 }
 // 关闭钩子,可以在另一个线程中调用
 public void shutdown() {
 closed.set(true);
 consumer.wakeup();
 }
}

```



我们故意避免为了消息处理而实现特殊的线程模型(即Handle new records部分)，有多种方式实现多线程的消息处理。

1) 一个线程一个消费者

每个线程都有自己的消费者实例，消息消费逻辑和消息处理逻辑都在消费者线程中完成。这种方式的利弊：

* 优点：很容易实现，执行很快，因为没有线程之间的交互和协调。
* 优点：对于每个partition要保证顺序处理比较容易实现。每个线程只需要按照顺序处理它接收到的消息即可。
* 缺点：更多的消费者意味着集群的TCP连接也很多。不过kafka处理连接是很高效的，所以这个代价并不是很大。
* 缺点：多个消费者意味着发送更多的请求给服务器，每一批发送的数据变少(发送更多批)，就会降低I/O吞吐量。
* 缺点：所有进程之间的线程数量会被partitions的数量所限制。

2) 解耦消费和处理逻辑

另一种方式是有一个或多个消费者线程用来消费消息，并将消费结果ConsumerRecords转移一个阻塞队列中，
它会被消息处理线程池消费，消息处理线程顾名思义就是处理消息的线程。这种方式的利弊：

* 优点：可以相互独立地扩展消费者数量和处理器数量。可以只用一个消费者线程服务于多个处理线程，避免partitions的限制。
* 缺点：在处理器线程之间保证消息处理的顺序是比较困难的。因为线程之间是独立的，线程之间的顺序是无法保证的。所以即使是比较早的数据块也有可能比靠后面的数据块更晚被处理到。如果要求消息的处理是无序的，当然是没有问题的。
* 缺点：手动提交offset变得困难，因为它需要所有的线程协调起来确保这个partition的消息已经被处理完毕。

解决上面的缺点有多种方式。比如每个处理线程都可以有自己的队列，消费者可以对TopicPartition的hash结果放入不同处理线程的队列中，这样也可以确保消息被顺序地消费，并且简化提交offset的逻辑。



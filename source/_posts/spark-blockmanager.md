---
title: Spark BlockManager分析
date: 2018-04-04 15:59:48
tags: 
     - 源码分析
     - 性能优化
     - 大数据
     - 内存管理
categories: spark
---
BlockManager 是 spark 中至关重要的一个组件， 在 spark的的运行过程中到处都有 BlockManager 的身影， 只有搞清楚 BlockManager 的原理和机制，你才能更加深入的理解 spark。 今天我们来揭开 BlockaManager 的底层原理和设计思路。
<!-- more -->

## 整体架构

BlockManager 是一个嵌入在 spark 中的 key-value型分布式存储系统，是为 spark 量身打造的，BlockManager 在一个 spark 应用中作为一个本地缓存运行在所有的节点上， 包括所有 driver 和 executor上。 BlockManager 对本地和远程提供一致的 get 和set 数据块接口， BlockManager 本身使用不同的存储方式来存储这些数据， 包括 memory, disk, off-heap。
[![](http://7xim8y.com1.z0.glb.clouddn.com/QQ20170330-2.png?imageView2/1/w/600/h/400)](http://7xim8y.com1.z0.glb.clouddn.com/QQ20170330-2.png?imageView2/1/w/600/h/400)

上面是一个整体的架构图， BlockManagerMaster拥有BlockManagerMasterEndpoint 的actor和所有BlockManagerSlaveEndpoint的ref， 可以通过这些引用对 slave 下达命令

executor 节点上的BlockManagerMaster 则拥有BlockManagerMasterEndpoint的ref和自身BlockManagerSlaveEndpoint的actor。可以通过 Master的引用注册自己。

在master 和 slave 可以正常的通信之后， 就可以根据设计的交互协议进行交互， 整个分布式缓存系统也就运转起来了

## 初始化

我们知道， sparkEnv 启动的时候会启动各个组件， BlockManager 也不例外， 也是这个时候启动的
sparkEnv调用create 方法启动。
Spark context启动时候会初始化SparkEnv

```scala  
_env = createSparkEnv(_conf, isLocal, listenerBus)
SparkEnv.set(_env)
```


调用如下方法createSparkEnv

```scala  
private[spark] def createSparkEnv(
 conf: SparkConf,
 isLocal: Boolean,
 listenerBus: LiveListenerBus): SparkEnv = {
 SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master))
 }
```
执行SparkEnv.createDriverEnv

```scala 
private[spark] def createDriverEnv(
 conf: SparkConf,
 isLocal: Boolean,
 listenerBus: LiveListenerBus,
 numCores: Int,
 mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
 assert(conf.contains(DRIVER_HOST_ADDRESS),
 s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
 assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
 val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
 val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
 val port = conf.get("spark.driver.port").toInt
 val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
 Some(CryptoStreamUtils.createKey(conf))
 } else {
 None
 }
 create(
 conf,
 SparkContext.DRIVER_IDENTIFIER,
 bindAddress,
 advertiseAddress,
 port,
 isLocal,
 numCores,
 ioEncryptionKey,
 listenerBus = listenerBus,
 mockOutputCommitCoordinator = mockOutputCommitCoordinator
 )
 }
```


最终执行create方法，create 方法中初始化blockManagerMaster,blockManager

```scala  
val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
 BlockManagerMaster.DRIVER_ENDPOINT_NAME,
 new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
 conf, isDriver)
  
val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
 serializerManager, conf, memoryManager, mapOutputTracker, shuffleManager,
 blockTransferService, securityManager, numUsableCores)
```


启动的时候会根据自己是在 driver 还是 executor 上进行不同的启动过程

```scala 
def registerOrLookupEndpoint(
 name: String, endpointCreator: => RpcEndpoint):
 RpcEndpointRef = {
 if (isDriver) {
 logInfo("Registering " + name)
 rpcEnv.setupEndpoint(name, endpointCreator)
 } else {
 RpcUtils.makeDriverRef(name, conf, rpcEnv)
 }
 }
```


sparkEnv 在 master上启动的时候， 构造了一个 BlockManagerMasterEndpoint, 然后把这个Endpoint 注册在 rpcEnv中， 同时也会启动自己的 BlockManager
[![](http://7xim8y.com1.z0.glb.clouddn.com/sparkenv-driver-blockmanager1.png?imageView2/1/w/600/h/500)](http://7xim8y.com1.z0.glb.clouddn.com/sparkenv-driver-blockmanager1.png?imageView2/1/w/600/h/500)

sparkEnv 在executor上启动的时候， 通过 setupEndpointRef 方法获取到了 BlockManagerMaster的引用 BlockManagerMasterRef， 同时也会启动自己的 BlockManager
[![](http://7xim8y.com1.z0.glb.clouddn.com/sparkenv-executor-blockmanager1.png?imageView2/1/w/600/h/500)](http://7xim8y.com1.z0.glb.clouddn.com/sparkenv-executor-blockmanager1.png?imageView2/1/w/600/h/500)

在 BlockManager 初始化自己的时候， 会向 BlockManagerMasterEndpoint 注册自己， BlockManagerMasterEndpoint 发送 registerBlockManager消息， BlockManagerMasterEndpoint 接受到消息， 把 BlockManagerSlaveEndpoint 的引用 保存在自己的 blockManagerInfo 数据结构中以待后用

在SparkContext初始化时候调用

```scala  
_env.blockManager.initialize(_applicationId)

```



方法如下：

```scala
/**
 * Initializes the BlockManager with the given appId. This is not performed in the constructor as
 * the appId may not be known at BlockManager instantiation time (in particular for the driver,
 * where it is only learned after registration with the TaskScheduler).
 *
 * This method initializes the BlockTransferService and ShuffleClient, registers with the
 * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
 * service if configured.
 */
 def initialize(appId: String): Unit = {
 blockTransferService.init(this)
 shuffleClient.init(appId)

 blockReplicationPolicy = {
 val priorityClass = conf.get(
 "spark.storage.replication.policy", classOf[RandomBlockReplicationPolicy].getName)
 val clazz = Utils.classForName(priorityClass)
 val ret = clazz.newInstance.asInstanceOf[BlockReplicationPolicy]
 logInfo(s"Using $priorityClass for block replication policy")
 ret
 }

 val id =
 BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)

 val idFromMaster = master.registerBlockManager(
 id,
 maxOnHeapMemory,
 maxOffHeapMemory,
 slaveEndpoint)

 blockManagerId = if (idFromMaster != null) idFromMaster else id

 shuffleServerId = if (externalShuffleServiceEnabled) {
 logInfo(s"external shuffle service port = $externalShuffleServicePort")
 BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
 } else {
 blockManagerId
 }

 // Register Executors' configuration with the local shuffle service, if one should exist.
 if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
 registerWithExternalShuffleServer()
 }

 logInfo(s"Initialized BlockManager: $blockManagerId")
 }

```

调用master 如下方法

```scala  
val idFromMaster = master.registerBlockManager(
 id,
 maxOnHeapMemory,
 maxOffHeapMemory,
 slaveEndpoint)
```


继续往下：


```scala
/**
 * Register the BlockManager's id with the driver. The input BlockManagerId does not contain
 * topology information. This information is obtained from the master and we respond with an
 * updated BlockManagerId fleshed out with this information.
 */
 def registerBlockManager(
 blockManagerId: BlockManagerId,
 maxOnHeapMemSize: Long,
 maxOffHeapMemSize: Long,
 slaveEndpoint: RpcEndpointRef): BlockManagerId = {
 logInfo(s"Registering BlockManager $blockManagerId")
 val updatedId = driverEndpoint.askSync[BlockManagerId](
 RegisterBlockManager(blockManagerId, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint))
 logInfo(s"Registered BlockManager $updatedId")
 updatedId
 }

```


BlockManagerMasterEndpoint接受到消息 RegisterBlockManager

```scala  
case RegisterBlockManager(blockManagerId, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint) =>
 context.reply(register(blockManagerId, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint))
```


register 方法把 BlockManagerSlaveEndpoint 的引用 保存在自己的 blockManagerInfo 数据结构

```scala  
/**
 * Returns the BlockManagerId with topology information populated, if available.
 */
 private def register(
 idWithoutTopologyInfo: BlockManagerId,
 maxOnHeapMemSize: Long,
 maxOffHeapMemSize: Long,
 slaveEndpoint: RpcEndpointRef): BlockManagerId = {
 // the dummy id is not expected to contain the topology information.
 // we get that info here and respond back with a more fleshed out block manager id
 val id = BlockManagerId(
 idWithoutTopologyInfo.executorId,
 idWithoutTopologyInfo.host,
 idWithoutTopologyInfo.port,
 topologyMapper.getTopologyForHost(idWithoutTopologyInfo.host))

 val time = System.currentTimeMillis()
 if (!blockManagerInfo.contains(id)) {
 blockManagerIdByExecutor.get(id.executorId) match {
 case Some(oldId) =>
 // A block manager of the same executor already exists, so remove it (assumed dead)
 logError("Got two different block manager registrations on same executor - "
 + s" will replace old one $oldId with new one $id")
 removeExecutor(id.executorId)
 case None =>
 }
 logInfo("Registering block manager %s with %s RAM, %s".format(
 id.hostPort, Utils.bytesToString(maxOnHeapMemSize + maxOffHeapMemSize), id))

 blockManagerIdByExecutor(id.executorId) = id

 blockManagerInfo(id) = new BlockManagerInfo(
 id, System.currentTimeMillis(), maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint)
 }
 listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxOnHeapMemSize + maxOffHeapMemSize,
 Some(maxOnHeapMemSize), Some(maxOffHeapMemSize)))
 id
 }
```


## 分布式协议

下面的一个表格是 master 和 slave 接受到各种类型的消息， 以及接受到消息后，做的处理。

* BlockManagerMasterEndpoint 接受的消息

| 消息 | 处理 |
| :-- | --: |
| RegisterBlockManager | slave 注册自己的消息，会保存在自己的blockManagerInfo中 |
| UpdateBlockInfo | 一个Block的更新消息，BlockId作为一个Block的唯一标识，会保存Block所在的节点和位置关系，以及block 存储级别，大小 占用内存和磁盘大小 |
| GetLocationsMultipleBlockIds | 获取多个Block所在 的位置，位置中会反映Block位于哪个 executor, host 和端口 |
| GetPeers | 一个block有可能在多个节点上存在，返回一个节点列表 |
| GetExecutorEndpointRef | 根据BlockId,获取所在executorEndpointRef 也就是 BlockManagerSlaveEndpoint的引用 |
| GetMemoryStatus | 获取所有节点上的BlockManager的最大内存和剩余内存 |
| GetStorageStatus | 获取所有节点上的BlockManager的最大磁盘空间和剩余磁盘空间 |
| GetBlockStatus | 获取一个Block的状态信息，位置，占用内存和磁盘大小 |
| GetMatchingBlockIds | 获取一个Block的存储级别和所占内存和磁盘大小 |
| RemoveRdd | 删除Rdd对应的Block数据 |
| RemoveBroadcast | 删除Broadcast对应的Block数据 |
| RemoveBlock | 删除一个Block数据，会找到数据所在的slave,然后向slave发送一个删除消息 |
| RemoveExecutor | 从BlockManagerInfo中删除一个BlockManager, 并且删除这个 BlockManager上的所有的Blocks |
| BlockManagerHeartbeat | slave 发送心跳给 master , 证明自己还活着 |

* BlockManagerSlaveEndpoint 接受的消息

| 消息 | 处理 |
| :-- | --: |
| RemoveBlock | slave删除自己BlockManager上的一个Block |
| RemoveRdd | 删除Rdd对应的Block数据 |
| RemoveShuffle | 删除 shuffleId对应的BlockId的Block |
| RemoveBroadcast | 删除 BroadcastId对应的BlockId的Block |
| GetBlockStatus | 获取一个Block的存储级别和所占内存和磁盘大小 |

根据以上的协议， 相信我们可以很清楚的猜测整个交互的流程， 一般过程应该是这样的， slave的 BlockManager 在自己接的上存储一个 Block, 然后把这个 BlockId 汇报到master的BlockManager , 经过 cache, shuffle 或者 Broadcast后，别的节点需要上一步的Block的时候， 会到 master 获取数据所在位置， 然后去相应节点上去 fetch

## 存储层

在RDD层面上我们了解到RDD是由不同的partition组成的，我们所进行的transformation和action是在partition上面进行的；而在storage模块内部，RDD又被视为由不同的block组成，对于RDD的存取是以block为单位进行的，本质上partition和block是等价的，只是看待的角度不同。在Spark storage模块中中存取数据的最小单位是block，所有的操作都是以block为单位进行的。
[![](http://7xim8y.com1.z0.glb.clouddn.com/storage_layer.png?imageView2/1/w/600/h/500)](http://7xim8y.com1.z0.glb.clouddn.com/storage_layer.png?imageView2/1/w/600/h/500)

BlockManager对象被创建的时候会创建出MemoryStore和DiskStore对象用以存取block，如果内存中拥有足够的内存， 就 使用 MemoryStore存储， 如果 不够， 就 spill 到 磁盘中， 通过 DiskStore进行存储。

* DiskStore 有一个DiskBlockManager,DiskBlockManager 主要用来创建并持有逻辑 blocks 与磁盘上的 blocks之间的映射，一个逻辑 block 通过 BlockId 映射到一个磁盘上的文件。 在 DiskStore 中会调用 diskManager.getFile 方法， 如果子文件夹不存在，会进行创建， 文件夹的命名方式为(spark-local-yyyyMMddHHmmss-xxxx, xxxx是一个随机数)， 所有的block都会存储在所创建的folder里面。

* MemoryStore 相对于DiskStore需要根据block id hash计算出文件路径并将block存放到对应的文件里面，MemoryStore管理block就显得非常简单：MemoryStore内部维护了一个hash map来管理所有的block，以block id为key将block存放到hash map中。而从MemoryStore中取得block则非常简单，只需从hash map中取出block id对应的value即可。

BlockManager 的 PUT 和GET接口

* GET操作 如果 local 中存在就直接返回， 从本地获取一个Block, 会先判断如果是 useMemory， 直接从内存中取出， 如果是 useDisk， 会从磁盘中取出返回， 然后根据useMemory判断是否在内存中缓存一下，方便下次获取， 如果local 不存在， 从其他节点上获取， 当然元信息是存在 drive上的，要根据我们上文中提到的 GETlocation 协议获取 Block 所在节点位置， 然后到其他节点上获取。

* PUT操作 操作之前会加锁来避免多线程的问题， 存储的时候会根据 存储级别， 调用对应的是 memoryStore 还是 diskStore， 然后在具体存储器上面调用 存储接口。 如果有 replication 需求， 会把数据备份到其他的机器上面



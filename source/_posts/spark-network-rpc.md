---
title: Spark RPC通信层设计原理分析
date: 2018-04-04 16:08:40
tags: 
     - 源码分析
     - 大数据
     - rpc
categories: spark
---
Spark将RPC通信层设计的非常巧妙，融合了各种设计/架构模式，将一个分布式集群系统的通信层细节完全屏蔽，这样在上层的计算框架的设计中能够获得很好的灵活性。同时，如果上层想要增加各种新的特性，或者对来自不同企业或组织的程序员贡献的特性，也能够很容易地增加进来，可以避开复杂的通信层而将注意力集中在上层计算框架的处理和优化上，入手难度非常小。另外，对上层计算框架中的各个核心组件的开发、功能增强，以及Bug修复等都会变得更加容易
<!-- more -->
## Spark RPC层设计概览

Spark RPC层是基于优秀的网络通信框架Netty设计开发的，同时获得了Netty所具有的网络通信的可靠性和高效性。我们先把Spark中与RPC相关的一些类的关系梳理一下，为了能够更直观地表达RPC的设计，我们先从类的设计来看，如下图所示
[![](http://7xim8y.com1.z0.glb.clouddn.com/SparkRPC-ClassDiagram.png?imageView2/1/w/800/h/500)](http://7xim8y.com1.z0.glb.clouddn.com/SparkRPC-ClassDiagram.png?imageView2/1/w/800/h/500)
通过上图，可以清晰地将RPC设计分离出来，能够对RPC层有一个整体的印象。了解Spark RPC层的几个核心的概念（我们通过Spark源码中对应的类名来标识），能够更好地理解设计

### RpcEndpoint

RpcEndpoint定义了RPC通信过程中的通信端对象，除了具有管理一个RpcEndpoint生命周期的操作（constructor -> onStart -> receive* -> onStop），并给出了通信过程中一个RpcEndpoint所具有的基于事件驱动的行为（连接、断开、网络异常），实际上对于Spark框架来说主要是接收消息并处理，具体可以看对应特质RpcEndpoint的代码定义，如下所示

```scala  
private[spark] trait RpcEndpoint {

 /**
 * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
 */
 val rpcEnv: RpcEnv

 /**
 * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
 * called. And `self` will become `null` when `onStop` is called.
 *
 * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
 * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
 */
 final def self: RpcEndpointRef = {
 require(rpcEnv != null, "rpcEnv has not been initialized")
 rpcEnv.endpointRef(this)
 }

 /**
 * Process messages from `RpcEndpointRef.send` or `RpcCallContext.reply`. If receiving a
 * unmatched message, `SparkException` will be thrown and sent to `onError`.
 */
 def receive: PartialFunction[Any, Unit] = {
 case _ => throw new SparkException(self + " does not implement 'receive'")
 }

 /**
 * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
 * `SparkException` will be thrown and sent to `onError`.
 */
 def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
 case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
 }

 /**
 * Invoked when any exception is thrown during handling messages.
 */
 def onError(cause: Throwable): Unit = {
 // By default, throw e and let RpcEnv handle it
 throw cause
 }

 /**
 * Invoked when `remoteAddress` is connected to the current node.
 */
 def onConnected(remoteAddress: RpcAddress): Unit = {
 // By default, do nothing.
 }

 /**
 * Invoked when `remoteAddress` is lost.
 */
 def onDisconnected(remoteAddress: RpcAddress): Unit = {
 // By default, do nothing.
 }

 /**
 * Invoked when some network error happens in the connection between the current node and
 * `remoteAddress`.
 */
 def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
 // By default, do nothing.
 }

 /**
 * Invoked before [[RpcEndpoint]] starts to handle any message.
 */
 def onStart(): Unit = {
 // By default, do nothing.
 }

 /**
 * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
 * use it to send or ask messages.
 */
 def onStop(): Unit = {
 // By default, do nothing.
 }

```

通过上面的receive方法，接收由RpcEndpointRef.send方法发送的消息，该类消息不需要进行响应消息（Reply），而只是在RpcEndpoint端进行处理。通过receiveAndReply方法，接收由RpcEndpointRef.ask发送的消息，RpcEndpoint端处理完消息后，需要给调用RpcEndpointRef.ask的通信端响应消息（Reply）

### RpcEndpointRef

RpcEndpointRef是一个对RpcEndpoint的远程引用对象，通过它可以向远程的RpcEndpoint端发送消息以进行通信。RpcEndpointRef特质的定义，代码如下所示：

```scala 
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
 extends Serializable with Logging {

 private[this] val maxRetries = RpcUtils.numRetries(conf)
 private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf)
 private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

 /**
 * return the address for the [[RpcEndpointRef]]
 */
 def address: RpcAddress

 def name: String

 /**
 * Sends a one-way asynchronous message. Fire-and-forget semantics.
 */
 def send(message: Any): Unit

 /**
 * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
 * receive the reply within the specified timeout.
 *
 * This method only sends the message once and never retries.
 */
 def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

 /**
 * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
 * receive the reply within a default timeout.
 *
 * This method only sends the message once and never retries.
 */
 def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

 /**
 * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
 * default timeout, throw an exception if this fails.
 *
 * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
 * loop of [[RpcEndpoint]].

 * @param message the message to send
 * @tparam T type of the reply message
 * @return the reply message from the corresponding [[RpcEndpoint]]
 */
 def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)

 /**
 * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
 * specified timeout, throw an exception if this fails.
 *
 * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
 * loop of [[RpcEndpoint]].
 *
 * @param message the message to send
 * @param timeout the timeout duration
 * @tparam T type of the reply message
 * @return the reply message from the corresponding [[RpcEndpoint]]
 */
 def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
 val future = ask[T](message, timeout)
 timeout.awaitResult(future)
 }

}

```


send方法发送消息后不等待响应，亦即Send-and-forget，Spark中基于Netty实现，实现在NettyRpcEndpointRef中

```scala  
override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
 nettyEnv.ask(new RequestMessage(nettyEnv.address, this, message), timeout)
 }
```


### RpcEnv

一个RpcEnv是一个RPC环境对象，它负责管理RpcEndpoint的注册，以及如何从一个RpcEndpoint获取到一个RpcEndpointRef。RpcEndpoint是一个通信端，例如Spark集群中的Master，或Worker，都是一个RpcEndpoint。但是，如果想要与一个RpcEndpoint端进行通信，一定需要获取到该RpcEndpoint一个RpcEndpointRef，而获取该RpcEndpointRef只能通过一个RpcEnv环境对象来获取。所以说，一个RpcEnv对象才是RPC通信过程中的“指挥官”，在RpcEnv类中，有一个核心的方法：
def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef
通过上面方法，可以注册一个RpcEndpoint到RpcEnv环境对象中，有RpcEnv来管理RpcEndpoint到RpcEndpointRef的绑定关系。在注册RpcEndpoint时，每个RpcEndpoint都需要有一个唯一的名称。
Spark中基于Netty实现通信，所以对应的RpcEnv实现为NettyRpcEnv，上面方法的实现，如下所示：

```scala  
override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
 dispatcher.registerRpcEndpoint(name, endpoint)
 }
```



调用NettyRpcEnv内部的Dispatcher对象注册一个RpcEndpoint

```scala  
def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
 val addr = RpcEndpointAddress(nettyEnv.address, name)
 val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
 synchronized {
 if (stopped) {
 throw new IllegalStateException("RpcEnv has been stopped")
 }
 if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
 throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
 }
 val data = endpoints.get(name)
 endpointRefs.put(data.endpoint, data.ref)
 receivers.offer(data)  // for the OnStart message
 }
 endpointRef
 }
```


一个RpcEndpoint只能注册一次（根据RpcEndpoint的名称来检查唯一性），这样在Dispatcher内部注册并维护RpcEndpoint与RpcEndpointRef的绑定关系，通过如下两个内部结构：

```scala  
private val endpoints: ConcurrentMap[String, EndpointData] =
 new ConcurrentHashMap[String, EndpointData]
 private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
 new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]
```



可以看到，一个命名唯一的RpcEndpoint在Dispatcher中对应一个EndpointData来维护其信息，该数据结构定义，如下所示:

```scala  
private class EndpointData(
 val name: String,
 val endpoint: RpcEndpoint,
val ref: NettyRpcEndpointRef) {
 val inbox = new Inbox(ref, endpoint)
 }
```


这里，每一个命名唯一的RpcEndpoint对应一个线程安全的Inbox，所有发送给一个RpcEndpoint的消息，都由对应的Inbox将对应的消息路由给RpcEndpoint进行处理，后面我们会详细分析Inbox

## 创建NettyRpcEnv环境对象

创建NettyRpcEnv对象，是一个非常重的操作，所以在框架里使用过程中要尽量避免重复创建。创建NettyRpcEnv，会创建很多用来处理底层RPC通信的线程和数据结构。具体的创建过程，如下图所示：
[![](http://res.cloudinary.com/dod7hzkn2/image/upload/v1517463382/SparkRPC-Create-NettyRpcEnv_xdhorq.png)](http://res.cloudinary.com/dod7hzkn2/image/upload/v1517463382/SparkRPC-Create-NettyRpcEnv_xdhorq.png)

```scala
private[rpc] class NettyRpcEnvFactory extends RpcEnvFactory with Logging {

 def create(config: RpcEnvConfig): RpcEnv = {
 val sparkConf = config.conf
 // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
 // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
 val javaSerializerInstance =
 new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
 val nettyEnv =
 new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress,
 config.securityManager)
 if (!config.clientMode) {
 val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
 nettyEnv.startServer(config.bindAddress, actualPort)
 (nettyEnv, nettyEnv.address.port)
 }
 try {
 Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
 } catch {
 case NonFatal(e) =>
 nettyEnv.shutdown()
 throw e
 }
 }
 nettyEnv
 }
}
```

具体要点，描述如下：

* 创建一个NettyRpcEnv对象对象，需要通过NettyRpcEnvFactory来创建
* Dispatcher负责RPC消息的路由，它能够将消息路由到对应的RpcEndpoint进行处理
* NettyStreamManager负责提供文件服务（文件、JAR文件、目录）
* NettyRpcHandler负责处理网络IO事件，接收RPC调用请求，并通过Dispatcher派发消息
* TransportContext负责管理网路传输上下文信息：创建MessageEncoder、MessageDecoder、TransportClientFactory、TransportServer
* TransportServer配置并启动一个RPC Server服务

## 消息路由过程分析

基于Standalone模式，Spark集群具有Master和一组Worker，Worker与Master之间需要进行通信，我们以此为例，来说明基于Spark PRC层是如何实现消息的路由的。
首先看Master端实现，代码如下所示：

```scala  
def startRpcEnvAndEndpoint(
 host: String,
 port: Int,
 webUiPort: Int,
 conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
 val securityMgr = new SecurityManager(conf)
 val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
 val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
 new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
 val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
 (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
 }

```


上面代码中，创建一个RpcEnv对象，通过创建一个NettyRpcEnvFactory对象来完成该RpcEnv对象的创建，实际创建了一个NettyRpcEnv对象。接着，通过setupEndpoint方法注册一个RpcEndpoint，这里Master就是一个RpcEndpoint，返回的masterEndpoint是Master的RpcEndpointRef引用对象。下面，我们看一下，发送一个BoundPortsRequest消息，具体的消息路由过程，如下图所示：
[![](http://res.cloudinary.com/dod7hzkn2/image/upload/v1517393595/Master_agnadl.png)](http://res.cloudinary.com/dod7hzkn2/image/upload/v1517393595/Master_agnadl.png)

上图中显示本地消息和远程消息派发的流程，最主要的区别是在接收消息时：接收消息走的是Inbox，发送消息走的是Outbox

## 本地消息路由

发送一个BoundPortsRequest消息，实际走的是本地消息路由，直接放到对应的Inbox中，对应的代码处理逻辑如下所示：

```scala
/**
 * Posts a message to a specific endpoint.
 *
 * @param endpointName name of the endpoint.
 * @param message the message to post
 * @param callbackIfStopped callback function if the endpoint is stopped.
 */
private def postMessage(
 endpointName: String,
 message: InboxMessage,
 callbackIfStopped: (Exception) => Unit): Unit = {
 val error = synchronized {
 val data = endpoints.get(endpointName)
 if (stopped) {
 Some(new RpcEnvStoppedException())
 } else if (data == null) {
 Some(new SparkException(s"Could not find $endpointName."))
 } else {
 data.inbox.post(message)
 receivers.offer(data)
 None
 }
 }
 // We don't need to call `onStop` in the `synchronized` block
 error.foreach(callbackIfStopped)
}

```


上面通过data.inbox派发消息，然后将消息data :EndpointData放入到receivers队列，触发Dispatcher内部的MessageLoop线程去消费，如下所示：

```scala
/** Message loop used for dispatching messages. */
 private class MessageLoop extends Runnable {
 override def run(): Unit = {
 try {
 while (true) {
 try {
 val data = receivers.take()
 if (data == PoisonPill) {
 // Put PoisonPill back so that other MessageLoops can see it.
 receivers.offer(PoisonPill)
 return
 }
 data.inbox.process(Dispatcher.this)
 } catch {
 case NonFatal(e) => logError(e.getMessage, e)
 }
 }
 } catch {
 case ie: InterruptedException => // exit
 }
 }
 }

```


上面的过程在Dispatcher 内部线程池执行。内部线程池如下

```scala  
private val threadpool: ThreadPoolExecutor = {
 val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
 math.max(2, Runtime.getRuntime.availableProcessors()))
 val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
 for (i 
 pool.execute(new MessageLoop)
 }
 pool
 }

```



这里，又继续调用Inbox的process方法来派发消息到指定的RpcEndpoint。通过上面的序列图，我们可以通过源码分析看到，原始消息被层层封装为一个RpcMessage ，该消息在Inbox的process方法中处理派发逻辑，如下所示：

```scala 
case RpcMessage(_sender, content, context) =>
 try {
 endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
 throw new SparkException(s"Unsupported message $message from ${_sender}")
 })
 } catch {
 case NonFatal(e) =>
 context.sendFailure(e)
 // Throw the exception -- this exception will be caught by the safelyCall function.
 // The endpoint's onError function will be called.
 throw e
 }
```


到这里，消息已经发送给对应的RpcEndpoint的receiveAndReply方法，我们这里实际上是Master实现类，这里的消息解包后为content: BoundPortsRequest，接下来应该看Master的receiveAndReply方法如何处理该本地消息，代码如下所示：

```scala 
case BoundPortsRequest =>
 context.reply(BoundPortsResponse(address.port, webUi.boundPort, restServerBoundPort))

```



可以看出，实际上上面的处理逻辑没有什么处理，只是通过BoundPortsResponse返回了几个Master端的几个端口号数据。

## 远程消息路由

我们都知道，Worker启动时，会向Master注册，通过该场景我们分析一下远程消息路由的过程。
先看一下Worker端向Master注册过程，如下图所示：
[![](http://res.cloudinary.com/dod7hzkn2/image/upload/v1517457688/Worker.ask__xjou68.png)](http://res.cloudinary.com/dod7hzkn2/image/upload/v1517457688/Worker.ask__xjou68.png)

Worker启动时，会首先获取到一个Master的RpcEndpointRef远程引用，通过该引用对象能够与Master进行RPC通信，经过上面消息派发，最终通过Netty的Channel将消息发送到远程Master端。
通过前面说明，我们知道Worker向Master注册的消息RegisterWorker应该最终会被路由到Master对应的Inbox中，然后派发给Master进行处理。下面，我们看一下Master端接收并处理消息的过程，如下图所示：
[![](http://res.cloudinary.com/dod7hzkn2/image/upload/v1517457805/Master.receiveAndReply_vdzfwo.png)](http://res.cloudinary.com/dod7hzkn2/image/upload/v1517457805/Master.receiveAndReply_vdzfwo.png)

上图分为两部分：一部分是从远端接收消息RegisterWorker，将接收到的消息放入到Inbox中；另一部分是触发MessageLoop线程处理该消息，进而通过调用Inbox的process方法，继续调用RpcEndpoint（Master）的receiveAndReply方法，处理消息RegisterWorker，如下所示：

```scala  
case RegisterWorker(
 id, workerHost, workerPort, workerRef, cores, memory, workerWebUiUrl, masterAddress) =>
 logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
 workerHost, workerPort, cores, Utils.megabytesToString(memory)))
 if (state == RecoveryState.STANDBY) {
 workerRef.send(MasterInStandby)
 } else if (idToWorker.contains(id)) {
 workerRef.send(RegisterWorkerFailed("Duplicate worker ID"))
 } else {
 val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
 workerRef, workerWebUiUrl)
 if (registerWorker(worker)) {
 persistenceEngine.addWorker(worker)
 workerRef.send(RegisteredWorker(self, masterWebUiUrl, masterAddress))
 schedule()
 } else {
 val workerAddress = worker.endpoint.address
 logWarning("Worker registration failed. Attempted to re-register worker at same " +
 "address: " + workerAddress)
 workerRef.send(RegisterWorkerFailed("Attempted to re-register worker at same address: "
 + workerAddress))
 }
 }

```

如果Worker注册成功，则Master会通过context对象回复Worker响应

```scala 
context.reply(RegisteredWorker(self, masterWebUiUrl))

```


这样，如果一切正常，则Worker会收到RegisteredWorker响应消息，从而获取到Master的RpcEndpointRef引用对象，能够通过该引用对象与Master交互。





class SimpleConsumer:
    def __init__():
        pass
    def getConsumerGroup():
        pass
    def subscribe(topic, filterExpression):
        pass
    def unsubscribe(topic):
        pass
    def getSubscriptionExpressions():
        pass
    def receive(maxMessageNum, invisibleDuration):
        pass
    def receiveAsync(maxMessageNum, invisibleDuration):
        pass
    def ack(messageView):
        pass
    def ackAsync(messageView):
        pass
    def changeInvisibleDuration(messageView, invisibleDuration):
        pass
    def changeInvisibleDurationAsync(messageView, invisibleDuration):
        pass
    def close():
        pass
class SimpleConsumerImpl(SimpleConsumer):
    log = LoggerFactory.getLogger("SimpleConsumerImpl")

    def __init__(self, clientConfiguration: ClientConfiguration, consumerGroup: str, awaitDuration: Duration,
             subscriptionExpressions: Map[str, FilterExpression]):
        self.consumerGroup = consumerGroup
        self.awaitDuration = awaitDuration
        self.topicIndex = AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE))
        self.subscriptionExpressions = subscriptionExpressions
        self.subscriptionRouteDataCache = ConcurrentHashMap()
        super().__init__(clientConfiguration, consumerGroup, subscriptionExpressions.keySet())
        groupResource = Resource(consumerGroup)
        self.simpleSubscriptionSettings = SimpleSubscriptionSettings(
            self.clientId, self.endpoints, groupResource,
            clientConfiguration.getRequestTimeout(), awaitDuration, subscriptionExpressions)

    def startUp(self):
        try:
            self.log.info("Begin to start the rocketmq simple consumer, clientId={}", self.clientId)
            super().startUp()
            self.log.info("The rocketmq simple consumer starts successfully, clientId={}", self.clientId)
        except Exception as t:
            self.log.error("Failed to start the rocketmq simple consumer, try to shutdown it, clientId={}",
                    self.clientId, exc_info=t)
            self.shutDown()
            raise t

    def shutDown(self):
        self.log.info("Begin to shutdown the rocketmq simple consumer, clientId={}", self.clientId)
        super().shutDown()
        self.log.info("Shutdown the rocketmq simple consumer successfully, clientId={}", self.clientId)

    def getConsumerGroup(self) -> str:
        return self.consumerGroup

    def subscribe(self, topic: str, filterExpression: FilterExpression) -> SimpleConsumer:
        if not self.isRunning():
            self.log.error("Unable to add subscription because simple consumer is not running, state={}, clientId={}",
                    self.state(), self.clientId)
            raise IllegalStateException("Simple consumer is not running now")
        future = self.getRouteData(topic)
        self.handleClientFuture(future)
        self.subscriptionExpressions[topic] = filterExpression
        return self

    def unsubscribe(self, topic):
        # Check consumer status.
        if not self.isRunning():
            log.error("Unable to remove subscription because simple consumer is not running, state={}, "
                "clientId={}".format(self.state(), clientId))
            raise IllegalStateException("Simple consumer is not running now")

        self.subscriptionExpressions.pop(topic)
        return self

    def getSubscriptionExpressions(self):
        return dict(self.subscriptionExpressions)

    def wrapHeartbeatRequest(self):
        return HeartbeatRequest.newBuilder().setGroup(self.getProtobufGroup()).setClientType(ClientType.SIMPLE_CONSUMER).build()

    def receive(self, maxMessageNum: int, invisibleDuration: Duration) -> List[MessageView]:
        future: ListenableFuture[List[MessageView]] = self.receive0(maxMessageNum, invisibleDuration)
        return self.handleClientFuture(future)

    def receiveAsync(self, maxMessageNum: int, invisibleDuration: Duration) -> CompletableFuture[List[MessageView]]:
        future: ListenableFuture[List[MessageView]] = self.receive0(maxMessageNum, invisibleDuration)
        return Futures.toCompletableFuture(future)

    def receive0(self, maxMessageNum: int, invisibleDuration: Duration) -> ListenableFuture[List[MessageView]]:
        if not self.isRunning():
            log.error("Unable to receive message because simple consumer is not running, state={}, clientId={}",
                      self.state(), self.clientId)
            e: IllegalStateException = IllegalStateException("Simple consumer is not running now")
            return Futures.immediateFailedFuture(e)

        if maxMessageNum <= 0:
            e: IllegalArgumentException = IllegalArgumentException("maxMessageNum must be greater than 0")
            return Futures.immediateFailedFuture(e)

        copy: dict = dict(self.subscriptionExpressions)
        topics: List[str] = list(copy.keys())

        if not topics:
            e: IllegalArgumentException = IllegalArgumentException("There is no topic to receive message")
            return Futures.immediateFailedFuture(e)

        topic: str = topics[self.topicIndex.getAndIncrement() % len(topics)]
        filterExpression: FilterExpression = copy[topic]

        routeFuture: ListenableFuture[SubscriptionLoadBalancer] = self.getSubscriptionLoadBalancer(topic)
        future0: ListenableFuture[ReceiveMessageResult] = Futures.transformAsync(routeFuture, lambda result: self.receiveMessage(
            self.wrapReceiveMessageRequest(maxMessageNum, result.takeMessageQueue(), filterExpression, invisibleDuration, self.awaitDuration),
            result.takeMessageQueue(),
            self.awaitDuration
        ), MoreExecutors.directExecutor())

        return Futures.transformAsync(future0, lambda result: Futures.immediateFuture(result.getMessageViews()),
                                       self.clientCallbackExecutor)
                    
    def ack(self, messageView: MessageView):
        future = self.ack0(messageView)
        self.handleClientFuture(future)

    def ackAsync(self, messageView: MessageView) -> Future:
        future = self.ack0(messageView)
        return concurrent.futures.Future().add_done_callback(lambda _: self.handleClientFuture(future))

    def ack0(self, messageView: MessageView) -> ListenableFuture:
        if not self.isRunning():
            error_message = "Unable to ack message because simple consumer is not running, state={}, clientId={}".format(
                self.state(), self.clientId
            )
            raise Exception(error_message)
        if not isinstance(messageView, MessageViewImpl):
            raise ValueError("Failed downcasting for messageView")
        impl = messageView
        future = self.ackMessage(impl)
        return Futures.transformAsync(
            future,
            partial(self.voidFuture, future),
            self.clientCallbackExecutor
        )

    def changeInvisibleDuration(self, messageView: MessageView, invisibleDuration: Duration) -> None:
        future = self.changeInvisibleDuration0(messageView, invisibleDuration)
        self.handleClientFuture(future)

    def changeInvisibleDurationAsync(self, messageView: MessageView, invisibleDuration: Duration) -> futures.Future:
        future = self.changeInvisibleDuration0(messageView, invisibleDuration)
        return FutureConverter.toCompletableFuture(future)

    def changeInvisibleDuration0(self, messageView: MessageView, invisibleDuration: Duration) -> ListenableFuture[None]:
        checkServiceState(self)
        if not isinstance(messageView, MessageViewImpl):
            exception = IllegalArgumentException("Failed downcasting for messageView")
            return Futures.immediateFailedFuture(exception)
        impl = messageView
        future = self.changeInvisibleDuration(impl, invisibleDuration)
        return Futures.transformAsync(future, lambda response: self.handleResponse(impl, response), MoreExecutors.directExecutor())

    def handleResponse(self, impl: MessageViewImpl, response: ChangeInvisibleDurationResponse) -> ListenableFuture[None]:
        impl.setReceiptHandle(response.getReceiptHandle())
        status = response.getStatus()
        StatusChecker.check(status, response)
        return Futures.immediateVoidFuture()

    def close(self) -> None:
        self.stopAsync().awaitTerminated()

    def getSettings(self) -> Settings:
        return self.simpleSubscriptionSettings

    def updateSubscriptionLoadBalancer(self, topic: str, topicRouteData: TopicRouteData) -> SubscriptionLoadBalancer:
        subscriptionLoadBalancer = self.subscriptionRouteDataCache.get(topic)
        subscriptionLoadBalancer = subscriptionLoadBalancer.update(topicRouteData) if subscriptionLoadBalancer is not None else SubscriptionLoadBalancer(topicRouteData)
        self.subscriptionRouteDataCache[topic] = subscriptionLoadBalancer
        return subscriptionLoadBalancer

    def onTopicRouteDataUpdate0(self, topic: str, topicRouteData: TopicRouteData) -> None:
        self.updateSubscriptionLoadBalancer(topic, topicRouteData)

    def getSubscriptionLoadBalancer(self, topic: str) -> ListenableFuture[SubscriptionLoadBalancer]:
        loadBalancer = self.subscriptionRouteDataCache.get(topic)
        if loadBalancer is not None:
            return Futures.immediateFuture(loadBalancer)
        future = getRouteData(topic)
        return Futures.transform(future, lambda routeData: self.updateSubscriptionLoadBalancer(topic, routeData), MoreExecutors.directExecutor())
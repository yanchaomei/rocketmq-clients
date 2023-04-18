from typing import Tuple
from datetime import timedelta
from rpcClient import RpcClient
from producer import Producer
from typing import Dict
from threading import Lock

'''

'''
class ClientManagerImpl:
    def __init__(self, client,channel,credentials,async_worker):
        self.client = client
        self.rpcClientTable: Dict[Endpoints, RpcClient] = {}
        self.rpcClientTableLock = Lock()
        self.RPC_CLIENT_MAX_IDLE_DURATION = 120000
        self.log = get_logger(__name__)
        self.channel = channel
        self.credentials = credentials
        self.async_worker = async_worker or futures.ThreadPoolExecutor()
    
    def startUp(self):
        clientId = self.client.getClientId()
        log.info("Begin to start the client manager, clientId={}".format(clientId))
        self.scheduler.schedule_with_fixed_delay(
            lambda: self.clearIdleRpcClients(),
            RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY.toNanos(),
            RPC_CLIENT_IDLE_CHECK_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        )

        self.scheduler.schedule_with_fixed_delay(
            lambda: self.client.doHeartbeat(),
            HEART_BEAT_INITIAL_DELAY.toNanos(),
            HEART_BEAT_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        )

        self.scheduler.schedule_with_fixed_delay(
            lambda: log.info("Start to log statistics, clientVersion={}, clientWrapperVersion={}, "
                                + "clientEndpoints={}, os description=[{}], java description=[{}], clientId={}".format(
                                    MetadataUtils.getVersion(), MetadataUtils.getWrapperVersion(), client.getEndpoints(),
                                    Utilities.getOsDescription(), Utilities.getJavaDescription(), clientId)) or self.client.doStats(),
            LOG_STATS_INITIAL_DELAY.toNanos(),
            LOG_STATS_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        )

        self.scheduler.schedule_with_fixed_delay(
            lambda: self.client.syncSettings(),
            SYNC_SETTINGS_DELAY.toNanos(),
            SYNC_SETTINGS_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        )
        log.info("The client manager starts successfully, clientId={}".format(clientId))


    def shutDown(self):
        clientId = self.client.getClientId()
        log.info("Begin to shutdown the client manager, clientId={}".format(clientId))
        self.scheduler.shutdown()
        try:
            if not ExecutorServices.awaitTerminated(self.scheduler):
                log.error("[Bug] Timeout to shutdown the client scheduler, clientId={}".format(clientId))
            else:
                log.info("Shutdown the client scheduler successfully, clientId={}".format(clientId))
            self.rpcClientTableLock.writeLock().lock()
            try:
                it = self.rpcClientTable.entrySet().iterator()
                while it.hasNext():
                    entry = it.next()
                    rpcClient = entry.getValue()
                    it.remove()
                    rpcClient.shutdown()
            finally:
                self.rpcClientTableLock.writeLock().unlock()
            log.info("Shutdown all rpc client(s) successfully, clientId={}".format(clientId))
            self.asyncWorker.shutdown()
            if not ExecutorServices.awaitTerminated(self.asyncWorker):
                log.error("[Bug] Timeout to shutdown the client async worker, clientId={}".format(clientId))
            else:
                log.info("Shutdown the client async worker successfully, clientId={}".format(clientId))
        except InterruptedException as e:
            log.error("[Bug] Unexpected exception raised while shutdown client manager, clientId={}".format(clientId), e)
            raise IOError(e)
        log.info("Shutdown the client manager successfully, clientId={}".format(clientId))


    def serviceName(self):
        return super().serviceName() + "-" + client.getClientId().getIndex()


    async def query_route(self, endpoints, request, duration):
        try:
            metadata = self.client.sign()
            context = Context(endpoints, metadata)
            rpc_client = self.get_rpc_client(endpoints)
            future = rpc_client.query_route(metadata, request, self.async_worker, duration)
            return RpcFuture(context, request, future)
        except Exception as e:
            return RpcFuture(e)

    async def heartbeat(self, endpoints: Tuple[str], request: HeartbeatRequest, duration: timedelta):
        try:
            metadata = self.client.sign()
            context = Context(endpoints, metadata)
            rpc_client = self.get_rpc_client(endpoints)
            future = rpc_client.heartbeat(metadata, request, async_worker, duration)
            return RpcFuture(context, request, future)
        except Exception as e:
            return RpcFuture(e)

    async def send_message(self, endpoints: Tuple[str], request: SendMessageRequest, duration: timedelta):
        try:
            metadata = self.client.sign()
            context = Context(endpoints, metadata)
            rpc_client = self.get_rpc_client(endpoints)
            future = rpc_client.send_message(metadata, request, async_worker, duration)
            return RpcFuture(context, request, future)
        except Exception as e:
            return RpcFuture(e)

    def getRpcClient(self, endpoints: Endpoints) -> RpcClient:
        rpcClient = None
        self.rpcClientTableLock.acquire_read()
        try:
            rpcClient = self.rpcClientTable.get(endpoints)
            if rpcClient is not None:
                return rpcClient
        finally:
            self.rpcClientTableLock.release()

        self.rpcClientTableLock.acquire_write()
        try:
            rpcClient = self.rpcClientTable.get(endpoints)
            if rpcClient is not None:
                return rpcClient

            try:
                rpcClient = RpcClientImpl(endpoints, self.client.isSslEnabled())
            except SSLException as e:
                self.log.error(f"Failed to get RPC client, endpoints={endpoints}, clientId={self.client.getClientId()}", exc_info=True)
                raise ClientException("Failed to generate RPC client") from e

            self.rpcClientTable[endpoints] = rpcClient
            return rpcClient
        finally:
            self.rpcClientTableLock.release()

    
    async def queryAssignment(self, endpoints, request, duration):
        try:
            metadata = self.client.sign()
            context = Context(endpoints, metadata)
            rpcClient = self.getRpcClient(endpoints)
            future = rpcClient.queryAssignment(metadata, request, self.asyncWorker, duration)
            return RpcFuture(context, request, future)
        except Exception as e:
            return RpcFuture(e)
    
    async def receive_message(self, endpoints: Endpoints, request: ReceiveMessageRequest,
                          duration: Duration) -> Future[MessageMap[List[ReceiveMessageResponse]]]:
        try:
            metadata = client.sign()
            context = Context(endpoints, metadata)
            rpc_client = get_rpc_client(endpoints)
            future = rpc_client.receive_message(metadata, request, async_worker, duration)
            return RpcFuture(context, request, future)
        except Exception as e:
            return RpcFuture(e)

    async def ack_message(endpoints: Endpoints, request: AckMessageRequest, duration: Duration) -> RpcFuture[AckMessageRequest, AckMessageResponse]:
        try:
            metadata = client.sign()
            context = Context(endpoints, metadata)
            rpc_client = get_rpc_client(endpoints)
            future = rpc_client.ack_message(metadata, request, async_worker, duration)
            return RpcFuture(context, request, future)
        except Exception as e:
            return RpcFuture(e)

    async def change_invisible_duration(endpoints: Endpoints, request: ChangeInvisibleDurationRequest, duration: Duration) -> RpcFuture[ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse]:
        try:
            metadata = client.sign()
            context = Context(endpoints, metadata)
            rpc_client = get_rpc_client(endpoints)
            future = rpc_client.change_invisible_duration(metadata, request, async_worker, duration)
            return RpcFuture(context, request, future)
        except Exception as e:
            return RpcFuture(e)

    async def forward_message_to_dead_letter_queue(endpoints: Endpoints, request: ForwardMessageToDeadLetterQueueRequest, duration: Duration) -> RpcFuture[ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse]:
        try:
            metadata = client.sign()
            context = Context(endpoints, metadata)
            rpc_client = get_rpc_client(endpoints)
            future: Future[ForwardMessageToDeadLetterQueueResponse] = rpc_client.forward_message_to_dead_letter_queue(metadata, request, async_worker, duration)
            return RpcFuture(context, request, future)
        except Exception as t:
            return RpcFuture(t)

    def end_transaction(self, endpoints: Endpoints, request: EndTransactionRequest, duration: Duration) -> RpcFuture[EndTransactionRequest, EndTransactionResponse]:
        try:
            metadata = client.sign()
            context = Context(endpoints, metadata)
            rpc_client = get_rpc_client(endpoints)
            future = rpc_client.end_transaction(metadata, request, async_worker, duration)
            return RpcFuture(context, request, future)
        except Exception as t:
            return RpcFuture(t)

    def notify_client_termination(self, endpoints: Endpoints, request: NotifyClientTerminationRequest, duration: Duration) -> RpcFuture[NotifyClientTerminationRequest, NotifyClientTerminationResponse]:
        try:
            metadata = client.sign()
            context = Context(endpoints, metadata)
            rpc_client = get_rpc_client(endpoints)
            future= rpc_client.notify_client_termination(metadata, request, async_worker, duration)
            return RpcFuture(context, request, future)
        except Exception as t:
            return RpcFuture(t)

    def telemetry(self, endpoints: Endpoints, duration: Duration, response_observer: StreamObserver[TelemetryCommand]) -> StreamObserver[TelemetryCommand]:
        metadata = self.sign()
        rpc_client = self.get_rpc_client(endpoints)
        return rpc_client.telemetry(metadata, self.async_worker, duration, response_observer)

    def get_scheduler(self) -> ScheduledExecutorService:
        return self.scheduler

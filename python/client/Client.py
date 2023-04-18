import logging
import concurrent.futures
import threading
import collections
import time

class Client:
    TELEMETRY_TIMEOUT = 60 * 365 * 86400

    def __init__(self, clientConfiguration, topics):
        super().__init__()
        self.clientConfiguration = clientConfiguration
        self.endpoints = Endpoints(clientConfiguration.getEndpoints())
        self.topics = topics
        self.isolated = collections.newSetFromMap(collections.ConcurrentHashMap())
        self.clientCallbackExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=threading.cpu_count())
        self.clientMeterManager = ClientMeterManager(ClientId(), clientConfiguration)
        self.telemetryCommandExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.clientId = self.clientMeterManager.getClientId()
        self.clientManager = ClientManagerImpl(self)
        self.topicRouteCache = collections.ConcurrentHashMap()
        self.inflightRouteFutureTable = collections.defaultdict(set)
        self.inflightRouteFutureLock = threading.Lock()
        self.sessionsTable = collections.defaultdict(ClientSessionImpl)
        self.sessionsLock = threading.RLock()
        self.compositedMessageInterceptor = CompositedMessageInterceptor(
            [MessageMeterInterceptor(self, self.clientMeterManager)]
        )
        self.updateRouteCacheFuture = None

    def start_up(self) -> None:
        log.info("Begin to start the rocketmq client, clientId={}", self.clientId)
        self.clientManager.start_async().await_running()
        # Fetch topic route from remote.
        log.info("Begin to fetch topic(s) route data from remote during client startup, clientId={}, topics={}",
            self.clientId, self.topics)
        for topic in self.topics:
            future = self.fetch_topic_route(topic)
            future.result()
        log.info("Fetch topic route data from remote successfully during startup, clientId={}, topics={}",
            self.clientId, self.topics)
        # Update route cache periodically.
        scheduler = self.clientManager.get_scheduler()
        self.updateRouteCacheFuture = scheduler.schedule_with_fixed_delay(
            lambda: self.update_route_cache(),
            10,
            30,
            TimeUnit.SECONDS
        )
        log.info("The rocketmq client starts successfully, clientId={}", self.clientId)

    def shut_down(self):
        log.info("Begin to shutdown the rocketmq client, clientId={}", self.clientId)
        self.notify_client_termination()
        if self.updateRouteCacheFuture is not None:
            self.updateRouteCacheFuture.cancel(False)
        self.telemetryCommandExecutor.shutdown()
        if not ExecutorServices.awaitTerminated(self.telemetryCommandExecutor):
            log.error("[Bug] Timeout to shutdown the telemetry command executor, clientId={}", self.clientId)
        else:
            log.info("Shutdown the telemetry command executor successfully, clientId={}", self.clientId)
        log.info("Begin to release all telemetry sessions, clientId={}", self.clientId)
        self.release_client_sessions()
        log.info("Release all telemetry sessions successfully, clientId={}", self.clientId)
        self.clientManager.stopAsync().awaitTerminated()
        self.clientCallbackExecutor.shutdown()
        if not ExecutorServices.awaitTerminated(self.clientCallbackExecutor):
            log.error("[Bug] Timeout to shutdown the client callback executor, clientId={}", self.clientId)
        self.clientMeterManager.shutdown()
        log.info("Shutdown the rocketmq client successfully, clientId={}", self.clientId)

    def updateRouteCache(self):
        pass

    def onSessionConnected(self, session):
        pass

    def onSessionDisconnected(self, session):
        pass

    def onMessage(self, message):
        pass

    def do_before(context, general_messages):
        try:
            composited_message_interceptor.do_before(context, general_messages)
        except Exception as e:
            # Should never reach here.
            logging.error(f"[Bug] Exception raised while handling messages, clientId={client_id}", exc_info=True)

    def do_after(self, context, general_messages):
        try:
            self.composited_message_interceptor.do_after(context, general_messages)
        except Exception as e:
            log.error("[Bug] Exception raised while handling messages, clientId={}", self.client_id, e)

    def settings_command(self):
        settings = self.get_settings().to_protobuf()
        return TelemetryCommand(settings=settings)

    def telemetry(self, endpoints: Endpoints, observer: StreamObserver[TelemetryCommand]) -> None:
        try:
            return self.client_manager.telemetry(endpoints, TELEMETRY_TIMEOUT, observer)
        except ClientException as e:
            raise e
        except Exception as e:
            raise InternalErrorException(e)

    def is_endpoints_deprecated(self, endpoints: Endpoints) -> bool:
        total_route_endpoints = self.get_total_route_endpoints()
        return endpoints not in total_route_endpoints

    def on_print_thread_stack_trace_command(self, endpoints: Endpoints, command: PrintThreadStackTraceCommand):
        nonce = command.nonce
        task = lambda: self._print_thread_stack_trace(endpoints, nonce)
        try:
            self.telemetry_command_executor.submit(task)
        except Exception as e:
            log.error("[Bug] Exception raised while submitting task to print thread stack trace, endpoints={}, "
                + "nonce={}, clientId={}", endpoints, nonce, self.client_id, e)

    def get_settings(self) -> Settings:
        pass

    def on_settings_command(self, endpoints: Endpoints, settings: apache.rocketmq.v2.Settings) -> None:
        metric = Metric(settings.metric)
        client_meter_manager.reset(metric)
        self.get_settings().sync(settings)

    def sync_settings(self) -> None:
        settings = self.get_settings().to_protobuf()
        command = TelemetryCommand(settings=settings)
        total_route_endpoints = self.get_total_route_endpoints()
        for endpoints in total_route_endpoints:
            try:
                self.telemetry(endpoints, command)
            except Exception as e:
                log.error("Failed to telemeter settings, clientId={}, endpoints={}", self.client_id, endpoints, e)

    def telemetry(self, endpoints: Endpoints, command: TelemetryCommand) -> None:
        try:
            client_session = self.get_client_session(endpoints)
            client_session.write(command)
        except Exception as e:
            log.error("Failed to fire write telemetry command, clientId={}, endpoints={}", self.client_id, endpoints, e)

    def release_client_sessions(self) -> None:
        with self.sessions_lock:
            for session in self.sessions_table.values():
                session.release()

    def remove_client_session(self, endpoints: Endpoints, client_session: ClientSessionImpl) -> None:
        with self.sessions_lock:
            log.info("Remove client session, clientId={}, endpoints={}", self.client_id, endpoints)
            self.sessions_table.pop(endpoints, None)

    
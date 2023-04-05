
class rpcClient:
    def __init__(self, endpoints, sslEnabled):
        self.Logger = MqLogManager.Instance.GetCurrentClassLogger()
        self._target = endpoints.GrpcTarget(sslEnabled)
        self._channel = GrpcChannel.ForAddress(self._target, GrpcChannelOptions(
            HttpHandler = CreateHttpHandler()
        ))
        invoker = self._channel.Intercept(ClientLoggerInterceptor())
        self._stub = Proto.MessagingService.MessagingServiceClient(invoker)

    async def Shutdown(self):
        if self._channel is not None:
            await self._channel.ShutdownAsync()
    def idleDuration():
        pass
    def shutdown():
        pass
    def queryRoute(metadata, request, executor, duration):
        pass
    def heartbeat( metadata, request,executor, duration):
        pass
    def sendMessage(metadata, request, executor, duration):
        pass
    def queryAssignment( metadata,  request,executor,  duration):
        pass
    def receiveMessage( metadata,request,  executor,  duration):
        pass
    def ackMessage( metadata,  request,executor,  duration):
        pass
    def changeInvisibleDuration( metadata,request,  executor,  duration):
        pass
    def forwardMessageToDeadLetterQueue(metadata,  request,  executor,  duration):
        pass
    def endTransaction( metadata,request,  executor,  duration):
        pass
    def notifyClientTermination( metadata,request,  executor,  duration):
        pass
    def telemetry( metadata,  executor,  duration,responseObserver):
        pass
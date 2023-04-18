import grpc
import time
import threading
import logging
from grpc_metadata import Metadata, MetadataUtils
from concurrent import futures
import protocol.service_pb2 as service_pb2
import protocol.service_pb2_grpc as service_pb2_grpc

class RpcClient:
    CONNECT_TIMEOUT_MILLIS = 3 * 1000
    GRPC_MAX_MESSAGE_SIZE = 2**31 - 1

    def __init__(self, endpoints, ssl_enabled):
        channel_builder = grpc.insecure_channel if not ssl_enabled else grpc.secure_channel
        self.channel = channel_builder(endpoints.grpc_target, 
            options=[('grpc.max_send_message_length', self.GRPC_MAX_MESSAGE_SIZE),
                     ('grpc.max_receive_message_length', self.GRPC_MAX_MESSAGE_SIZE),
                     ('grpc.connect_timeout_ms', self.CONNECT_TIMEOUT_MILLIS)])
        self.future_stub = service_pb2_grpc.MessagingServiceFutureStub(self.channel)
        self.stub = service_pb2_grpc.MessagingServiceStub(self.channel)
        self.activity_nano_time = time.monotonic_ns()

    def idle_duration(self):
        return time.monotonic_ns() - self.activity_nano_time

    def shutdown(self):
        self.channel.close()

    def query_route(self, metadata, request, executor, duration):
        self.activity_nano_time = time.monotonic_ns()
        headers_interceptor = grpc.metadata_call_credentials(metadata)
        options = (('grpc.timeout', duration),)
        context = grpc.create_channel_context(executor)
        context = context.with_call_credentials(headers_interceptor).with_options(options)
        return self.future_stub.QueryRoute.future(request, context=context)

    def heartbeat(self, metadata, request, executor, duration):
        self.activity_nano_time = time.monotonic_ns()
        headers_interceptor = grpc.metadata_call_credentials(metadata)
        options = (('grpc.timeout', duration),)
        context = grpc.create_channel_context(executor)
        context = context.with_call_credentials(headers_interceptor).with_options(options)
        return self.future_stub.Heartbeat.future(request, context=context)

    # other methods can be similarly implemented

    def send_message(self, metadata, request, executor, duration):
        self.activity_nano_time = time.monotonic_ns()
        return self.future_stub.sendMessage(request, metadata=metadata, timeout=duration)

    def query_assignment(self, metadata, request, executor, duration):
        self.activity_nano_time = time.monotonic_ns()
        headers_interceptor = grpc.metadata_call_credentials(metadata)
        options = (('grpc.timeout', duration),)
        context = grpc.create_channel_context(executor)
        context = context.with_call_credentials(headers_interceptor).with_options(options)
        return self.future_stub.QueryAssignment(request, context=context)

    def receive_message(self, metadata, request, executor, duration):
        self.activity_nano_time = time.monotonic_ns()
        headers_interceptor = grpc.metadata_call_credentials(metadata)
        options = (('grpc.timeout', duration),)
        context = grpc.create_channel_context(executor)
        context = context.with_call_credentials(headers_interceptor).with_options(options)
        future = futures.Future()
        responses = []

        def on_response(response):
            responses.append(response)

        def on_error(e):
            future.set_exception(e)

        def on_completed():
            future.set_result(responses)

        stream = self.stub.ReceiveMessage(request, context=context)
        stream.add_done_callback(on_completed)
        stream.add_callback(on_response)
        stream.add_errback(on_error)
        return future

    def ack_message(self, metadata, request, executor, duration):
        self.activity_nano_time = time.monotonic_ns()
        headers_interceptor = grpc.metadata_call_credentials(metadata)
        options = (('grpc.timeout', duration),)
        context = grpc.create_channel_context(executor)
        context = context.with_call_credentials(headers_interceptor).with_options(options)
        return self.future_stub.AckMessage(request, context=context)

    def change_invisible_duration(self, metadata, request, executor, duration):
        self.activity_nano_time = time.monotonic_ns()
        headers_interceptor = grpc.metadata_call_credentials(metadata)
        options = (('grpc.timeout', duration),)
        context = grpc.create_channel_context(executor)
        context = context.with_call_credentials(headers_interceptor).with_options(options)
        return self.future_stub.ChangeInvisibleDuration(request, context=context)

    def forward_message_to_dead_letter_queue(self, metadata, request, executor, duration):
        self.activity_nano_time = time.monotonic_ns()
        headers_interceptor = grpc.metadata_call_credentials(metadata)
        options = (('grpc.timeout', duration),)
        context = grpc.create_channel_context(executor)
        context = context.with_call_credentials(headers_interceptor).with_options(options)
        return self.future_stub.ForwardMessageToDeadLetterQueue(request, context=context)
    
    def endTransaction(self, metadata, request, executor, duration):
        self.activity_nano_time = time.monotonic_ns()
        headers_interceptor = grpc.metadata_call_credentials(metadata)
        options = (('grpc.timeout', duration),)
        context = grpc.create_channel_context(executor)
        context = context.with_call_credentials(headers_interceptor).with_options(options)
        return self.future_stub.endTransaction(request, context=context)
    

    def notifyClientTermination(self, metadata, request, executor, duration):
        self.activity_nano_time = time.monotonic_ns()
        headers_interceptor = grpc.metadata_call_credentials(metadata)
        options = (('grpc.timeout', duration),)
        context = grpc.create_channel_context(executor)
        context = context.with_call_credentials(headers_interceptor).with_options(options)
        return self.future_stub.notifyClientTermination(request, context=context)

    def telemetry(self, metadata, executor, duration, response_observer):
        interceptor = MetadataUtils.newAttachHeadersInterceptor(metadata)
        stub0 = self.stub.withInterceptors(interceptor).withExecutor(executor).withDeadlineAfter(duration.toNanos(), TimeUnit.NANOSECONDS)
        return stub0.telemetry(response_observer)
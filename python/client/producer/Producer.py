from message.Message import Message
from concurrent.futures import Future
from typing import Set
import logging
from Client import Client

class Producer(Client):
    def __init__():
        pass
    def sendSync(message):
        pass
    def sendTxn(message, transaction):
        pass
    def sendAsync(message):
        pass
    def beginTransaction():
        pass
    def close():
        pass


class ProducerImpl(Producer):
    log = logging.getLogger(__name__)

    def __init__(self, clientConfiguration, topics: Set[str], maxAttempts: int, checker: TransactionChecker):
        super().__init__(clientConfiguration, topics)
        retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(maxAttempts)
        self.publishingSettings = PublishingSettings(self.clientId, self.endpoints, retryPolicy,
                                                      self.clientConfiguration.getRequestTimeout(), topics)
        self.checker = checker
        self.publishingRouteDataCache = {}
    
    def send(self, messages: List[Message], tx_enabled: bool) -> SettableFuture[List[SendReceiptImpl]]:
        future = SettableFuture.create()

        # Check producer state before message publishing.
        if not self.is_running():
            e = IllegalStateException("Producer is not running now")
            future.set_exception(e)
            log.error("Unable to send message because producer is not running, state={}, clientId={}"
                      .format(self.state(), self.client_id))
            return future

        pub_messages = ImmutableList.Builder()
        for message in messages:
            try:
                pub_message = PublishingMessageImpl(message, self.publishing_settings, tx_enabled)
                pub_messages.add(pub_message)
            except Exception as e:
                # Failed to refine message, no need to proceed.
                log.error("Failed to refine message to send, clientId={}, message={}".format(self.client_id, message),
                          e)
                future.set_exception(e)
                return future

        # Collect topics to send message.
        topics = {message.get_topic() for message in pub_messages.build()}
        if len(topics) > 1:
            # Messages have different topics, no need to proceed.
            e = IllegalArgumentException("Messages to send have different topics")
            future.set_exception(e)
            log.error("Messages to be sent have different topics, no need to proceed, topic(s)={}, clientId={}"
                      .format(topics, self.client_id))
            return future

        topic = next(iter(topics))
        # Collect message types.
        message_types = {pub_message.get_message_type() for pub_message in pub_messages.build()}
        if len(message_types) > 1:
            # Messages have different message type, no need to proceed.
            e = IllegalArgumentException("Messages to send have different types, please check")
            future.set_exception(e)
            log.error("Messages to be sent have different message types, no need to proceed, topic={}, "
                      "messageType(s)={}, clientId={}".format(topic, message_types, self.client_id), e)
            return future

        message_type = next(iter(message_types))
        message_group = None

        # Message group must be same if message type is FIFO, or no need to proceed.
        if message_type == MessageType.FIFO:
            message_groups = {pub_message.get_message_group().get() for pub_message in pub_messages.build()
                              if pub_message.get_message_group().isPresent()}
            if len(message_groups) > 1:
                e = IllegalArgumentException("FIFO messages to send have different message groups, "
                                               "messageGroups={}".format(message_groups))
                future.set_exception(e)
                log.error("FIFO messages to be sent have different message groups, no need to proceed, topic={}, "
                          "messageGroups={}, clientId={}".format(topic, message_groups, self.client_id), e)
                return future
            message_group = next(iter(message_groups))

        self.topics.add(topic)
        # Get publishing topic route.
        route_future = self.get_publishing_load_balancer(topic)
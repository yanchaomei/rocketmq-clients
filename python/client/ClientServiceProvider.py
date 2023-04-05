from producer.Producer import ProducerBuilderImpl
from consumer.PushConsumer import PushConsumerBuilderImpl
from consumer.SimpleConsumer import SimpleConsumerBuilderImpl
from message.Message import MessageBuilderImpl

from abc import ABC, abstractmethod

class ClientServiceProvider(ABC):
    @staticmethod
    @abstractmethod
    def load_service():
        pass

    @abstractmethod
    def new_producer_builder(self):
        pass

    @abstractmethod
    def new_message_builder(self):
        pass

    @abstractmethod
    def new_push_consumer_builder(self):
        pass

    @abstractmethod
    def new_simple_consumer_builder(self):
        pass


class ClientServiceProviderImpl(ClientServiceProvider):
    def newProducerBuilder(self):
        return ProducerBuilderImpl()

    def newPushConsumerBuilder(self):
        return PushConsumerBuilderImpl()

    def newSimpleConsumerBuilder(self):
        return SimpleConsumerBuilderImpl()

    def newMessageBuilder(self):
        return MessageBuilderImpl()

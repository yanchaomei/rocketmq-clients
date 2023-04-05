
class Message:
    def __init__():
        pass
    def getTopic():
        pass
    def getBody():
        pass
    def getProperties():
        pass
    def getTag():
        pass
    def getKeys():
        pass
    def getMessageGroup():
        pass
    def getDeliveryTimestamp():
        pass

class MessageImpl(Message):
    def __init__(self, messageVars):
        self.keys = messageVars["keys"]
        self.body = messageVars["body"]
        self.topic = messageVars["topic"]
        self.tag = messageVars["tag"]
        self.messageGroup = messageVars["messageGroup"]
        self.deliveryTimestamp = messageVars["deliveryTimestamp"]
        self.properties = messageVars["properties"]

    def getTopic(self):
        return self.topic

    def getBody(self):
        return self.body

    def getProperties(self):
        return self.properties

    def getTag(self):
        return self.tag

    def getKeys(self):
        return self.keys

    def getMessageGroup(self):
        return self.messageGroup

    def getDeliveryTimestamp(self):
        return self.deliveryTimestamp


class MessageBuilder:
    def __init__(self):
        self.topic = None
        self.body = None
        self.tag = None
        self.messageGroup = None
        self.deliveryTimestamp = None
        self.keys = None
        self.properties = None

    def setTopic(topic):
        pass
    def setBody(body):
        pass
    def setTopic(topic):
        pass
    def setTag(tag):
        pass
    def setKeys(keys):
        pass
    def setMessageGroup(messageGroup):
        pass
    def setDeliveryTimestamp(deliveryTimestamp):
        pass
    def addProperty(key, value):
        pass
    def build():
        message = MessageImpl() # pass the varibles 
        return message

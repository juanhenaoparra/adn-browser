from consumer.consumer import ConsumerType
from consumer.email import EmailConsumer

def NewConsumer(type: ConsumerType):
    if type == ConsumerType.EMAIL:
        return EmailConsumer()
from abc import ABC, abstractmethod
from enum import Enum
from pydantic import BaseModel


class ConsumerType(Enum):
    EMAIL = "email"


class Consumer(BaseModel, ABC):
    type: ConsumerType

    @abstractmethod
    def send(self, properties, body):
        pass

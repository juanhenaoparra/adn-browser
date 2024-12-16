import pika
import json
import os
from typing import Any, Dict
from dotenv import load_dotenv

load_dotenv()


class Publisher:
    def __init__(self):
        self.host = os.getenv("RMQ_HOST", "localhost")
        self.port = int(os.getenv("RMQ_PORT", "5673"))
        self.user = os.getenv("RMQ_USER", "admin")
        self.password = os.getenv("RMQ_PASS", "admin")
        self.queue_name = "event.drivent.email"

    def publish(self, message: Dict[str, Any]) -> None:
        """
        Publish a message to RabbitMQ queue

        Args:
            message: Dictionary containing the message to be published
        """
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host, port=self.port, credentials=credentials
        )
        
        connection = pika.BlockingConnection(parameters)
        
        channel = connection.channel()

        # Declare the queue (creates if doesn't exist)
        channel.queue_declare(queue=self.queue_name, durable=True)

        # Convert message to JSON string
        message_body = json.dumps(message)

        # Publish the message
        channel.basic_publish(
            exchange="event.drivent.exchange",
            routing_key="email",
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=1,
            ),
        )

        connection.close()

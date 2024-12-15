import argparse
import os
from dotenv import load_dotenv

from consumer.consumer import ConsumerType
from consumer.new import NewConsumer
from myqueue.queue import QueueHandler, QueueCredentials

load_dotenv()

config = {
    ConsumerType.EMAIL: {
        "queue": "event.drivent.email",
        "exchange": "event.drivent.exchange",
        "routing_key": "email",
    }
}


def main():
    queue_handler = None

    try:
        parser = argparse.ArgumentParser(description="Notification Consumer Fallback")
        parser.add_argument(
            "type", help="Type of the consumer", default=ConsumerType.EMAIL.value
        )
        args = parser.parse_args()

        consumer_type = ConsumerType(args.type)
        consumer = NewConsumer(consumer_type)

        queue_credentials = QueueCredentials(
            user=os.getenv("RMQ_USER"),
            passw=os.getenv("RMQ_PASS"),
        )

        queue_handler = QueueHandler(
            credentials=queue_credentials,
            host=os.getenv("RMQ_HOST"),
            port=os.getenv("RMQ_PORT"),
            queue=config[consumer_type]["queue"],
            exchange_name=config[consumer_type]["exchange"],
            routing_key=config[consumer_type]["routing_key"],
            on_message=consumer.send,
        )

        queue_handler.suscribe()
    except Exception as e:
        print("Fatal error: ", str(e))

        if queue_handler:
            queue_handler.close()


if __name__ == "__main__":
    main()

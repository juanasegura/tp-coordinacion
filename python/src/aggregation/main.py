import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_client = {}
        self.eofs_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        fruits = self.amount_by_client.setdefault(client_id, {})
        fruits[fruit] = fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))


    def _process_eof(self, client_id):
        logging.info(f"Received EOF of client {client_id}")
        received = self.eofs_by_client.get(client_id, 0) + 1
        if received < SUM_AMOUNT:
            self.eofs_by_client[client_id] = received
            return
        self.eofs_by_client.pop(client_id, None)
        fruits = self.amount_by_client.pop(client_id, {})
        fruit_top = sorted(fruits.values())[::-1][:TOP_SIZE]
        self.output_queue.send(
            message_protocol.internal.serialize(
                [client_id, [[i.fruit, i.amount] for i in fruit_top]]
            )
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def _handle_sigterm(self):
        logging.info("Received SIGTERM signal")
        self.input_exchange.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, lambda signum, frame: self._handle_sigterm())
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            self.input_exchange.close()
            self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()

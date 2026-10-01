import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.chunks_by_client = {}

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        [client_id, fruit_top_chunk] = message_protocol.internal.deserialize(message)
        state = self.chunks_by_client.setdefault(
            client_id, {"fruits": {}, "chunks": 0}
        )
        for [fruit, amount] in fruit_top_chunk:
            fruits = state["fruits"]
            fruits[fruit] = fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
        state["chunks"] += 1

        if state["chunks"] == AGGREGATION_AMOUNT:
            del self.chunks_by_client[client_id]
            fruit_top = sorted(state["fruits"].values())[::-1][:TOP_SIZE]
            self.output_queue.send(
                message_protocol.internal.serialize(
                    [client_id, [[i.fruit, i.amount] for i in fruit_top]]
                )
            )
        ack()

    def _handle_sigterm(self):
        logging.info("Received SIGTERM signal")
        self.input_queue.stop_consuming()

    def start(self):
        signal.signal(signal.SIGTERM, lambda signum, frame: self._handle_sigterm())
        try:
            self.input_queue.start_consuming(self.process_messsage)
        finally:
            self.input_queue.close()
            self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()

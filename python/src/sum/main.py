import os
import logging
import threading
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"]
        )
        self.control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)],
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.lock = threading.Lock()
        self.amount_by_client = {} ## diccionario tipo {cliente: {fruta: }}
        self.records_total = {}  # {cliente: n}   n del EOF, y además flag "ya estoy en cierre"
        self.local_count = {}  # {cliente: int}  registros recibidos y todavía no anunciados
        self.global_count = {}  # {cliente: int}  suma de todos los COUNT que me llegaron
        self.opened = set()  # clientes cuyo OPEN ya procesé

    def _get_target_aggregation(self, fruit):
        return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT

    # Publica la cantidad de mensajes recibido por un cliente cuyo eof ya fue anunciado
    def _publish_count(self, client_id, amount):
        self.control_output.send(
            message_protocol.internal.serialize(
                {"type": "count", "client": client_id, "amount": amount}
            )
        )

    # Publica en el exchange cuando recive el EOF de un cliente, con la cantidad total de mensajes de ese cliente
    def _publish_open(self, client_id, records):
        self.control_output.send(
            message_protocol.internal.serialize(
                {"type": "open", "client": client_id, "records": records}
            )
        )

    def _open_client(self, client_id, n):
        self.records_total[client_id] = n
        self.global_count.setdefault(client_id, 0)
        self.opened.add(client_id)
        return self.local_count.pop(client_id, 0)

    def _check_barrier(self, client_id):
        if client_id not in self.opened:
            return
        if self.global_count.get(client_id, 0) < self.records_total[client_id]:
            return
        self._flush(client_id)

    def _flush(self, client_id):
        for fruit, final_fruit_item in self.amount_by_client.pop(client_id, {}).items():
            self.data_output_exchanges[self._get_target_aggregation(fruit)].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize([client_id])
            )
        self.records_total.pop(client_id, None)
        self.global_count.pop(client_id, None)
        self.local_count.pop(client_id, None)
        self.opened.discard(client_id)

    def _process_data(self, client_id, fruit, amount):
        with self.lock:
            fruits = self.amount_by_client.setdefault(client_id, {})
            fruits[fruit] = fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            if client_id in self.records_total:
                self._publish_count(client_id, 1)
            else:
                self.local_count[client_id] = self.local_count.get(client_id, 0) + 1

# me falta loggear el eof
    def _process_eof(self, client_id, records):
        with self.lock:
            if client_id in self.records_total:
                return
            pending = self._open_client(client_id, records)
            self._publish_open(client_id, records)
            if pending:
                self._publish_count(client_id, pending)

    def _process_open(self, client_id, records):
        with self.lock:
            if client_id not in self.opened:
                pending = self._open_client(client_id, records)
                if pending:
                    self._publish_count(client_id, pending)
            self._check_barrier(client_id)

    def _process_count(self, client_id, amount):
        with self.lock:
            if client_id not in self.opened:
                return
            self.global_count[client_id] = self.global_count.get(client_id, 0) + amount
            self._check_barrier(client_id)

    def _on_control_message(self, message, ack, nack):
        msg = message_protocol.internal.deserialize(message)
        if msg["type"] == "open":
            self._process_open(msg["client"], msg["records"])
        else:
            self._process_count(msg["client"], msg["amount"])
        ack()

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        threading.Thread(
            target=self.control_input.start_consuming,
            args=[self._on_control_message],
            daemon=True,
        ).start()
        self.input_queue.start_consuming(self.process_data_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()

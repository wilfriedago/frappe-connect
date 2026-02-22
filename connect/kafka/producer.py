"""Kafka Producer client wrapper."""

from connect.utils.logging import log_error, log_info


class KafkaProducerClient:
	"""Wraps confluent-kafka Producer with delivery tracking."""

	def __init__(self, config: dict):
		from confluent_kafka import Producer

		self._producer = Producer(config)
		self._delivery_result = None

	def produce(
		self,
		topic: str,
		value: bytes,
		key: str | None = None,
		headers: dict | None = None,
	) -> dict | None:
		"""Produce a message to Kafka and flush.

		Returns delivery metadata dict with partition/offset on success,
		or raises an exception on failure.
		"""
		self._delivery_result = None

		kafka_headers = []
		if headers:
			kafka_headers = [(k, v.encode() if isinstance(v, str) else v) for k, v in headers.items()]

		self._producer.produce(
			topic=topic,
			key=key,
			value=value,
			headers=kafka_headers or None,
			on_delivery=self._on_delivery,
		)
		self._producer.flush(timeout=30)

		if self._delivery_result and self._delivery_result.get("error"):
			raise Exception(f"Kafka delivery failed: {self._delivery_result['error']}")

		return self._delivery_result

	def _on_delivery(self, err, msg):
		"""Callback for delivery confirmation."""
		if err:
			self._delivery_result = {"error": str(err)}
			log_error("Kafka delivery failed", str(err))
		else:
			self._delivery_result = {
				"topic": msg.topic(),
				"partition": msg.partition(),
				"offset": msg.offset(),
				"key": msg.key().decode() if msg.key() else None,
			}
			log_info(
				"Kafka message delivered",
				f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}",
			)

	def flush(self, timeout: float = 30):
		"""Flush pending messages."""
		self._producer.flush(timeout=timeout)

	def close(self):
		"""Flush and close the producer."""
		self._producer.flush(timeout=10)

"""Kafka Consumer client wrapper."""
import signal
import time

import frappe

from connect.utils.logging import log_error, log_info


class KafkaConsumerClient:
    """Wraps confluent-kafka Consumer with graceful shutdown and message processing."""

    def __init__(self, config: dict, topics: list[str]):
        from confluent_kafka import Consumer

        self._consumer = Consumer(config)
        self._topics = topics
        self._running = False

    def start(self, message_handler, poll_timeout: float = 1.0, max_messages: int = 0):
        """Start the consumer poll loop.

        Args:
            message_handler: Callable(msg) -> None. Called for each message.
            poll_timeout: Seconds to wait for messages in each poll.
            max_messages: Stop after N messages (0 = unlimited).
        """
        self._running = True
        self._setup_signal_handlers()
        self._consumer.subscribe(self._topics)

        log_info("Consumer started", f"topics={self._topics}")
        message_count = 0

        try:
            while self._running:
                msg = self._consumer.poll(timeout=poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    self._handle_error(msg)
                    continue

                try:
                    message_handler(msg)
                    self._consumer.commit(message=msg)
                    message_count += 1

                    if max_messages > 0 and message_count >= max_messages:
                        log_info("Consumer max messages reached", f"count={message_count}")
                        break

                except Exception as e:
                    log_error(
                        "Consumer message processing failed",
                        str(e),
                        exc=e,
                    )
                    # Commit offset to move past the problematic message
                    self._consumer.commit(message=msg)

        except Exception as e:
            log_error("Consumer loop error", str(e), exc=e)
        finally:
            self._shutdown()

    def _handle_error(self, msg):
        """Handle Kafka consumer errors."""
        from confluent_kafka import KafkaError

        error = msg.error()
        if error.code() == KafkaError._PARTITION_EOF:
            # End of partition — not an error
            return
        log_error("Kafka consumer error", f"code={error.code()} reason={error.str()}")

    def _setup_signal_handlers(self):
        """Set up graceful shutdown on SIGTERM and SIGINT."""

        def _signal_handler(signum, frame):
            log_info("Consumer shutdown signal received", f"signal={signum}")
            self._running = False

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

    def _shutdown(self):
        """Clean shutdown of the consumer."""
        log_info("Consumer shutting down", "closing consumer")
        try:
            self._consumer.close()
        except Exception as e:
            log_error("Consumer close error", str(e), exc=e)

    def stop(self):
        """Stop the consumer loop."""
        self._running = False

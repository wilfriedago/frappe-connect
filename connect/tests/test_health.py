"""Tests for health check utilities.

Uses mocks to simulate Kafka and Schema Registry connection results.
"""
import unittest
from unittest.mock import MagicMock, patch


class TestCheckKafkaHealth(unittest.TestCase):
    """Test check_kafka_health."""

    @patch("connect.utils.health.log_error")
    def test_healthy_kafka(self, mock_log_error):
        """Returns ok when Kafka broker is reachable."""
        from connect.utils.health import check_kafka_health

        config = {"bootstrap.servers": "localhost:9092"}

        with patch("connect.utils.health.AdminClient") as MockAdmin:
            mock_admin = MagicMock()
            mock_broker = MagicMock()
            mock_broker.host = "localhost"
            mock_broker.port = 9092
            mock_metadata = MagicMock()
            mock_metadata.brokers.values.return_value = [mock_broker]
            mock_admin.list_topics.return_value = mock_metadata
            MockAdmin.return_value = mock_admin

            result = check_kafka_health(config)
            self.assertEqual(result["status"], "ok")
            self.assertIn("brokers", result)

    @patch("connect.utils.health.log_error")
    def test_unhealthy_kafka(self, mock_log_error):
        """Returns error when Kafka is unreachable."""
        from connect.utils.health import check_kafka_health

        config = {"bootstrap.servers": "localhost:9092"}

        with patch("connect.utils.health.AdminClient") as MockAdmin:
            MockAdmin.side_effect = Exception("Connection refused")

            result = check_kafka_health(config)
            self.assertEqual(result["status"], "error")
            self.assertIn("detail", result)


class TestCheckSchemaRegistryHealth(unittest.TestCase):
    """Test check_schema_registry_health."""

    @patch("connect.utils.health.log_error")
    def test_healthy_registry(self, mock_log_error):
        """Returns ok when Schema Registry is reachable."""
        from connect.utils.health import check_schema_registry_health

        config = {"url": "http://localhost:8081"}

        with patch("connect.utils.health.SchemaRegistryClient") as MockSR:
            mock_sr = MagicMock()
            mock_sr.get_subjects.return_value = ["subject1"]
            MockSR.return_value = mock_sr

            result = check_schema_registry_health(config)
            self.assertEqual(result["status"], "ok")

    @patch("connect.utils.health.log_error")
    def test_unhealthy_registry(self, mock_log_error):
        """Returns error when Schema Registry is unreachable."""
        from connect.utils.health import check_schema_registry_health

        config = {"url": "http://localhost:8081"}

        with patch("connect.utils.health.SchemaRegistryClient") as MockSR:
            MockSR.side_effect = Exception("Connection refused")

            result = check_schema_registry_health(config)
            self.assertEqual(result["status"], "error")


class TestCheckFullHealth(unittest.TestCase):
    """Test the aggregate health check."""

    @patch("connect.utils.health.check_schema_registry_health")
    @patch("connect.utils.health.check_kafka_health")
    def test_all_healthy(self, mock_kafka, mock_sr):
        """Full health returns ok when all components are healthy."""
        from connect.utils.health import check_full_health

        mock_kafka.return_value = {"status": "ok", "broker": "localhost:9092"}
        mock_sr.return_value = {"status": "ok", "url": "http://localhost:8081"}

        result = check_full_health()
        self.assertEqual(result["status"], "ok")

    @patch("connect.utils.health.check_schema_registry_health")
    @patch("connect.utils.health.check_kafka_health")
    def test_partial_failure(self, mock_kafka, mock_sr):
        """Full health returns error if any component fails."""
        from connect.utils.health import check_full_health

        mock_kafka.return_value = {"status": "error", "error": "timeout"}
        mock_sr.return_value = {"status": "ok", "url": "http://localhost:8081"}

        result = check_full_health()
        self.assertEqual(result["status"], "error")


if __name__ == "__main__":
    unittest.main()

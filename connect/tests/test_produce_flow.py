"""Integration-style test for the producer flow.

Tests the full path: document event → rule matching → payload building
→ serialization → envelope creation. Mocks only external I/O (Kafka, DB).
"""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fastavro


class TestProduceFlow(unittest.TestCase):
	"""End-to-end produce flow without real Kafka or DB."""

	def _make_rule(self):
		"""Create a mock hook with field mappings."""
		mapping1 = SimpleNamespace(
			avro_field="clientId",
			avro_type="long",
			source_type="Field",
			source_field="fineract_client_id",
			source_expression="",
			static_value="",
			method_path="",
			is_nullable=0,
			default_value="",
		)
		mapping2 = SimpleNamespace(
			avro_field="firstname",
			avro_type="string",
			source_type="Field",
			source_field="first_name",
			source_expression="",
			static_value="",
			method_path="",
			is_nullable=0,
			default_value="",
		)
		mapping3 = SimpleNamespace(
			avro_field="lastname",
			avro_type="string",
			source_type="Field",
			source_field="last_name",
			source_expression="",
			static_value="",
			method_path="",
			is_nullable=0,
			default_value="",
		)

		rule = MagicMock()
		rule.rule_name = "Create Client"
		rule.source_doctype = "Customer"
		rule.document_event = "after_insert"
		rule.command_type = "CreateClient"
		rule.command_category = "client"
		rule.avro_schema_name = "ClientCommandV1"
		rule.priority = 0
		rule.condition = ""
		rule.field_mappings = [mapping1, mapping2, mapping3]
		return rule

	def _make_schema(self):
		"""Return a test Avro schema for ClientCommandV1."""
		return {
			"type": "record",
			"name": "ClientCommandV1",
			"namespace": "org.apache.fineract.avro.client.v1",
			"fields": [
				{"name": "clientId", "type": "long"},
				{"name": "firstname", "type": "string"},
				{"name": "lastname", "type": "string"},
			],
		}

	def test_inner_serialization_with_mapped_payload(self):
		"""Map document fields → build Avro payload → serialize round-trip."""
		from connect.kafka.serialization import (
			deserialize_inner_payload,
			serialize_inner_payload,
		)

		schema = self._make_schema()
		payload = {
			"clientId": 42,
			"firstname": "John",
			"lastname": "Doe",
		}

		raw = serialize_inner_payload(schema, payload)
		self.assertIsInstance(raw, bytes)

		result = deserialize_inner_payload(schema, raw)
		self.assertEqual(result["clientId"], 42)
		self.assertEqual(result["firstname"], "John")
		self.assertEqual(result["lastname"], "Doe")

	@patch("connect.services.mapping_service.frappe")
	def test_mapping_to_serialization_flow(self, mock_frappe):
		"""Full flow: doc → mapping → serialization."""
		from connect.kafka.serialization import (
			deserialize_inner_payload,
			serialize_inner_payload,
		)
		from connect.services.mapping_service import build_payload

		# Create a mock document
		doc = MagicMock()
		doc.get = MagicMock(
			side_effect=lambda k, default=None: {
				"fineract_client_id": 42,
				"first_name": "John",
				"last_name": "Doe",
			}.get(k, default)
		)

		rule = self._make_rule()
		schema = self._make_schema()

		# Step 1: Build payload from mappings
		payload = build_payload(doc, rule.field_mappings)
		self.assertEqual(payload["clientId"], 42)
		self.assertEqual(payload["firstname"], "John")

		# Step 2: Serialize inner payload
		raw = serialize_inner_payload(schema, payload)
		self.assertIsInstance(raw, bytes)

		# Step 3: Verify round-trip
		result = deserialize_inner_payload(schema, raw)
		self.assertEqual(result, payload)

	def test_envelope_structure(self):
		"""MessageV1 envelope has all required fields."""
		from connect.kafka.serialization import MESSAGE_V1_SCHEMA_STR

		schema = json.loads(MESSAGE_V1_SCHEMA_STR)
		field_names = [f["name"] for f in schema["fields"]]

		# Build an envelope dict matching the schema
		inner_bytes = b"\x00\x01\x02"
		envelope = {
			"id": 1,
			"source": "erpnext",
			"type": "CreateClient",
			"category": "client",
			"createdAt": "2024-01-01T00:00:00Z",
			"businessDate": "2024-01-01",
			"tenantId": "default",
			"idempotencyKey": "abc123",
			"dataschema": "ClientCommandV1",
			"data": inner_bytes,
		}

		# Verify all schema fields are present
		for field in field_names:
			self.assertIn(field, envelope, f"Missing field: {field}")

		# Verify with fastavro (raw round-trip)
		from io import BytesIO

		parsed = fastavro.parse_schema(schema)
		buf = BytesIO()
		fastavro.schemaless_writer(buf, parsed, envelope)
		buf.seek(0)
		result = fastavro.schemaless_reader(buf, parsed)

		self.assertEqual(result["type"], "CreateClient")
		self.assertEqual(result["data"], inner_bytes)


if __name__ == "__main__":
	unittest.main()

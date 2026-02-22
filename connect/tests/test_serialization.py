"""Tests for the two-tier Avro serialization module."""

import json
import unittest
from io import BytesIO

import fastavro

from connect.kafka.serialization import (
	MESSAGE_V1_SCHEMA_STR,
	deserialize_inner_payload,
	serialize_inner_payload,
)


class TestInnerPayloadSerialization(unittest.TestCase):
	"""Test raw Avro binary serialization (inner payload, no Confluent header)."""

	def setUp(self):
		self.client_schema = {
			"type": "record",
			"name": "ClientCommandV1",
			"namespace": "org.apache.fineract.avro.client.v1",
			"fields": [
				{"name": "clientId", "type": "long"},
				{"name": "firstname", "type": "string"},
				{"name": "lastname", "type": "string"},
				{"name": "externalId", "type": ["null", "string"], "default": None},
			],
		}
		self.sample_payload = {
			"clientId": 42,
			"firstname": "John",
			"lastname": "Doe",
			"externalId": {"string": "EXT-001"},
		}

	def test_round_trip(self):
		"""Serializing then deserializing returns the original payload."""
		raw = serialize_inner_payload(self.client_schema, self.sample_payload)
		self.assertIsInstance(raw, bytes)
		self.assertGreater(len(raw), 0)

		deserialized = deserialize_inner_payload(self.client_schema, raw)
		self.assertEqual(deserialized["clientId"], 42)
		self.assertEqual(deserialized["firstname"], "John")
		self.assertEqual(deserialized["lastname"], "Doe")

	def test_nullable_field(self):
		"""Nullable union fields serialize correctly when None."""
		payload = {
			"clientId": 1,
			"firstname": "Jane",
			"lastname": "Doe",
			"externalId": None,
		}
		raw = serialize_inner_payload(self.client_schema, payload)
		result = deserialize_inner_payload(self.client_schema, raw)
		self.assertIsNone(result["externalId"])

	def test_empty_string_field(self):
		"""Empty strings are preserved through serialization."""
		payload = {
			"clientId": 2,
			"firstname": "",
			"lastname": "",
			"externalId": None,
		}
		raw = serialize_inner_payload(self.client_schema, payload)
		result = deserialize_inner_payload(self.client_schema, raw)
		self.assertEqual(result["firstname"], "")
		self.assertEqual(result["lastname"], "")

	def test_large_long_value(self):
		"""Large long values survive round-trip."""
		payload = {
			"clientId": 2**40,
			"firstname": "Big",
			"lastname": "Number",
			"externalId": None,
		}
		raw = serialize_inner_payload(self.client_schema, payload)
		result = deserialize_inner_payload(self.client_schema, raw)
		self.assertEqual(result["clientId"], 2**40)

	def test_invalid_schema_raises(self):
		"""Passing a payload that doesn't match schema raises an error."""
		bad_payload = {"nonexistent_field": "value"}
		with self.assertRaises(Exception):
			serialize_inner_payload(self.client_schema, bad_payload)


class TestMessageV1Schema(unittest.TestCase):
	"""Verify the MessageV1 envelope schema is valid Avro."""

	def test_schema_is_valid_json(self):
		schema = json.loads(MESSAGE_V1_SCHEMA_STR)
		self.assertEqual(schema["name"], "MessageV1")
		self.assertEqual(schema["namespace"], "org.apache.fineract.avro")

	def test_schema_parses(self):
		schema = json.loads(MESSAGE_V1_SCHEMA_STR)
		parsed = fastavro.parse_schema(schema)
		self.assertIsNotNone(parsed)

	def test_schema_has_expected_fields(self):
		schema = json.loads(MESSAGE_V1_SCHEMA_STR)
		field_names = {f["name"] for f in schema["fields"]}
		expected = {
			"id",
			"source",
			"type",
			"category",
			"createdAt",
			"businessDate",
			"tenantId",
			"idempotencyKey",
			"dataschema",
			"data",
		}
		self.assertEqual(field_names, expected)

	def test_data_field_is_bytes(self):
		schema = json.loads(MESSAGE_V1_SCHEMA_STR)
		data_field = next(f for f in schema["fields"] if f["name"] == "data")
		self.assertEqual(data_field["type"], "bytes")


if __name__ == "__main__":
	unittest.main()

"""Two-tier Avro serialization/deserialization.

Replicates the exact format from producer_test.py:
- Inner payload: raw Avro binary via fastavro.schemaless_writer (no Confluent header)
- Outer envelope: Confluent wire format via AvroSerializer (MessageV1)
"""

import json
from io import BytesIO

import fastavro

MESSAGE_V1_SCHEMA_STR = json.dumps(
	{
		"type": "record",
		"name": "MessageV1",
		"namespace": "org.apache.fineract.avro",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "source", "type": "string"},
			{"name": "type", "type": "string"},
			{"name": "category", "type": "string"},
			{"name": "createdAt", "type": "string"},
			{"name": "businessDate", "type": "string"},
			{"name": "tenantId", "type": "string"},
			{"name": "idempotencyKey", "type": "string"},
			{"name": "dataschema", "type": "string"},
			{"name": "data", "type": "bytes"},
		],
	}
)


def serialize_inner_payload(schema_dict: dict, payload: dict) -> bytes:
	"""Serialize an inner Avro payload using fastavro (raw binary, no Confluent header).

	Args:
		schema_dict: Parsed Avro schema as a Python dict.
		payload: Payload dict matching the schema.

	Returns:
		Raw Avro binary bytes.
	"""
	parsed_schema = fastavro.parse_schema(schema_dict)
	buf = BytesIO()
	fastavro.schemaless_writer(buf, parsed_schema, payload)
	return buf.getvalue()


def deserialize_inner_payload(schema_dict: dict, data: bytes) -> dict:
	"""Deserialize raw Avro binary back to a Python dict.

	Args:
		schema_dict: Parsed Avro schema as a Python dict.
		data: Raw Avro binary bytes.

	Returns:
		Deserialized payload dict.
	"""
	parsed_schema = fastavro.parse_schema(schema_dict)
	buf = BytesIO(data)
	return fastavro.schemaless_reader(buf, parsed_schema)


def create_avro_serializer(sr_client, auto_register: bool = True):
	"""Create a Confluent AvroSerializer for the MessageV1 envelope.

	Args:
		sr_client: Confluent SchemaRegistryClient instance.
		auto_register: Whether to auto-register schemas.

	Returns:
		AvroSerializer configured for MessageV1.
	"""
	from confluent_kafka.schema_registry import record_subject_name_strategy
	from confluent_kafka.schema_registry.avro import AvroSerializer

	return AvroSerializer(
		sr_client,
		MESSAGE_V1_SCHEMA_STR,
		conf={
			"auto.register.schemas": auto_register,
			"subject.name.strategy": record_subject_name_strategy,
		},
	)


def serialize_envelope(avro_serializer, envelope: dict, topic: str) -> bytes:
	"""Serialize a MessageV1 envelope using the Confluent wire format.

	Args:
		avro_serializer: AvroSerializer for MessageV1.
		envelope: MessageV1 dict with all required fields.
		topic: Kafka topic for serialization context.

	Returns:
		Confluent wire format bytes [0x00][schema-id][avro-binary].
	"""
	from confluent_kafka.serialization import MessageField, SerializationContext

	ctx = SerializationContext(topic, MessageField.VALUE)
	return avro_serializer(envelope, ctx)


def create_avro_deserializer(sr_client):
	"""Create a Confluent AvroDeserializer for the MessageV1 envelope.

	Args:
		sr_client: Confluent SchemaRegistryClient instance.

	Returns:
		AvroDeserializer configured for MessageV1.
	"""
	from confluent_kafka.schema_registry.avro import AvroDeserializer

	return AvroDeserializer(sr_client, MESSAGE_V1_SCHEMA_STR)


def deserialize_envelope(avro_deserializer, data: bytes, topic: str) -> dict:
	"""Deserialize a Confluent wire format message back to a MessageV1 dict.

	Args:
		avro_deserializer: AvroDeserializer for MessageV1.
		data: Raw Kafka message value bytes.
		topic: Kafka topic for deserialization context.

	Returns:
		MessageV1 dict.
	"""
	from confluent_kafka.serialization import MessageField, SerializationContext

	ctx = SerializationContext(topic, MessageField.VALUE)
	return avro_deserializer(data, ctx)

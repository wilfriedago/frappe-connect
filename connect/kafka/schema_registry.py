"""Schema Registry client wrapper with three-layer caching."""
import json
from datetime import datetime

import frappe

from connect.utils.logging import log_error, log_info


class SchemaRegistryService:
    """Wraps the Confluent Schema Registry client with caching."""

    def __init__(self, config: dict):
        from confluent_kafka.schema_registry import SchemaRegistryClient

        self._client = SchemaRegistryClient(config)

    @property
    def client(self):
        return self._client

    def get_latest_schema(self, subject: str) -> dict:
        """Fetch the latest schema for a subject from the registry.

        Returns dict with keys: schema_str, schema_id, version, subject.
        """
        try:
            registered = self._client.get_latest_version(subject)
            return {
                "schema_str": registered.schema.schema_str,
                "schema_id": registered.schema_id,
                "version": registered.version,
                "subject": subject,
            }
        except Exception as e:
            log_error("Schema Registry fetch failed", str(e), exc=e)
            raise

    def get_schema_by_id(self, schema_id: int) -> str:
        """Fetch a schema string by its numeric ID."""
        try:
            schema = self._client.get_schema(schema_id)
            return schema.schema_str
        except Exception as e:
            log_error(f"Schema Registry fetch by ID {schema_id} failed", str(e), exc=e)
            raise

    def register_schema(self, subject: str, schema_str: str) -> int:
        """Register a schema under a subject. Returns the schema ID."""
        from confluent_kafka.schema_registry import Schema

        try:
            schema = Schema(schema_str, "AVRO")
            schema_id = self._client.register_schema(subject, schema)
            log_info("Schema registered", f"subject={subject}, schema_id={schema_id}")
            return schema_id
        except Exception as e:
            log_error("Schema registration failed", str(e), exc=e)
            raise

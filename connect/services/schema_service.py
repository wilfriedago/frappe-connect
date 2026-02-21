"""Schema service — three-layer cache resolution for Avro schemas.

Cache layers:
1. Redis (fastest, TTL-based)
2. MariaDB (Fineract Avro Schema DocType, persistent)
3. Schema Registry (authoritative, network call)
"""
import json

import frappe
from frappe.utils import now_datetime

from connect.utils.cache import cache_delete, cache_get, cache_set
from connect.utils.logging import log_error, log_info


SCHEMA_CACHE_PREFIX = "fineract_schema:"


def get_schema(schema_name: str, settings=None) -> dict:
    """Resolve a schema by name through the three-layer cache.

    Returns the parsed schema dict.
    """
    if settings is None:
        settings = frappe.get_single("Fineract Kafka Settings")

    ttl = settings.schema_cache_ttl_seconds or 3600

    # Layer 1: Redis
    cache_key = f"{SCHEMA_CACHE_PREFIX}{schema_name}"
    cached = cache_get(cache_key)
    if cached:
        return json.loads(cached) if isinstance(cached, str) else cached

    # Layer 2: MariaDB
    schema_doc = frappe.db.get_value(
        "Fineract Avro Schema",
        {"schema_name": schema_name, "is_latest": 1},
        ["schema_json", "schema_id"],
        as_dict=True,
    )
    if schema_doc and schema_doc.schema_json:
        schema_dict = json.loads(schema_doc.schema_json)
        cache_set(cache_key, schema_doc.schema_json, expires_in_sec=ttl)
        return schema_dict

    # Layer 3: Schema Registry
    schema_dict = _fetch_from_registry(schema_name, settings)
    if schema_dict:
        # Save to Layer 2 + Layer 1
        _save_schema_to_db(schema_name, schema_dict, settings)
        cache_set(cache_key, json.dumps(schema_dict), expires_in_sec=ttl)
        return schema_dict

    frappe.throw(f"Schema not found: {schema_name}")


def _fetch_from_registry(schema_name: str, settings) -> dict | None:
    """Fetch a schema from Schema Registry by subject name."""
    try:
        from connect.kafka.schema_registry import SchemaRegistryService

        sr_config = settings.get_schema_registry_config()
        sr_service = SchemaRegistryService(sr_config)

        # Try subject = schema_name (common convention)
        result = sr_service.get_latest_schema(schema_name)
        if result:
            return json.loads(result["schema_str"])
    except Exception as e:
        log_error("Schema Registry fetch failed", f"schema={schema_name}, error={e}", exc=e)
    return None


def _save_schema_to_db(schema_name: str, schema_dict: dict, settings):
    """Save a fetched schema to the MariaDB cache (Fineract Avro Schema DocType)."""
    try:
        schema_json = json.dumps(schema_dict, indent=2)

        # Determine schema type from name
        schema_type = "command"
        if "MessageV1" in schema_name:
            schema_type = "envelope"
        elif "BusinessEvent" in schema_name:
            schema_type = "event"

        doc = frappe.get_doc(
            {
                "doctype": "Fineract Avro Schema",
                "schema_name": schema_name,
                "schema_version": 1,
                "schema_type": schema_type,
                "schema_json": schema_json,
                "is_latest": 1,
                "last_fetched": now_datetime(),
            }
        )
        doc.insert(ignore_permissions=True)
        frappe.db.commit()
        log_info("Schema saved to DB", f"schema={schema_name}")
    except Exception as e:
        log_error("Failed to save schema to DB", str(e), exc=e)


def invalidate_schema_cache(schema_name: str | None = None):
    """Invalidate schema cache. If schema_name is None, invalidate all."""
    if schema_name:
        cache_delete(f"{SCHEMA_CACHE_PREFIX}{schema_name}")
    else:
        # Clear all schema cache entries
        schemas = frappe.get_all("Fineract Avro Schema", pluck="schema_name")
        for name in schemas:
            cache_delete(f"{SCHEMA_CACHE_PREFIX}{name}")


def refresh_schema_cache():
    """Scheduled job: refresh all cached schemas from Schema Registry.

    Runs every 6 hours to pre-warm the cache.
    """
    settings = frappe.get_single("Fineract Kafka Settings")
    if not settings.enabled:
        return

    schemas = frappe.get_all(
        "Fineract Avro Schema",
        filters={"is_latest": 1},
        fields=["schema_name"],
    )

    refreshed = 0
    for schema in schemas:
        try:
            schema_dict = _fetch_from_registry(schema.schema_name, settings)
            if schema_dict:
                ttl = settings.schema_cache_ttl_seconds or 3600
                cache_key = f"{SCHEMA_CACHE_PREFIX}{schema.schema_name}"
                cache_set(cache_key, json.dumps(schema_dict), expires_in_sec=ttl)
                refreshed += 1
        except Exception as e:
            log_error("Schema refresh failed", f"schema={schema.schema_name}, error={e}", exc=e)

    log_info("Schema cache refresh complete", f"refreshed={refreshed}/{len(schemas)}")

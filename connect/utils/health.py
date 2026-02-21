"""Health check utilities for Kafka and Schema Registry."""
import frappe

from connect.utils.logging import log_error, log_info


def check_kafka_health(config: dict) -> dict:
    """Check if Kafka brokers are reachable.

    Returns dict with 'status' ('ok'/'error') and 'detail'.
    """
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient(config)
        metadata = admin.list_topics(timeout=10)
        brokers = list(metadata.brokers.values())
        return {
            "status": "ok",
            "detail": f"{len(brokers)} broker(s) available",
            "brokers": [f"{b.host}:{b.port}" for b in brokers],
        }
    except Exception as e:
        log_error("Kafka health check failed", str(e), exc=e)
        return {"status": "error", "detail": str(e)}


def check_schema_registry_health(config: dict) -> dict:
    """Check if Schema Registry is reachable.

    Returns dict with 'status' ('ok'/'error') and 'detail'.
    """
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient

        client = SchemaRegistryClient(config)
        # Listing subjects is a lightweight health check
        subjects = client.get_subjects()
        return {
            "status": "ok",
            "detail": f"{len(subjects)} subject(s) registered",
            "subjects": subjects,
        }
    except Exception as e:
        log_error("Schema Registry health check failed", str(e), exc=e)
        return {"status": "error", "detail": str(e)}


def check_full_health() -> dict:
    """Run all health checks using current settings.

    Returns dict with overall status and individual component results.
    """
    try:
        settings = frappe.get_single("Fineract Kafka Settings")
    except Exception as e:
        return {
            "status": "error",
            "detail": f"Cannot load Fineract Kafka Settings: {e}",
            "kafka": {"status": "unknown"},
            "schema_registry": {"status": "unknown"},
        }

    kafka_config = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
    }
    # Apply security config
    if hasattr(settings, "_apply_security_config"):
        settings._apply_security_config(kafka_config)

    sr_config = settings.get_schema_registry_config()

    kafka_result = check_kafka_health(kafka_config)
    sr_result = check_schema_registry_health(sr_config)

    overall = "ok" if kafka_result["status"] == "ok" and sr_result["status"] == "ok" else "error"

    return {
        "status": overall,
        "enabled": bool(settings.enabled),
        "environment": settings.environment,
        "kafka": kafka_result,
        "schema_registry": sr_result,
    }

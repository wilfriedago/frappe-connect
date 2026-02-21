"""API endpoints for the connect app.

Provides whitelisted methods for health checks, manual triggers,
and schema management.
"""
import frappe

from connect.utils.errors import ERROR_CODES, error, ok
from connect.utils.logging import log_error


@frappe.whitelist()
def health():
    """Health check endpoint for Kafka and Schema Registry.

    Returns overall status and individual component health.
    """
    try:
        from connect.utils.health import check_full_health

        result = check_full_health()
        if result["status"] == "ok":
            return ok("All systems operational", result)
        return error("Health check failed", result, code=ERROR_CODES["INTEGRATION_ERROR"])
    except Exception as exc:
        log_error("Health check API error", str(exc), exc=exc)
        return error("Health check failed", {"detail": str(exc)}, code=ERROR_CODES["APP_ERROR"])


@frappe.whitelist()
def manual_produce(doctype: str, docname: str, rule_name: str):
    """Manually trigger a Kafka message production for a specific document and rule.

    Useful for testing or re-sending failed messages.
    """
    try:
        from connect.services.producer_service import produce_message
        from connect.utils.idempotency import generate_uuid

        doc = frappe.get_doc(doctype, docname)

        # Generate a new idempotency key for manual triggers
        idempotency_key = generate_uuid()

        produce_message(
            doc=doc,
            rule_name=rule_name,
            idempotency_key=idempotency_key,
        )
        return ok("Message produced", {"idempotency_key": idempotency_key})

    except frappe.DoesNotExistError:
        return error(f"Document not found: {doctype}/{docname}", code=ERROR_CODES["NOT_FOUND"])
    except Exception as exc:
        log_error("Manual produce failed", str(exc), exc=exc)
        return error("Production failed", {"detail": str(exc)}, code=ERROR_CODES["KAFKA_ERROR"])


@frappe.whitelist()
def refresh_schemas():
    """Manually refresh the Avro schema cache."""
    try:
        from connect.services.schema_service import refresh_schema_cache

        refresh_schema_cache()
        return ok("Schema cache refreshed")
    except Exception as exc:
        log_error("Schema refresh failed", str(exc), exc=exc)
        return error("Schema refresh failed", {"detail": str(exc)}, code=ERROR_CODES["SCHEMA_ERROR"])


@frappe.whitelist()
def get_active_rules():
    """Get all active emission rules grouped by DocType."""
    try:
        rules = frappe.get_all(
            "Fineract Event Emission Rule",
            filters={"enabled": 1},
            fields=["rule_name", "source_doctype", "document_event", "command_type", "priority"],
            order_by="source_doctype, priority",
        )
        # Group by doctype
        grouped = {}
        for rule in rules:
            dt = rule.source_doctype
            if dt not in grouped:
                grouped[dt] = []
            grouped[dt].append(rule)

        return ok("Active rules", {"rules": grouped, "total": len(rules)})
    except Exception as exc:
        log_error("Get active rules failed", str(exc), exc=exc)
        return error("Failed to fetch rules", {"detail": str(exc)}, code=ERROR_CODES["APP_ERROR"])


@frappe.whitelist()
def get_kafka_stats():
    """Get Kafka log statistics."""
    try:
        stats = frappe.db.sql(
            """
            SELECT
                direction,
                status,
                COUNT(*) as count
            FROM `tabFineract Kafka Log`
            WHERE creation >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            GROUP BY direction, status
            ORDER BY direction, status
            """,
            as_dict=True,
        )
        return ok("Kafka stats (last 24h)", {"stats": stats})
    except Exception as exc:
        log_error("Get Kafka stats failed", str(exc), exc=exc)
        return error("Failed to fetch stats", {"detail": str(exc)}, code=ERROR_CODES["APP_ERROR"])

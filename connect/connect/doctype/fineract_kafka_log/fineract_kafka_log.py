import frappe
from frappe.model.document import Document
from frappe.utils import now_datetime


class FineractKafkaLog(Document):
    """Audit log for every produced/consumed Kafka message.

    Serves as both observability artifact and idempotency store.
    """

    def before_insert(self):
        if not self.status:
            self.status = "Pending"

    @staticmethod
    def log_produced(
        idempotency_key: str,
        command_type: str,
        topic: str,
        tenant_id: str,
        source_doctype: str = None,
        source_docname: str = None,
        rule_name: str = None,
        payload_json: str = None,
    ) -> "FineractKafkaLog":
        """Create a log entry for a produced message."""
        log = frappe.get_doc(
            {
                "doctype": "Fineract Kafka Log",
                "direction": "Produced",
                "status": "Pending",
                "idempotency_key": idempotency_key,
                "command_type": command_type,
                "topic": topic,
                "tenant_id": tenant_id,
                "source_doctype": source_doctype,
                "source_docname": source_docname,
                "rule_name": rule_name,
                "payload_json": payload_json,
            }
        )
        log.insert(ignore_permissions=True)
        return log

    @staticmethod
    def log_consumed(
        idempotency_key: str,
        event_type: str,
        topic: str,
        partition: int,
        offset: int,
        tenant_id: str = None,
        handler_name: str = None,
        payload_json: str = None,
    ) -> "FineractKafkaLog":
        """Create a log entry for a consumed message."""
        log = frappe.get_doc(
            {
                "doctype": "Fineract Kafka Log",
                "direction": "Consumed",
                "status": "Pending",
                "idempotency_key": idempotency_key,
                "event_type": event_type,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "tenant_id": tenant_id,
                "handler_name": handler_name,
                "payload_json": payload_json,
            }
        )
        log.insert(ignore_permissions=True)
        return log

    def mark_delivered(self, partition: int = None, offset: int = None):
        """Update log to Delivered status after successful Kafka produce."""
        updates = {"status": "Delivered", "processed_at": now_datetime()}
        if partition is not None:
            updates["partition"] = partition
        if offset is not None:
            updates["offset"] = offset
        for field, value in updates.items():
            self.db_set(field, value, update_modified=False)

    def mark_processed(self):
        """Update log to Processed status after successful dispatch."""
        self.db_set("status", "Processed", update_modified=False)
        self.db_set("processed_at", now_datetime(), update_modified=False)

    def mark_failed(self, error_message: str, error_traceback: str = None):
        """Update log to Failed status with error details."""
        self.db_set("status", "Failed", update_modified=False)
        self.db_set("error_message", error_message, update_modified=False)
        if error_traceback:
            self.db_set("error_traceback", error_traceback, update_modified=False)
        self.db_set("retry_count", (self.retry_count or 0) + 1, update_modified=False)

    def mark_dead_letter(self, error_message: str):
        """Update log to Dead Letter status."""
        self.db_set("status", "Dead Letter", update_modified=False)
        self.db_set("error_message", error_message, update_modified=False)

    def mark_skipped(self, reason: str = None):
        """Update log to Skipped status (e.g., duplicate idempotency key)."""
        self.db_set("status", "Skipped", update_modified=False)
        if reason:
            self.db_set("error_message", reason, update_modified=False)

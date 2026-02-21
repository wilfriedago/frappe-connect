"""Scheduled cleanup job: purge old Kafka logs and stale pending entries."""

import frappe
from frappe.utils import add_days, now_datetime

from connect.utils.logging import log_error, log_info


def cleanup_kafka_logs():
	"""Scheduled job: delete Kafka log entries older than retention period.

	Also re-enqueues or marks as Failed any stale Pending entries.
	"""
	try:
		settings = frappe.get_single("Fineract Kafka Settings")
		if not settings.enabled:
			return

		retention_days = settings.log_retention_days or 30
		cutoff = add_days(now_datetime(), -retention_days)

		# Delete old logs beyond retention
		deleted = frappe.db.delete(
			"Fineract Kafka Log",
			{
				"creation": ("<", cutoff),
				"status": ("in", ["Delivered", "Processed", "Skipped", "Dead Letter"]),
			},
		)
		if deleted:
			log_info("Kafka log cleanup", f"Deleted {deleted} old log entries")

		# Handle stale Pending entries (older than 10 minutes)
		stale_cutoff = add_days(now_datetime(), 0)  # Will use minutes below
		stale_logs = frappe.db.sql(
			"""
            SELECT name, direction, command_type, source_doctype, source_docname, rule_name, idempotency_key, retry_count
            FROM `tabFineract Kafka Log`
            WHERE status = 'Pending'
            AND TIMESTAMPDIFF(MINUTE, creation, NOW()) > 10
            AND retry_count < %s
            """,
			(settings.max_produce_retries or 5,),
			as_dict=True,
		)

		for log_entry in stale_logs:
			if log_entry.direction == "Produced" and log_entry.rule_name:
				# Re-enqueue the produce job
				try:
					frappe.enqueue(
						"connect.background_jobs.produce_message.produce_fineract_command",
						queue="default",
						timeout=120,
						doctype=log_entry.source_doctype,
						docname=log_entry.source_docname,
						rule_name=log_entry.rule_name,
						idempotency_key=log_entry.idempotency_key,
					)
					frappe.db.set_value(
						"Fineract Kafka Log",
						log_entry.name,
						"retry_count",
						(log_entry.retry_count or 0) + 1,
						update_modified=False,
					)
					log_info("Stale log re-enqueued", f"name={log_entry.name}")
				except Exception as e:
					log_error("Failed to re-enqueue stale log", str(e), exc=e)
			else:
				# Mark as Failed if can't retry
				frappe.db.set_value(
					"Fineract Kafka Log",
					log_entry.name,
					"status",
					"Failed",
					update_modified=False,
				)
				frappe.db.set_value(
					"Fineract Kafka Log",
					log_entry.name,
					"error_message",
					"Stale Pending entry marked as Failed",
					update_modified=False,
				)

		# Mark as Failed: entries that exceeded max retries
		frappe.db.sql(
			"""
            UPDATE `tabFineract Kafka Log`
            SET status = 'Failed',
                error_message = 'Exceeded max retries'
            WHERE status = 'Pending'
            AND TIMESTAMPDIFF(MINUTE, creation, NOW()) > 10
            AND retry_count >= %s
            """,
			(settings.max_produce_retries or 5,),
		)

		frappe.db.commit()

	except Exception as e:
		log_error("Kafka log cleanup failed", str(e), exc=e)

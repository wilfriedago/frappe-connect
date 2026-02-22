"""Correlation service — links produced and consumed messages."""

import frappe

from connect.utils.logging import log_error, log_info


def correlate_messages(produced_log_name: str, consumed_log_name: str):
	"""Link a produced Kafka log entry to a consumed one (bidirectional)."""
	try:
		frappe.db.set_value(
			"Connect Message Log",
			produced_log_name,
			"correlated_log",
			consumed_log_name,
			update_modified=False,
		)
		frappe.db.set_value(
			"Connect Message Log",
			consumed_log_name,
			"correlated_log",
			produced_log_name,
			update_modified=False,
		)
		log_info("Messages correlated", f"produced={produced_log_name} ↔ consumed={consumed_log_name}")
	except Exception as e:
		log_error("Correlation failed", str(e), exc=e)


def find_produced_log(idempotency_key: str) -> str | None:
	"""Find a produced log entry by idempotency key."""
	result = frappe.db.get_value(
		"Connect Message Log",
		{"idempotency_key": idempotency_key, "direction": "Produced"},
		"name",
	)
	return result


def find_consumed_log(idempotency_key: str) -> str | None:
	"""Find a consumed log entry by idempotency key."""
	result = frappe.db.get_value(
		"Connect Message Log",
		{"idempotency_key": idempotency_key, "direction": "Consumed"},
		"name",
	)
	return result


def auto_correlate_by_external_id(external_id: str):
	"""Try to correlate produced and consumed messages sharing an external ID.

	Searches payloads for matching externalId values.
	"""
	try:
		produced = frappe.db.sql(
			"""
            SELECT name FROM `tabConnect Message Log`
            WHERE direction = 'Produced'
            AND payload_json LIKE %s
            ORDER BY creation DESC LIMIT 1
            """,
			(f'%"externalId": "{external_id}"%',),
			as_dict=True,
		)
		consumed = frappe.db.sql(
			"""
            SELECT name FROM `tabConnect Message Log`
            WHERE direction = 'Consumed'
            AND payload_json LIKE %s
            ORDER BY creation DESC LIMIT 1
            """,
			(f'%"externalId": "{external_id}"%',),
			as_dict=True,
		)

		if produced and consumed:
			correlate_messages(produced[0].name, consumed[0].name)
			return True
	except Exception as e:
		log_error("Auto-correlation failed", str(e), exc=e)

	return False

"""Background job: produce a single Fineract command to Kafka."""

import frappe

from connect.services.producer_service import produce_message
from connect.utils.logging import log_error


def produce_fineract_command(
	doctype: str,
	docname: str,
	rule_name: str,
	idempotency_key: str,
):
	"""RQ job entry point: load document and produce a Kafka message.

	Called via frappe.enqueue() with enqueue_after_commit=True from
	the producer_service.on_document_event() handler.
	"""
	try:
		doc = frappe.get_doc(doctype, docname)
	except frappe.DoesNotExistError:
		log_error(
			"Produce job: document not found",
			f"doctype={doctype}, name={docname} (may have been deleted)",
		)
		return
	except Exception as e:
		log_error("Produce job: failed to load document", str(e), exc=e)
		raise

	try:
		produce_message(
			doc=doc,
			rule_name=rule_name,
			idempotency_key=idempotency_key,
		)
	except Exception as e:
		log_error(
			"Produce job failed",
			f"doctype={doctype}, name={docname}, rule={rule_name}, error={e}",
			exc=e,
		)
		raise

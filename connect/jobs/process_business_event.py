"""Background job: process a consumed Fineract business event."""

import json

import frappe

from connect.utils.logging import log_error, log_info


def process_document_action(
	action_type: str,
	target_doctype: str,
	field_mapping_json: str | None,
	correlation_field: str | None,
	payload: dict,
):
	"""Process a Create/Update Document action from a consumed event.

	Args:
		action_type: 'Create Document' or 'Update Document'.
		target_doctype: The DocType to create/update.
		field_mapping_json: JSON string mapping payload fields → DocType fields.
		correlation_field: Payload field used for matching (e.g., 'externalId').
		payload: The deserialized inner Avro payload dict.
	"""
	try:
		# Parse field mappings
		field_map = {}
		if field_mapping_json:
			field_map = json.loads(field_mapping_json)

		# Build values dict from payload using field mappings
		values = {}
		for target_field, source_path in field_map.items():
			values[target_field] = _resolve_path(payload, source_path)

		if action_type == "Create Document":
			_create_document(target_doctype, values)
		elif action_type == "Update Document":
			_update_document(target_doctype, values, correlation_field, payload)

	except Exception as e:
		log_error(
			"Document action failed",
			f"type={action_type}, doctype={target_doctype}, error={e}",
			exc=e,
		)
		raise


def _resolve_path(data: dict, path: str):
	"""Resolve a dotted path in a dict. E.g., 'client.name' → data['client']['name']."""
	parts = path.split(".")
	current = data
	for part in parts:
		if isinstance(current, dict):
			current = current.get(part)
		else:
			return None
	return current


def _create_document(doctype: str, values: dict):
	"""Create a new Frappe document."""
	doc = frappe.get_doc({"doctype": doctype, **values})
	doc.insert(ignore_permissions=True)
	frappe.db.commit()
	log_info("Document created", f"doctype={doctype}, name={doc.name}")


def _update_document(
	doctype: str,
	values: dict,
	correlation_field: str | None,
	payload: dict,
):
	"""Find and update an existing document using correlation field."""
	if not correlation_field:
		log_error("Update document: no correlation field", f"doctype={doctype}")
		return

	# The correlation field value from the payload
	correlation_value = payload.get(correlation_field)
	if not correlation_value:
		log_error(
			"Update document: correlation value not found",
			f"doctype={doctype}, field={correlation_field}",
		)
		return

	# Find the document
	existing = frappe.db.get_value(
		doctype,
		{correlation_field: correlation_value},
		"name",
	)
	if not existing:
		log_error(
			"Update document: target not found",
			f"doctype={doctype}, {correlation_field}={correlation_value}",
		)
		return

	doc = frappe.get_doc(doctype, existing)
	for field, value in values.items():
		doc.set(field, value)
	doc.save(ignore_permissions=True)
	frappe.db.commit()
	log_info("Document updated", f"doctype={doctype}, name={doc.name}")

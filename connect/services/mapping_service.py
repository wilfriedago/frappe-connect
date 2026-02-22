"""Field mapping resolution engine.

Resolves field mappings from Connect Hook child table rows
into a payload dict suitable for Avro serialization.
"""

import frappe

from connect.utils.logging import log_error
from connect.utils.validation import validate_avro_field_value


def build_payload(doc, field_mappings: list) -> dict:
	"""Build an Avro payload dict from document + field mappings.

	Args:
		doc: Frappe document object.
		field_mappings: List of Connect Hook Field Mapping child table rows.

	Returns:
		Dict with resolved values keyed by avro_field.
	"""
	payload = {}

	for mapping in field_mappings:
		try:
			raw_value = _resolve_source(doc, mapping)
			coerced = _coerce_value(raw_value, mapping)
			payload[mapping.avro_field] = coerced
		except Exception as e:
			log_error(
				"Field mapping resolution failed",
				f"field={mapping.avro_field}, source_type={mapping.source_type}, error={e}",
				exc=e,
			)
			# If nullable, set to None; otherwise propagate the error
			if mapping.is_nullable:
				payload[mapping.avro_field] = None
			else:
				raise

	return payload


def _resolve_source(doc, mapping) -> object:
	"""Resolve the source value based on source_type.

	Supports four source types:
	- Field: Direct doc.get(source_field)
	- Expression: frappe.safe_eval(expr, {"doc": doc})
	- Static: Fixed string value
	- Method: frappe.get_attr(dotted_path)(doc)
	"""
	source_type = mapping.source_type

	if source_type == "Field":
		return doc.get(mapping.source_field)

	elif source_type == "Expression":
		return frappe.safe_eval(
			mapping.source_expression,
			eval_globals={"doc": doc, "frappe": frappe},
		)

	elif source_type == "Static":
		return mapping.static_value

	elif source_type == "Method":
		method = frappe.get_attr(mapping.method_path)
		return method(doc)

	else:
		raise ValueError(f"Unknown source_type: {source_type}")


def _coerce_value(value, mapping):
	"""Coerce a resolved value to the target Avro type.

	Handles nullable unions and default values.
	"""
	# Apply default if value is None
	if value is None and mapping.default_value:
		value = mapping.default_value

	# Validate and coerce to Avro type
	return validate_avro_field_value(
		value,
		avro_type=mapping.avro_type,
		is_nullable=bool(mapping.is_nullable),
	)

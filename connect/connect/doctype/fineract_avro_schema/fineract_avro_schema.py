import json

import frappe
from frappe.model.document import Document


class FineractAvroSchema(Document):
    """Local cache of Avro schemas from the Schema Registry (MariaDB layer)."""

    def validate(self):
        self._validate_schema_json()

    def _validate_schema_json(self):
        if self.schema_json:
            try:
                parsed = json.loads(self.schema_json)
                if not isinstance(parsed, dict) or "type" not in parsed:
                    frappe.throw("Schema JSON must be a valid Avro schema with a 'type' field")
            except json.JSONDecodeError as e:
                frappe.throw(f"Invalid JSON in schema definition: {e}")

    def before_save(self):
        if self.is_latest:
            # Unmark other versions of the same schema as non-latest
            frappe.db.sql(
                """
                UPDATE `tabFineract Avro Schema`
                SET is_latest = 0
                WHERE schema_name = %s AND name != %s
                """,
                (self.schema_name, self.name),
            )

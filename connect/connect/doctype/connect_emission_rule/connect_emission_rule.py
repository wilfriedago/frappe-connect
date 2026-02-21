import frappe
from frappe.model.document import Document


class ConnectEmissionRule(Document):
    """Defines when a Frappe DocType event should produce a command message."""

    def validate(self):
        self._validate_condition()
        self._validate_field_mappings()

    def _validate_condition(self):
        if self.condition:
            try:
                compile(self.condition, "<condition>", "eval")
            except SyntaxError as e:
                frappe.throw(f"Invalid condition expression: {e}")

    def _validate_field_mappings(self):
        if not self.field_mappings:
            frappe.throw("At least one field mapping is required")

    def on_update(self):
        self._invalidate_active_doctypes_cache()

    def on_trash(self):
        self._invalidate_active_doctypes_cache()

    def _invalidate_active_doctypes_cache(self):
        frappe.cache().delete_value("connect_active_doctypes")

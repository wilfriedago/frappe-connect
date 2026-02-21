import frappe
from frappe.model.document import Document


class ConnectEventHandler(Document):
    """Maps incoming Business Events to Frappe actions."""

    def validate(self):
        self._validate_condition()
        self._validate_actions()

    def _validate_condition(self):
        if self.condition:
            try:
                compile(self.condition, "<condition>", "eval")
            except SyntaxError as e:
                frappe.throw(f"Invalid condition expression: {e}")

    def _validate_actions(self):
        if not self.actions:
            frappe.throw("At least one action is required")
        for action in self.actions:
            if not action.enabled:
                continue
            if action.action_type == "Sync Job" and not action.sync_job_type:
                frappe.throw(
                    f"Row {action.idx}: Sync Job Type is required for Sync Job actions"
                )
            elif action.action_type == "Method Call" and not action.method_path:
                frappe.throw(
                    f"Row {action.idx}: Method Path is required for Method Call actions"
                )
            elif (
                action.action_type in ("Create Document", "Update Document")
                and not action.target_doctype
            ):
                frappe.throw(
                    f"Row {action.idx}: Target DocType is required for {action.action_type} actions"
                )

/**
 * Connect Emission Rule — Form Script
 *
 * Adds:
 * - "Manual Push" button to test-fire a rule with a specific document
 * - Rule validation helpers
 */
frappe.ui.form.on("Connect Emission Rule", {
	refresh(frm) {
		if (!frm.doc.__islocal && frm.doc.enabled) {
			_add_manual_push_button(frm);
		}
		_add_recent_logs_section(frm);
	},
});

function _add_manual_push_button(frm) {
	frm.add_custom_button(__("Manual Push"), function () {
		const d = new frappe.ui.Dialog({
			title: __("Manual Push — {0}", [frm.doc.rule_name]),
			fields: [
				{
					label: __("Source DocType"),
					fieldname: "doctype",
					fieldtype: "Link",
					options: "DocType",
					default: frm.doc.source_doctype,
					read_only: 1,
					reqd: 1,
				},
				{
					label: __("Document Name"),
					fieldname: "docname",
					fieldtype: "Dynamic Link",
					options: "doctype",
					reqd: 1,
					description: __(
						"Select a specific {0} to push through this rule",
						[frm.doc.source_doctype]
					),
				},
			],
			primary_action_label: __("Push to Broker"),
			primary_action(values) {
				d.hide();
				frappe.call({
					method: "connect.api.manual_produce",
					args: {
						doctype: values.doctype,
						docname: values.docname,
						rule_name: frm.doc.name,
					},
					freeze: true,
					freeze_message: __(
						"Producing message for {0} {1}...",
						[values.doctype, values.docname]
					),
					callback(r) {
						if (r.message && r.message.ok) {
							frappe.show_alert({
								message: __(
									"Message produced! Key: {0}",
									[r.message.data.idempotency_key]
								),
								indicator: "green",
							});
						} else {
							frappe.msgprint({
								title: __("Push Failed"),
								message: r.message
									? r.message.message
									: __("Unknown error"),
								indicator: "red",
							});
						}
					},
				});
			},
		});
		d.show();
	}, __("Actions"));
}

function _add_recent_logs_section(frm) {
	if (frm.doc.__islocal) return;

	frappe.call({
		method: "frappe.client.get_count",
		args: {
			doctype: "Connect Message Log",
			filters: { rule_name: frm.doc.name },
		},
		async: true,
		callback(r) {
			if (r.message) {
				frm.dashboard.add_indicator(
					__("Total Messages: {0}", [r.message]),
					r.message > 0 ? "blue" : "grey"
				);
			}
		},
	});

	frappe.call({
		method: "frappe.client.get_count",
		args: {
			doctype: "Connect Message Log",
			filters: { rule_name: frm.doc.name, status: "Failed" },
		},
		async: true,
		callback(r) {
			if (r.message && r.message > 0) {
				frm.dashboard.add_indicator(
					__("Failed: {0}", [r.message]),
					"red"
				);
			}
		},
	});
}

/**
 * Connect Message Log — Form Script
 *
 * Adds:
 * - Status color badges in header
 * - Retry failed message button
 * - Link to source document
 */
frappe.ui.form.on("Connect Message Log", {
	refresh(frm) {
		_set_status_indicator(frm);
		_add_retry_button(frm);
		_add_source_link(frm);
	},
});

function _set_status_indicator(frm) {
	const status = frm.doc.status;
	const status_colors = {
		Pending: "orange",
		Sent: "blue",
		Delivered: "blue",
		Failed: "red",
		Processed: "green",
		Skipped: "grey",
		"Dead Letter": "darkgrey",
	};

	const color = status_colors[status] || "grey";
	frm.page.set_indicator(__(status), color);
}

function _add_retry_button(frm) {
	if (frm.doc.status !== "Failed" || frm.doc.direction !== "Produced") return;
	if (!frm.doc.source_doctype || !frm.doc.source_docname || !frm.doc.rule_name)
		return;

	frm.add_custom_button(__("Retry Production"), function () {
		frappe.confirm(
			__(
				"This will re-produce the message for {0} {1} using rule '{2}'. Continue?",
				[frm.doc.source_doctype, frm.doc.source_docname, frm.doc.rule_name]
			),
			function () {
				frappe.call({
					method: "connect.api.manual_produce",
					args: {
						doctype: frm.doc.source_doctype,
						docname: frm.doc.source_docname,
						rule_name: frm.doc.rule_name,
					},
					freeze: true,
					freeze_message: __("Producing message..."),
					callback: function (r) {
						if (r.message && r.message.ok) {
							frappe.show_alert({
								message: __(
									"Message re-produced with key: {0}",
									[r.message.data.idempotency_key]
								),
								indicator: "green",
							});
							frm.reload_doc();
						} else {
							frappe.msgprint({
								title: __("Production Failed"),
								message: r.message
									? r.message.message
									: __("Unknown error"),
								indicator: "red",
							});
						}
					},
				});
			}
		);
	});
}

function _add_source_link(frm) {
	if (frm.doc.source_doctype && frm.doc.source_docname) {
		frm.dashboard.add_comment(
			__("Source: {0}", [
				`<a href="/app/${frappe.router.slug(frm.doc.source_doctype)}/${frm.doc.source_docname}">
					${frm.doc.source_doctype} ${frm.doc.source_docname}
				</a>`,
			]),
			"blue",
			true
		);
	}
}

/**
 * Connect Message Log — List View Script
 *
 * Adds:
 * - Color-coded status indicators in list view
 * - Quick filters for common statuses
 */
frappe.listview_settings["Connect Message Log"] = {
	get_indicator(doc) {
		const status_map = {
			Pending: [__("Pending"), "orange", "status,=,Pending"],
			Sent: [__("Sent"), "blue", "status,=,Sent"],
			Delivered: [__("Delivered"), "blue", "status,=,Delivered"],
			Failed: [__("Failed"), "red", "status,=,Failed"],
			Processed: [__("Processed"), "green", "status,=,Processed"],
			Skipped: [__("Skipped"), "grey", "status,=,Skipped"],
			"Dead Letter": [__("Dead Letter"), "darkgrey", "status,=,Dead Letter"],
		};
		return status_map[doc.status] || [__(doc.status), "grey", ""];
	},

	formatters: {
		direction(value) {
			const icon = value === "Produced" ? "↑" : "↓";
			const color = value === "Produced" ? "#2E86C1" : "#28B463";
			return `<span style="color: ${color}; font-weight: 600;">${icon} ${value}</span>`;
		},
	},

	onload(listview) {
		// Add quick filter buttons
		listview.page.add_inner_button(__("Failed Only"), function () {
			listview.filter_area.clear().then(() => {
				listview.filter_area.add([[listview.doctype, "status", "=", "Failed"]]);
			});
		});

		listview.page.add_inner_button(__("Last Hour"), function () {
			const one_hour_ago = frappe.datetime.add_to_date(
				frappe.datetime.now_datetime(),
				{ hours: -1 }
			);
			listview.filter_area.clear().then(() => {
				listview.filter_area.add([
					[listview.doctype, "creation", ">=", one_hour_ago],
				]);
			});
		});
	},
};

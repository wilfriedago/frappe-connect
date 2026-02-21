/**
 * Connect Settings — Form Script
 *
 * Adds:
 * - "Test Connection" button (Kafka + Schema Registry health check)
 * - "Refresh Schemas" button
 * - Connection status indicators
 * - Quick stats dashboard
 */
frappe.ui.form.on("Connect Settings", {
	refresh(frm) {
		_add_test_connection_button(frm);
		_add_refresh_schemas_button(frm);
		_add_connection_status_section(frm);
		_add_stats_section(frm);
	},

	after_save(frm) {
		// Re-check connection status after settings change
		_update_connection_status(frm);
	},
});

function _add_test_connection_button(frm) {
	frm.add_custom_button(
		__("Test Connection"),
		function () {
			frappe.show_alert({
				message: __("Testing connection..."),
				indicator: "blue",
			});

			frappe.call({
				method: "connect.api.test_connection",
				freeze: true,
				freeze_message: __("Checking Kafka & Schema Registry..."),
				callback: function (r) {
					if (r.message && r.message.ok) {
						const data = r.message.data;
						const kafka_ok = data.kafka && data.kafka.status === "ok";
						const sr_ok =
							data.schema_registry && data.schema_registry.status === "ok";

						let html = '<div class="connect-health-result">';

						// Kafka status
						html += _build_health_row(
							"Kafka Brokers",
							kafka_ok,
							kafka_ok
								? data.kafka.detail
								: data.kafka.detail || "Connection failed"
						);

						// Schema Registry status
						html += _build_health_row(
							"Schema Registry",
							sr_ok,
							sr_ok
								? data.schema_registry.detail
								: data.schema_registry.detail || "Connection failed"
						);

						html += "</div>";

						frappe.msgprint({
							title: kafka_ok && sr_ok ? __("All Connected") : __("Connection Issues"),
							message: html,
							indicator: kafka_ok && sr_ok ? "green" : "red",
						});
					} else {
						frappe.msgprint({
							title: __("Connection Test Failed"),
							message: r.message
								? r.message.message
								: __("Unexpected error"),
							indicator: "red",
						});
					}
				},
				error: function () {
					frappe.msgprint({
						title: __("Error"),
						message: __("Failed to reach the server. Check your network."),
						indicator: "red",
					});
				},
			});
		},
		__("Actions")
	);
}

function _add_refresh_schemas_button(frm) {
	frm.add_custom_button(
		__("Refresh Schemas"),
		function () {
			frappe.call({
				method: "connect.api.refresh_schemas",
				freeze: true,
				freeze_message: __("Refreshing Avro schemas from registry..."),
				callback: function (r) {
					if (r.message && r.message.ok) {
						frappe.show_alert({
							message: __("Schema cache refreshed successfully"),
							indicator: "green",
						});
					} else {
						frappe.msgprint({
							title: __("Schema Refresh Failed"),
							message: r.message
								? r.message.message
								: __("Unknown error"),
							indicator: "red",
						});
					}
				},
			});
		},
		__("Actions")
	);
}

function _add_connection_status_section(frm) {
	// Only show if saved (has actual config)
	if (frm.doc.__islocal) return;

	const wrapper = frm.fields_dict.enabled
		? $(frm.fields_dict.enabled.wrapper).closest(".frappe-control").parent()
		: null;

	if (!wrapper) return;

	// Remove old status if exists
	wrapper.find(".connect-status-banner").remove();

	const $banner = $(
		'<div class="connect-status-banner" style="margin-bottom: 12px;"></div>'
	);
	wrapper.prepend($banner);

	_update_connection_status(frm, $banner);
}

function _update_connection_status(frm, $banner) {
	if (!$banner) {
		$banner = $(frm.wrapper).find(".connect-status-banner");
	}
	if (!$banner.length) return;

	$banner.html(
		'<div class="text-muted small"><i class="fa fa-spinner fa-spin"></i> Checking status...</div>'
	);

	frappe.call({
		method: "connect.api.test_connection",
		async: true,
		callback: function (r) {
			if (r.message && r.message.ok) {
				const data = r.message.data;
				const kafka_ok = data.kafka && data.kafka.status === "ok";
				const sr_ok =
					data.schema_registry && data.schema_registry.status === "ok";
				const all_ok = kafka_ok && sr_ok;

				$banner.html(
					`<div class="alert alert-${all_ok ? "success" : "warning"} p-2 mb-0" style="font-size: 0.85rem;">
						<span class="indicator-pill ${all_ok ? "green" : "orange"}"></span>
						<strong>${all_ok ? __("Connected") : __("Connection Issues")}</strong>
						<span class="text-muted ml-2">
							Kafka: ${kafka_ok ? "✓" : "✗"} &nbsp;|&nbsp; Schema Registry: ${sr_ok ? "✓" : "✗"}
						</span>
					</div>`
				);
			} else {
				$banner.html(
					`<div class="alert alert-danger p-2 mb-0" style="font-size: 0.85rem;">
						<span class="indicator-pill red"></span>
						<strong>${__("Disconnected")}</strong>
						<span class="text-muted ml-2">${r.message ? r.message.message : "Check configuration"}</span>
					</div>`
				);
			}
		},
		error: function () {
			$banner.html("");
		},
	});
}

function _add_stats_section(frm) {
	if (frm.doc.__islocal) return;

	frappe.call({
		method: "connect.api.get_kafka_stats",
		async: true,
		callback: function (r) {
			if (r.message && r.message.ok) {
				const stats = r.message.data.stats || [];
				if (!stats.length) return;

				let total_produced = 0;
				let total_consumed = 0;
				let total_failed = 0;

				stats.forEach(function (s) {
					if (s.direction === "Produced") total_produced += s.count;
					if (s.direction === "Consumed") total_consumed += s.count;
					if (s.status === "Failed" || s.status === "Dead Letter")
						total_failed += s.count;
				});

				frm.dashboard.add_indicator(
					__("Produced (24h): {0}", [total_produced]),
					total_produced > 0 ? "blue" : "grey"
				);
				frm.dashboard.add_indicator(
					__("Consumed (24h): {0}", [total_consumed]),
					total_consumed > 0 ? "green" : "grey"
				);
				frm.dashboard.add_indicator(
					__("Failed (24h): {0}", [total_failed]),
					total_failed > 0 ? "red" : "grey"
				);
			}
		},
	});
}

function _build_health_row(label, is_ok, detail) {
	const icon = is_ok ? "✓" : "✗";
	const color = is_ok ? "#28B463" : "#E74C3C";
	return `
		<div style="display: flex; align-items: center; padding: 8px 0; border-bottom: 1px solid var(--border-color);">
			<span style="color: ${color}; font-size: 18px; width: 24px; text-align: center;">${icon}</span>
			<strong style="margin-left: 8px; min-width: 140px;">${label}</strong>
			<span class="text-muted" style="margin-left: 12px;">${detail}</span>
		</div>
	`;
}

"""Rename Connect Emission Rule → Connect Hook, Connect Field Mapping → Connect Hook Field Mapping.

Pre-model-sync patch: renames DocType records, tables, and references
so that the new JSON definitions can be synced on top.
"""

import frappe


# (old_name, new_name, is_single, is_child_table)
RENAMES = [
	("Connect Field Mapping", "Connect Hook Field Mapping", False, True),
	("Connect Emission Rule", "Connect Hook", False, False),
]


def execute():
	"""Rename DocTypes from Connect Emission Rule to Connect Hook."""
	for old_name, new_name, is_single, is_child_table in RENAMES:
		if not frappe.db.exists("DocType", old_name):
			continue

		print(f"  Renaming DocType: {old_name} → {new_name}")

		# 1. Rename the database table
		if not is_single:
			old_table = f"tab{old_name}"
			new_table = f"tab{new_name}"
			if frappe.db.table_exists(old_table) and not frappe.db.table_exists(new_table):
				frappe.db.sql(f"ALTER TABLE `{old_table}` RENAME TO `{new_table}`")

		# 2. Update the DocType record name
		frappe.db.sql(
			"UPDATE `tabDocType` SET name=%s WHERE name=%s",
			(new_name, old_name),
		)

		# 3. Update DocField parent references
		frappe.db.sql(
			"UPDATE `tabDocField` SET parent=%s WHERE parent=%s",
			(new_name, old_name),
		)

		# 4. Update DocField options (Link fields pointing to old name)
		frappe.db.sql(
			"UPDATE `tabDocField` SET options=%s WHERE options=%s",
			(new_name, old_name),
		)

		# 5. Update DocPerm parent
		frappe.db.sql(
			"UPDATE `tabDocPerm` SET parent=%s WHERE parent=%s",
			(new_name, old_name),
		)

		# 6. Update Property Setter references
		if frappe.db.table_exists("tabProperty Setter"):
			frappe.db.sql(
				"UPDATE `tabProperty Setter` SET doc_type=%s WHERE doc_type=%s",
				(new_name, old_name),
			)

		# 7. Update Custom Field references
		if frappe.db.table_exists("tabCustom Field"):
			frappe.db.sql(
				"UPDATE `tabCustom Field` SET dt=%s WHERE dt=%s",
				(new_name, old_name),
			)

	# 8. Update child table parenttype references
	#    Connect Hook Field Mapping rows have parenttype = "Connect Emission Rule"
	if frappe.db.table_exists("tabConnect Hook Field Mapping"):
		frappe.db.sql(
			"UPDATE `tabConnect Hook Field Mapping` SET parenttype='Connect Hook' WHERE parenttype='Connect Emission Rule'"
		)

	# 9. Update Number Card references
	frappe.db.sql(
		"UPDATE `tabNumber Card` SET document_type='Connect Hook' WHERE document_type='Connect Emission Rule'"
	)
	frappe.db.sql(
		"UPDATE `tabNumber Card` SET name='Active Hooks', label='Active Hooks' WHERE name='Active Emission Rules'"
	)
	frappe.db.sql(
		"""UPDATE `tabNumber Card` SET filters_json=REPLACE(filters_json, 'Connect Emission Rule', 'Connect Hook') WHERE filters_json LIKE '%%Connect Emission Rule%%'"""
	)

	# 10. Update Workspace references
	frappe.db.sql(
		"""UPDATE `tabWorkspace Link` SET label='Connect Hook', link_to='Connect Hook' WHERE link_to='Connect Emission Rule'"""
	)
	frappe.db.sql(
		"""UPDATE `tabWorkspace Shortcut` SET link_to='Connect Hook', label='Hooks' WHERE link_to='Connect Emission Rule'"""
	)
	frappe.db.sql(
		"""UPDATE `tabWorkspace Number Card` SET number_card_name='Active Hooks', label='Active Hooks' WHERE number_card_name='Active Emission Rules'"""
	)

	# 11. Update Connect Message Log link references (rule_name field options will be updated by model sync)
	#     Data values are rule names (not DocType names), so they stay the same.

	# 12. Update the previous rename patch's references
	frappe.db.sql(
		"""UPDATE `tabPatch Log` SET patch=REPLACE(patch, 'Connect Emission Rule', 'Connect Hook') WHERE patch LIKE '%%Connect Emission Rule%%'"""
	)

	frappe.db.commit()
	print("  DocType rename complete (Emission Rule → Hook).")

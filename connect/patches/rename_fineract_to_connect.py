"""Rename Fineract DocTypes to Connect DocTypes.

Pre-model-sync patch: renames DocType records, tables, and references
so that the new JSON definitions can be synced on top.
"""

import frappe


# (old_name, new_name, is_single, is_child_table)
RENAMES = [
    ("Fineract Field Mapping", "Connect Field Mapping", False, True),
    ("Fineract Event Handler Action", "Connect Event Handler Action", False, True),
    ("Fineract Avro Schema", "Connect Avro Schema", False, False),
    ("Fineract Event Emission Rule", "Connect Emission Rule", False, False),
    ("Fineract Kafka Log", "Connect Message Log", False, False),
    ("Fineract Event Handler", "Connect Event Handler", False, False),
    ("Fineract Kafka Settings", "Connect Settings", True, False),
]


def execute():
    """Rename DocTypes from Fineract* to Connect*."""
    for old_name, new_name, is_single, is_child_table in RENAMES:
        if not frappe.db.exists("DocType", old_name):
            continue

        print(f"  Renaming DocType: {old_name} → {new_name}")

        # 1. Rename the regular database table (non-single, non-child)
        if not is_single:
            old_table = f"tab{old_name}"
            new_table = f"tab{new_name}"
            if frappe.db.table_exists(old_table) and not frappe.db.table_exists(
                new_table
            ):
                frappe.db.sql(f"ALTER TABLE `{old_table}` RENAME TO `{new_table}`")

        # 2. Update the DocType record name
        frappe.db.sql(
            "UPDATE `tabDocType` SET name=%s, module='Connect' WHERE name=%s",
            (new_name, old_name),
        )

        # 3. Update Singles table (for Single DocTypes)
        if is_single:
            frappe.db.sql(
                "UPDATE `tabSingles` SET doctype=%s WHERE doctype=%s",
                (new_name, old_name),
            )

        # 4. Update DocField parent references
        frappe.db.sql(
            "UPDATE `tabDocField` SET parent=%s WHERE parent=%s",
            (new_name, old_name),
        )

        # 5. Update DocField options (Link fields pointing to old name)
        frappe.db.sql(
            "UPDATE `tabDocField` SET options=%s WHERE options=%s",
            (new_name, old_name),
        )

        # 6. Update DocPerm parent
        frappe.db.sql(
            "UPDATE `tabDocPerm` SET parent=%s WHERE parent=%s",
            (new_name, old_name),
        )

        # 7. Update Property Setter references
        if frappe.db.table_exists("tabProperty Setter"):
            frappe.db.sql(
                "UPDATE `tabProperty Setter` SET doc_type=%s WHERE doc_type=%s",
                (new_name, old_name),
            )

        # 8. Update Custom Field references
        if frappe.db.table_exists("tabCustom Field"):
            frappe.db.sql(
                "UPDATE `tabCustom Field` SET dt=%s WHERE dt=%s",
                (new_name, old_name),
            )

    # 9. Fix cross-references in renamed tables
    #    Connect Message Log has Link fields pointing to old DocType names
    if frappe.db.table_exists("tabConnect Message Log"):
        # These columns reference rules/handlers/logs by Link field
        # The data values are document names, not DocType names, so they're fine.
        # But if any column stores the DocType name itself, update it.
        pass

    frappe.db.commit()
    print("  DocType rename complete.")

"""Sync Job Type: Fineract Client Sync.

Consumes Kafka business events about clients and syncs them
with corresponding ERPNext entities (e.g., Customer).
"""
import frappe

from connect.utils.logging import log_error, log_info


def execute(**kwargs):
    """Entry point for the Fineract Client Sync job.

    Called by Frappe Tweaks sync job framework.

    Args:
        **kwargs: Contains event_data, kafka_log_name, and handler metadata.
    """
    event_data = kwargs.get("event_data", {})
    kafka_log_name = kwargs.get("kafka_log_name")
    business_event_type = kwargs.get("business_event_type", "")

    log_info(
        "fineract_client_sync",
        f"Processing client event: {business_event_type}",
        kafka_log=kafka_log_name,
    )

    try:
        payload = event_data.get("payload", {})
        external_id = str(payload.get("clientId", ""))

        if not external_id:
            log_error("fineract_client_sync", "No clientId in event payload")
            return

        # Determine action based on event type
        if "Created" in business_event_type or "Activated" in business_event_type:
            _create_or_update_customer(external_id, payload)
        elif "Updated" in business_event_type:
            _create_or_update_customer(external_id, payload)
        elif "Closed" in business_event_type or "Rejected" in business_event_type:
            _deactivate_customer(external_id, payload)
        else:
            log_info(
                "fineract_client_sync",
                f"Unhandled client event type: {business_event_type}",
            )

        # Mark Kafka log as processed
        if kafka_log_name:
            kafka_log = frappe.get_doc("Fineract Kafka Log", kafka_log_name)
            kafka_log.mark_processed()

    except Exception as exc:
        log_error("fineract_client_sync", f"Client sync failed: {exc}", exc=exc)
        if kafka_log_name:
            try:
                kafka_log = frappe.get_doc("Fineract Kafka Log", kafka_log_name)
                kafka_log.mark_failed(str(exc))
            except Exception:
                pass
        raise


def _create_or_update_customer(external_id: str, payload: dict):
    """Create or update a Customer from Fineract client data."""
    customer_name = _find_customer_by_external_id(external_id)

    display_name = payload.get("displayName", "")
    first_name = payload.get("firstname", "")
    last_name = payload.get("lastname", "")
    full_name = display_name or f"{first_name} {last_name}".strip()

    if customer_name:
        # Update existing
        customer = frappe.get_doc("Customer", customer_name)
        customer.customer_name = full_name
        if payload.get("mobileNo"):
            # Store mobile on linked contact if needed
            pass
        customer.flags.ignore_permissions = True
        customer.save()
        log_info("fineract_client_sync", f"Updated Customer {customer_name} from Fineract client {external_id}")
    else:
        # Create new
        customer = frappe.new_doc("Customer")
        customer.customer_name = full_name
        customer.customer_type = "Individual"
        customer.customer_group = frappe.db.get_single_value(
            "Selling Settings", "customer_group"
        ) or "Individual"
        customer.territory = frappe.db.get_single_value(
            "Selling Settings", "territory"
        ) or "All Territories"
        # Store external reference
        customer.fineract_client_id = external_id
        customer.flags.ignore_permissions = True
        customer.insert()
        log_info(
            "fineract_client_sync",
            f"Created Customer {customer.name} from Fineract client {external_id}",
        )


def _deactivate_customer(external_id: str, payload: dict):
    """Mark a Customer as disabled when Fineract client is closed/rejected."""
    customer_name = _find_customer_by_external_id(external_id)
    if not customer_name:
        log_info(
            "fineract_client_sync",
            f"No Customer found for Fineract client {external_id} — skipping deactivation",
        )
        return

    customer = frappe.get_doc("Customer", customer_name)
    customer.disabled = 1
    customer.flags.ignore_permissions = True
    customer.save()
    log_info("fineract_client_sync", f"Disabled Customer {customer_name} (Fineract client {external_id})")


def _find_customer_by_external_id(external_id: str):
    """Find a Customer by its Fineract external ID."""
    result = frappe.db.get_value(
        "Customer",
        {"fineract_client_id": external_id},
        "name",
    )
    return result

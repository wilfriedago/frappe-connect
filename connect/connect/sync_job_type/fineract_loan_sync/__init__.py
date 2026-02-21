"""Sync Job Type: Fineract Loan Sync.

Consumes Kafka business events about loans and syncs them
with corresponding ERPNext entities (e.g., Loan).
"""
import frappe

from connect.utils.logging import log_error, log_info


def execute(**kwargs):
    """Entry point for the Fineract Loan Sync job.

    Called by Frappe Tweaks sync job framework.

    Args:
        **kwargs: Contains event_data, kafka_log_name, and handler metadata.
    """
    event_data = kwargs.get("event_data", {})
    kafka_log_name = kwargs.get("kafka_log_name")
    business_event_type = kwargs.get("business_event_type", "")

    log_info(
        "fineract_loan_sync",
        f"Processing loan event: {business_event_type}",
        kafka_log=kafka_log_name,
    )

    try:
        payload = event_data.get("payload", {})
        loan_id = str(payload.get("loanId", ""))

        if not loan_id:
            log_error("fineract_loan_sync", "No loanId in event payload")
            return

        # Route based on event type
        if "Approved" in business_event_type:
            _handle_loan_approved(loan_id, payload)
        elif "Disbursed" in business_event_type or "Disburse" in business_event_type:
            _handle_loan_disbursed(loan_id, payload)
        elif "Repayment" in business_event_type:
            _handle_loan_repayment(loan_id, payload)
        elif "Closed" in business_event_type or "WrittenOff" in business_event_type:
            _handle_loan_closed(loan_id, payload)
        elif "Created" in business_event_type or "Applied" in business_event_type:
            _handle_loan_created(loan_id, payload)
        else:
            log_info(
                "fineract_loan_sync",
                f"Unhandled loan event type: {business_event_type}",
            )

        # Mark Kafka log as processed
        if kafka_log_name:
            kafka_log = frappe.get_doc("Fineract Kafka Log", kafka_log_name)
            kafka_log.mark_processed()

    except Exception as exc:
        log_error("fineract_loan_sync", f"Loan sync failed: {exc}", exc=exc)
        if kafka_log_name:
            try:
                kafka_log = frappe.get_doc("Fineract Kafka Log", kafka_log_name)
                kafka_log.mark_failed(str(exc))
            except Exception:
                pass
        raise


def _handle_loan_created(loan_id: str, payload: dict):
    """Create a Loan record from Fineract loan application event."""
    existing = _find_loan_by_external_id(loan_id)
    if existing:
        log_info("fineract_loan_sync", f"Loan {existing} already exists for Fineract loan {loan_id}")
        return

    # Map Fineract client to ERPNext Customer/Applicant
    client_id = str(payload.get("clientId", ""))
    applicant = ""
    if client_id:
        applicant = frappe.db.get_value("Customer", {"fineract_client_id": client_id}, "name") or ""

    loan = frappe.new_doc("Loan")
    loan.fineract_loan_id = loan_id
    loan.applicant_type = "Customer"
    loan.applicant = applicant
    loan.loan_amount = payload.get("principal", 0)
    loan.repayment_method = "Repay Over Number of Periods"
    loan.repayment_periods = payload.get("numberOfRepayments", 0)

    loan.flags.ignore_permissions = True
    loan.flags.ignore_mandatory = True
    loan.insert()

    log_info("fineract_loan_sync", f"Created Loan {loan.name} from Fineract loan {loan_id}")


def _handle_loan_approved(loan_id: str, payload: dict):
    """Update Loan status when approved in Fineract."""
    loan_name = _find_loan_by_external_id(loan_id)
    if not loan_name:
        log_info("fineract_loan_sync", f"No Loan found for Fineract loan {loan_id}")
        return

    loan = frappe.get_doc("Loan", loan_name)
    if loan.docstatus == 0:
        # Submit the loan to mark it as approved
        loan.flags.ignore_permissions = True
        try:
            loan.submit()
            log_info("fineract_loan_sync", f"Approved Loan {loan_name} (Fineract loan {loan_id})")
        except Exception as exc:
            log_error("fineract_loan_sync", f"Failed to approve Loan {loan_name}: {exc}", exc=exc)


def _handle_loan_disbursed(loan_id: str, payload: dict):
    """Handle loan disbursement event from Fineract."""
    loan_name = _find_loan_by_external_id(loan_id)
    if not loan_name:
        log_info("fineract_loan_sync", f"No Loan found for Fineract loan {loan_id}")
        return

    # Create a Loan Disbursement Entry or update loan status
    loan = frappe.get_doc("Loan", loan_name)
    loan.disbursement_date = payload.get("actualDisbursementDate")
    loan.disbursed_amount = payload.get("principal", loan.loan_amount)
    loan.flags.ignore_permissions = True
    loan.save()

    log_info("fineract_loan_sync", f"Disbursed Loan {loan_name} (Fineract loan {loan_id})")


def _handle_loan_repayment(loan_id: str, payload: dict):
    """Handle loan repayment event from Fineract."""
    loan_name = _find_loan_by_external_id(loan_id)
    if not loan_name:
        log_info("fineract_loan_sync", f"No Loan found for Fineract loan {loan_id}")
        return

    # Log repayment — detailed Journal Entry creation can be added
    amount = payload.get("amount", 0)
    log_info(
        "fineract_loan_sync",
        f"Repayment of {amount} received for Loan {loan_name} (Fineract loan {loan_id})",
    )


def _handle_loan_closed(loan_id: str, payload: dict):
    """Handle loan closure/write-off event from Fineract."""
    loan_name = _find_loan_by_external_id(loan_id)
    if not loan_name:
        log_info("fineract_loan_sync", f"No Loan found for Fineract loan {loan_id}")
        return

    loan = frappe.get_doc("Loan", loan_name)
    loan.status = "Closed"
    loan.flags.ignore_permissions = True
    loan.save()

    log_info("fineract_loan_sync", f"Closed Loan {loan_name} (Fineract loan {loan_id})")


def _find_loan_by_external_id(loan_id: str):
    """Find a Loan by its Fineract external ID."""
    return frappe.db.get_value("Loan", {"fineract_loan_id": loan_id}, "name")

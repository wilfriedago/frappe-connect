"""Structured logging for Kafka operations."""

import logging
import traceback

import frappe

logger = logging.getLogger("connect")


def log_info(
	title: str,
	message: str,
	reference_doctype: str | None = None,
	reference_name: str | None = None,
):
	"""Log an informational message."""
	logger.info("%s - %s", title, message)


def log_error(
	title: str,
	message: str,
	exc: Exception | None = None,
	reference_doctype: str | None = None,
	reference_name: str | None = None,
):
	"""Record an error to both the logger and frappe error log."""
	if exc:
		tb = traceback.format_exc()
		logger.error("%s - %s\n%s", title, message, tb)
		try:
			frappe.log_error(tb, title)
		except Exception:
			pass  # Don't fail if frappe is not initialized
	else:
		logger.error("%s - %s", title, message)
		try:
			frappe.log
			frappe.log_error(message, title)
		except Exception:
			pass

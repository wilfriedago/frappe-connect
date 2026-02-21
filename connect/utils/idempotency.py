"""Idempotency key generation and dedup checks."""
import hashlib
import uuid

import frappe


def generate_idempotency_key(
    doctype: str,
    docname: str,
    event: str,
    command_type: str,
    rule_name: str,
) -> str:
    """Generate a deterministic idempotency key for a producer message.

    The key is a SHA-256 hash of the document identity + event + rule,
    ensuring the same document event with the same rule always produces
    the same key (preventing duplicates).
    """
    raw = f"{doctype}:{docname}:{event}:{command_type}:{rule_name}"
    return hashlib.sha256(raw.encode()).hexdigest()


def generate_consumer_idempotency_key(
    topic: str,
    partition: int,
    offset: int,
) -> str:
    """Generate an idempotency key for a consumed message.

    Based on topic/partition/offset which is unique per message.
    """
    raw = f"{topic}:{partition}:{offset}"
    return hashlib.sha256(raw.encode()).hexdigest()


def check_idempotency(idempotency_key: str) -> bool:
    """Check if a message with this idempotency key has already been processed.

    Returns True if the key already exists (duplicate), False if new.
    """
    exists = frappe.db.exists(
        "Fineract Kafka Log",
        {
            "idempotency_key": idempotency_key,
            "status": ("in", ["Delivered", "Processed", "Skipped"]),
        },
    )
    return bool(exists)


def generate_uuid() -> str:
    """Generate a new UUID string."""
    return str(uuid.uuid4())

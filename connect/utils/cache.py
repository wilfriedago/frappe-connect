"""Redis cache helpers for schemas and settings."""
import frappe


def cache_get(key: str):
    """Get a value from Redis cache."""
    return frappe.cache().get_value(key)


def cache_set(key: str, value, expires_in_sec: int = 300):
    """Set a value in Redis cache with TTL."""
    frappe.cache().set_value(key, value, expires_in_sec=expires_in_sec)


def cache_delete(key: str):
    """Delete a value from Redis cache."""
    frappe.cache().delete_value(key)


def cache_get_or_set(key: str, generator, expires_in_sec: int = 300):
    """Get from cache or generate and cache the value."""
    value = cache_get(key)
    if value is not None:
        return value
    value = generator()
    if value is not None:
        cache_set(key, value, expires_in_sec=expires_in_sec)
    return value

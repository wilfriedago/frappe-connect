"""Tests for idempotency key generation (pure functions, no DB dependency)."""
import unittest

from connect.utils.idempotency import (
    generate_consumer_idempotency_key,
    generate_idempotency_key,
    generate_uuid,
)


class TestGenerateIdempotencyKey(unittest.TestCase):
    """Test the deterministic producer idempotency key generator."""

    def test_deterministic(self):
        """Same inputs produce the same key."""
        key1 = generate_idempotency_key("Customer", "CUST-001", "after_insert", "CreateClient", "Rule 1")
        key2 = generate_idempotency_key("Customer", "CUST-001", "after_insert", "CreateClient", "Rule 1")
        self.assertEqual(key1, key2)

    def test_different_inputs_different_keys(self):
        """Different inputs produce different keys."""
        key1 = generate_idempotency_key("Customer", "CUST-001", "after_insert", "CreateClient", "Rule 1")
        key2 = generate_idempotency_key("Customer", "CUST-002", "after_insert", "CreateClient", "Rule 1")
        self.assertNotEqual(key1, key2)

    def test_different_events_different_keys(self):
        key1 = generate_idempotency_key("Customer", "CUST-001", "after_insert", "CreateClient", "Rule 1")
        key2 = generate_idempotency_key("Customer", "CUST-001", "on_update", "CreateClient", "Rule 1")
        self.assertNotEqual(key1, key2)

    def test_key_is_sha256_hex(self):
        """Key should be a 64-char hex string (SHA-256)."""
        key = generate_idempotency_key("Item", "ITEM-001", "on_update", "UpdateItem", "R1")
        self.assertEqual(len(key), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in key))


class TestGenerateConsumerIdempotencyKey(unittest.TestCase):
    """Test the consumer idempotency key generator."""

    def test_deterministic(self):
        key1 = generate_consumer_idempotency_key("fineract.events", 0, 100)
        key2 = generate_consumer_idempotency_key("fineract.events", 0, 100)
        self.assertEqual(key1, key2)

    def test_different_offsets(self):
        key1 = generate_consumer_idempotency_key("fineract.events", 0, 100)
        key2 = generate_consumer_idempotency_key("fineract.events", 0, 101)
        self.assertNotEqual(key1, key2)

    def test_different_partitions(self):
        key1 = generate_consumer_idempotency_key("fineract.events", 0, 100)
        key2 = generate_consumer_idempotency_key("fineract.events", 1, 100)
        self.assertNotEqual(key1, key2)


class TestGenerateUUID(unittest.TestCase):
    """Test the UUID generator."""

    def test_returns_string(self):
        self.assertIsInstance(generate_uuid(), str)

    def test_unique(self):
        uuids = {generate_uuid() for _ in range(100)}
        self.assertEqual(len(uuids), 100)

    def test_format(self):
        uid = generate_uuid()
        self.assertEqual(len(uid), 36)
        self.assertEqual(uid.count("-"), 4)


if __name__ == "__main__":
    unittest.main()

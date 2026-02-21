"""Tests for cache utility module.

Uses mock frappe.cache() to test cache helpers without Redis.
"""
import unittest
from unittest.mock import MagicMock, patch


class TestCacheHelpers(unittest.TestCase):
    """Test cache_get, cache_set, cache_delete, cache_get_or_set."""

    @patch("connect.utils.cache.frappe")
    def test_cache_get(self, mock_frappe):
        """cache_get calls frappe.cache().get_value."""
        from connect.utils.cache import cache_get

        mock_cache = MagicMock()
        mock_cache.get_value.return_value = "cached_value"
        mock_frappe.cache.return_value = mock_cache

        result = cache_get("my_key")
        mock_cache.get_value.assert_called_once_with("my_key")
        self.assertEqual(result, "cached_value")

    @patch("connect.utils.cache.frappe")
    def test_cache_set(self, mock_frappe):
        """cache_set calls frappe.cache().set_value."""
        from connect.utils.cache import cache_set

        mock_cache = MagicMock()
        mock_frappe.cache.return_value = mock_cache

        cache_set("my_key", "my_value", expires_in_sec=300)
        mock_cache.set_value.assert_called_once_with(
            "my_key", "my_value", expires_in_sec=300
        )

    @patch("connect.utils.cache.frappe")
    def test_cache_delete(self, mock_frappe):
        """cache_delete calls frappe.cache().delete_value."""
        from connect.utils.cache import cache_delete

        mock_cache = MagicMock()
        mock_frappe.cache.return_value = mock_cache

        cache_delete("my_key")
        mock_cache.delete_value.assert_called_once_with("my_key")

    @patch("connect.utils.cache.frappe")
    def test_cache_get_or_set_hit(self, mock_frappe):
        """cache_get_or_set returns cached value when present."""
        from connect.utils.cache import cache_get_or_set

        mock_cache = MagicMock()
        mock_cache.get_value.return_value = "exists"
        mock_frappe.cache.return_value = mock_cache

        fetcher = MagicMock(return_value="fresh")
        result = cache_get_or_set("key", fetcher, expires_in_sec=60)

        self.assertEqual(result, "exists")
        fetcher.assert_not_called()

    @patch("connect.utils.cache.frappe")
    def test_cache_get_or_set_miss(self, mock_frappe):
        """cache_get_or_set calls fetcher and caches on miss."""
        from connect.utils.cache import cache_get_or_set

        mock_cache = MagicMock()
        mock_cache.get_value.return_value = None
        mock_frappe.cache.return_value = mock_cache

        fetcher = MagicMock(return_value="computed")
        result = cache_get_or_set("key", fetcher, expires_in_sec=120)

        self.assertEqual(result, "computed")
        fetcher.assert_called_once()
        mock_cache.set_value.assert_called_once_with(
            "key", "computed", expires_in_sec=120
        )


if __name__ == "__main__":
    unittest.main()

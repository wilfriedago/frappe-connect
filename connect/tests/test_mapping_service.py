"""Tests for the mapping service (field resolution and payload building).

Uses mock document objects to avoid Frappe DB dependency.
"""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


class TestBuildPayload(unittest.TestCase):
    """Test build_payload with various source_type mappings."""

    def _make_mapping(self, **kwargs):
        """Create a mock field mapping row."""
        defaults = {
            "avro_field": "testField",
            "avro_type": "string",
            "source_type": "Field",
            "source_field": "",
            "source_expression": "",
            "static_value": "",
            "method_path": "",
            "is_nullable": 0,
            "default_value": "",
        }
        defaults.update(kwargs)
        return SimpleNamespace(**defaults)

    def _make_doc(self, **kwargs):
        """Create a mock Frappe document."""
        doc = MagicMock()
        doc.get = MagicMock(side_effect=lambda k, default=None: kwargs.get(k, default))
        doc.doctype = kwargs.get("doctype", "Customer")
        doc.name = kwargs.get("name", "CUST-001")
        return doc

    @patch("connect.services.mapping_service.frappe")
    def test_field_source(self, mock_frappe):
        """Field source_type reads from doc.get()."""
        from connect.services.mapping_service import build_payload

        doc = self._make_doc(customer_name="John Doe")
        mapping = self._make_mapping(
            avro_field="clientName",
            source_type="Field",
            source_field="customer_name",
        )
        result = build_payload(doc, [mapping])
        self.assertEqual(result["clientName"], "John Doe")

    @patch("connect.services.mapping_service.frappe")
    def test_static_source(self, mock_frappe):
        """Static source_type uses the fixed static_value."""
        from connect.services.mapping_service import build_payload

        doc = self._make_doc()
        mapping = self._make_mapping(
            avro_field="source",
            source_type="Static",
            static_value="erpnext",
        )
        result = build_payload(doc, [mapping])
        self.assertEqual(result["source"], "erpnext")

    @patch("connect.services.mapping_service.frappe")
    def test_expression_source(self, mock_frappe):
        """Expression source_type evaluates safely."""
        from connect.services.mapping_service import build_payload

        doc = self._make_doc(first="John", last="Doe")
        mock_frappe.safe_eval = lambda expr, eval_globals=None: eval(
            expr, {"doc": eval_globals["doc"]}
        )
        mapping = self._make_mapping(
            avro_field="fullName",
            avro_type="string",
            source_type="Expression",
            source_expression='doc.get("first") + " " + doc.get("last")',
        )
        result = build_payload(doc, [mapping])
        self.assertEqual(result["fullName"], "John Doe")

    @patch("connect.services.mapping_service.frappe")
    def test_method_source(self, mock_frappe):
        """Method source_type calls the referenced function."""
        from connect.services.mapping_service import build_payload

        doc = self._make_doc()
        mock_frappe.get_attr = MagicMock(return_value=lambda d: "computed-value")

        mapping = self._make_mapping(
            avro_field="computed",
            source_type="Method",
            method_path="myapp.utils.get_computed",
        )
        result = build_payload(doc, [mapping])
        self.assertEqual(result["computed"], "computed-value")

    @patch("connect.services.mapping_service.frappe")
    def test_nullable_field_uses_none(self, mock_frappe):
        """Nullable field resolves to None when source is missing."""
        from connect.services.mapping_service import build_payload

        doc = self._make_doc()
        mapping = self._make_mapping(
            avro_field="optional",
            avro_type="string",
            source_type="Field",
            source_field="nonexistent",
            is_nullable=1,
        )
        result = build_payload(doc, [mapping])
        self.assertIsNone(result["optional"])

    @patch("connect.services.mapping_service.frappe")
    def test_default_value_applied(self, mock_frappe):
        """Default value is used when source resolves to None."""
        from connect.services.mapping_service import build_payload

        doc = self._make_doc()
        mapping = self._make_mapping(
            avro_field="withDefault",
            avro_type="string",
            source_type="Field",
            source_field="nonexistent",
            default_value="fallback",
        )
        result = build_payload(doc, [mapping])
        self.assertEqual(result["withDefault"], "fallback")

    @patch("connect.services.mapping_service.frappe")
    def test_multiple_mappings(self, mock_frappe):
        """Multiple mappings produce a complete payload."""
        from connect.services.mapping_service import build_payload

        doc = self._make_doc(name="CUST-001", customer_name="Jane")
        mappings = [
            self._make_mapping(avro_field="id", avro_type="string", source_type="Field", source_field="name"),
            self._make_mapping(avro_field="name", avro_type="string", source_type="Field", source_field="customer_name"),
            self._make_mapping(avro_field="source", avro_type="string", source_type="Static", static_value="erpnext"),
        ]
        result = build_payload(doc, mappings)
        self.assertEqual(len(result), 3)
        self.assertEqual(result["id"], "CUST-001")
        self.assertEqual(result["name"], "Jane")
        self.assertEqual(result["source"], "erpnext")

    @patch("connect.services.mapping_service.frappe")
    def test_int_coercion(self, mock_frappe):
        """String value coerced to int when avro_type is int."""
        from connect.services.mapping_service import build_payload

        doc = self._make_doc(amount="100")
        mapping = self._make_mapping(
            avro_field="amount",
            avro_type="int",
            source_type="Field",
            source_field="amount",
        )
        result = build_payload(doc, [mapping])
        self.assertEqual(result["amount"], 100)
        self.assertIsInstance(result["amount"], int)


if __name__ == "__main__":
    unittest.main()

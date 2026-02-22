"""Tests for Avro field validation and coercion."""

import unittest

from connect.utils.validation import validate_avro_field_value


class TestValidateAvroFieldValue(unittest.TestCase):
	"""Test type coercion and nullable handling for Avro fields."""

	# --- string type ---
	def test_string_passthrough(self):
		self.assertEqual(validate_avro_field_value("hello", "string"), "hello")

	def test_int_to_string(self):
		self.assertEqual(validate_avro_field_value(42, "string"), "42")

	def test_none_string_not_nullable(self):
		with self.assertRaises(ValueError):
			validate_avro_field_value(None, "string", is_nullable=False)

	def test_none_string_nullable(self):
		self.assertIsNone(validate_avro_field_value(None, "string", is_nullable=True))

	# --- int type ---
	def test_int_passthrough(self):
		self.assertEqual(validate_avro_field_value(42, "int"), 42)

	def test_string_to_int(self):
		self.assertEqual(validate_avro_field_value("42", "int"), 42)

	def test_invalid_string_to_int(self):
		with self.assertRaises(ValueError):
			validate_avro_field_value("not_a_number", "int")

	# --- long type ---
	def test_long_passthrough(self):
		self.assertEqual(validate_avro_field_value(2**40, "long"), 2**40)

	def test_string_to_long(self):
		self.assertEqual(validate_avro_field_value("1099511627776", "long"), 2**40)

	# --- boolean type ---
	def test_bool_passthrough(self):
		self.assertTrue(validate_avro_field_value(True, "boolean"))
		self.assertFalse(validate_avro_field_value(False, "boolean"))

	def test_string_true_to_bool(self):
		self.assertTrue(validate_avro_field_value("true", "boolean"))
		self.assertTrue(validate_avro_field_value("1", "boolean"))
		self.assertTrue(validate_avro_field_value("yes", "boolean"))

	def test_string_false_to_bool(self):
		self.assertFalse(validate_avro_field_value("false", "boolean"))
		self.assertFalse(validate_avro_field_value("0", "boolean"))

	def test_int_to_bool(self):
		self.assertTrue(validate_avro_field_value(1, "boolean"))
		self.assertFalse(validate_avro_field_value(0, "boolean"))

	# --- bytes type ---
	def test_bytes_passthrough(self):
		self.assertEqual(validate_avro_field_value(b"data", "bytes"), b"data")

	def test_string_to_bytes(self):
		self.assertEqual(validate_avro_field_value("data", "bytes"), b"data")

	# --- nullable ---
	def test_nullable_none(self):
		for avro_type in ("string", "int", "long", "boolean", "bytes"):
			self.assertIsNone(
				validate_avro_field_value(None, avro_type, is_nullable=True),
				f"Failed for type {avro_type}",
			)

	def test_not_nullable_none_raises(self):
		for avro_type in ("string", "int", "long", "boolean", "bytes"):
			with self.assertRaises(ValueError, msg=f"Should raise for type {avro_type}"):
				validate_avro_field_value(None, avro_type, is_nullable=False)

	# --- unknown type ---
	def test_unknown_type_raises(self):
		with self.assertRaises(ValueError):
			validate_avro_field_value("val", "unknown_type")

	# --- empty string ---
	def test_empty_string(self):
		self.assertEqual(validate_avro_field_value("", "string"), "")


if __name__ == "__main__":
	unittest.main()

"""Tests for the error utility module."""
import unittest

from connect.utils.errors import ERROR_CODES, HTTP_STATUS, error, ok


class TestErrorCodes(unittest.TestCase):
    """Test error code definitions."""

    def test_all_codes_are_strings(self):
        for key, code in ERROR_CODES.items():
            self.assertIsInstance(code, str, f"ERROR_CODES[{key}] should be str")

    def test_all_codes_have_prefix(self):
        for key, code in ERROR_CODES.items():
            self.assertTrue(
                code.startswith("CONNECT-"),
                f"ERROR_CODES[{key}] = {code} should start with 'CONNECT-'",
            )

    def test_http_status_coverage(self):
        """Every error code in HTTP_STATUS should exist in ERROR_CODES."""
        for code in HTTP_STATUS:
            self.assertIn(
                code,
                ERROR_CODES.values(),
                f"HTTP_STATUS key {code} not found in ERROR_CODES values",
            )


class TestOkHelper(unittest.TestCase):
    def test_ok_basic(self):
        result = ok("Success")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["message"], "Success")

    def test_ok_with_data(self):
        result = ok("Done", {"count": 5})
        self.assertEqual(result["data"], {"count": 5})


class TestErrorHelper(unittest.TestCase):
    def test_error_basic(self):
        result = error("Something failed")
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["message"], "Something failed")

    def test_error_with_code(self):
        result = error("Bad input", code=ERROR_CODES["VALIDATION_ERROR"])
        self.assertEqual(result["code"], ERROR_CODES["VALIDATION_ERROR"])

    def test_error_with_data(self):
        result = error("Fail", {"detail": "x"}, code="CONNECT-999")
        self.assertEqual(result["data"], {"detail": "x"})
        self.assertEqual(result["code"], "CONNECT-999")


if __name__ == "__main__":
    unittest.main()

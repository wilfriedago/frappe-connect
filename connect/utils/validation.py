"""Input and payload validation helpers."""
import frappe

from connect.utils.errors import ERROR_CODES


def require_fields(payload: dict, fields: list[str]):
    """Raise a validation error if any required fields are missing or falsy."""
    missing = [f for f in fields if not payload.get(f)]
    if missing:
        frappe.throw(
            f"{ERROR_CODES['VALIDATION_ERROR']}: missing fields: {', '.join(missing)}"
        )


def validate_avro_field_value(value, avro_type: str, is_nullable: bool = False):
    """Validate and coerce a value to the expected Avro type.

    Returns the coerced value or raises ValueError.
    """
    if value is None:
        if is_nullable:
            return None
        raise ValueError(f"Value is None but field is not nullable (type={avro_type})")

    type_map = {
        "string": str,
        "int": int,
        "long": int,
        "boolean": bool,
        "bytes": bytes,
    }

    target_type = type_map.get(avro_type)
    if target_type is None:
        raise ValueError(f"Unknown Avro type: {avro_type}")

    if isinstance(value, target_type):
        return value

    # Coercion
    try:
        if avro_type in ("int", "long"):
            return int(value)
        elif avro_type == "boolean":
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes")
            return bool(value)
        elif avro_type == "string":
            return str(value)
        elif avro_type == "bytes":
            if isinstance(value, str):
                return value.encode("utf-8")
            return bytes(value)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot coerce {type(value).__name__} to {avro_type}: {e}")

    return value

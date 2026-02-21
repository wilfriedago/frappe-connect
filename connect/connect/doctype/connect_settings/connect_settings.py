import frappe
from frappe.model.document import Document


class ConnectSettings(Document):
    """Singleton configuration for Connect Kafka integration.

    Provides helper methods to build configuration dicts for Kafka producer,
    consumer, and Schema Registry clients. Settings are cached in Redis and
    invalidated on save.
    """

    CACHE_KEY = "connect_settings"

    def validate(self):
        if self.security_protocol in ("SASL_SSL", "SASL_PLAINTEXT"):
            if not self.sasl_mechanism:
                frappe.throw(
                    "SASL Mechanism is required when using SASL security protocol"
                )
            if not self.sasl_username or not self.sasl_password:
                frappe.throw(
                    "SASL Username and Password are required for SASL authentication"
                )

    def on_update(self):
        self._invalidate_cache()

    def _invalidate_cache(self):
        frappe.cache().delete_value(self.CACHE_KEY)

    @staticmethod
    def get_settings() -> "ConnectSettings":
        """Get cached settings singleton."""
        cached = frappe.cache().get_value(ConnectSettings.CACHE_KEY)
        if cached:
            return cached
        settings = frappe.get_single("Connect Settings")
        frappe.cache().set_value(
            ConnectSettings.CACHE_KEY, settings, expires_in_sec=300
        )
        return settings

    def get_producer_config(self) -> dict:
        """Build confluent-kafka Producer configuration dict."""
        config = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "acks": self.producer_acks or "all",
            "retries": self.producer_retries or 3,
            "linger.ms": self.producer_linger_ms or 5,
            "enable.idempotence": bool(self.enable_idempotence),
        }
        self._apply_security_config(config)
        return config

    def get_consumer_config(self) -> dict:
        """Build confluent-kafka Consumer configuration dict."""
        config = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.consumer_group_id or "openerp-connect-consumer",
            "auto.offset.reset": self.consumer_auto_offset_reset or "earliest",
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": self.consumer_session_timeout_ms or 30000,
            "enable.auto.commit": False,
            "partition.assignment.strategy": "cooperative-sticky",
        }
        self._apply_security_config(config)
        return config

    def get_schema_registry_config(self) -> dict:
        """Build Schema Registry client configuration dict."""
        config = {"url": self.schema_registry_url}
        if self.schema_registry_username and self.schema_registry_password:
            config["basic.auth.user.info"] = (
                f"{self.schema_registry_username}:{self.get_password('schema_registry_password')}"
            )
        return config

    def _apply_security_config(self, config: dict):
        """Apply security protocol, SASL, and SSL settings to a config dict."""
        protocol = self.security_protocol or "PLAINTEXT"
        config["security.protocol"] = protocol

        if "SASL" in protocol:
            config["sasl.mechanism"] = self.sasl_mechanism
            config["sasl.username"] = self.sasl_username
            config["sasl.password"] = self.get_password("sasl_password")

        if "SSL" in protocol:
            if self.ssl_ca_location:
                config["ssl.ca.location"] = self.ssl_ca_location
            if self.ssl_certificate_location:
                config["ssl.certificate.location"] = self.ssl_certificate_location
            if self.ssl_key_location:
                config["ssl.key.location"] = self.ssl_key_location

    def get_active_doctypes(self) -> set:
        """Return cached set of DocTypes that have active emission rules."""
        cache_key = "connect_active_doctypes"
        cached = frappe.cache().get_value(cache_key)
        if cached is not None:
            return set(cached)

        doctypes = frappe.get_all(
            "Connect Emission Rule",
            filters={"enabled": 1},
            pluck="source_doctype",
            distinct=True,
        )
        frappe.cache().set_value(cache_key, doctypes, expires_in_sec=60)
        return set(doctypes)

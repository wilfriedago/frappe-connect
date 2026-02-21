"""Consumer service — consumer loop, message routing, action dispatch.

Handles the full consumer flow:
1. Poll messages from Kafka
2. Deserialize envelope + inner payload
3. Route to matching event handler
4. Dispatch actions (Sync Jobs, method calls, document operations)
"""

import json
import traceback

import frappe

from connect.kafka.consumer import KafkaConsumerClient
from connect.kafka.schema_registry import SchemaRegistryService
from connect.kafka.serialization import (
    create_avro_deserializer,
    deserialize_envelope,
    deserialize_inner_payload,
)
from connect.services.schema_service import get_schema
from connect.utils.idempotency import (
    check_idempotency,
    generate_consumer_idempotency_key,
)
from connect.utils.logging import log_error, log_info


def start_consumer(site: str, max_messages: int = 0):
    """Start the Fineract Kafka consumer loop.

    Called from the bench command. Runs until SIGTERM/SIGINT.
    """
    import frappe

    frappe.init(site=site)
    frappe.connect()

    try:
        settings = frappe.get_single("Fineract Kafka Settings")
        if not settings.consumer_enabled:
            print("Consumer is disabled in Fineract Kafka Settings")
            return

        # Build topic list
        topics = [settings.events_topic]
        if settings.dlq_consumer_enabled and settings.dlq_topic:
            topics.append(settings.dlq_topic)

        # Set up Schema Registry for deserialization
        sr_config = settings.get_schema_registry_config()
        sr_service = SchemaRegistryService(sr_config)
        avro_deserializer = create_avro_deserializer(sr_service.client)

        # Create consumer
        consumer_config = settings.get_consumer_config()
        consumer = KafkaConsumerClient(consumer_config, topics)

        def message_handler(msg):
            _process_message(msg, avro_deserializer, settings)
            # Release thread-local Frappe state to prevent memory leaks
            frappe.local.release_local()

        poll_timeout = (settings.consumer_poll_timeout_ms or 1000) / 1000.0
        consumer.start(
            message_handler=message_handler,
            poll_timeout=poll_timeout,
            max_messages=max_messages,
        )

    except Exception as e:
        log_error("Consumer startup error", str(e), exc=e)
        raise
    finally:
        frappe.destroy()


def _process_message(msg, avro_deserializer, settings):
    """Process a single consumed Kafka message."""
    topic = msg.topic()
    partition = msg.partition()
    offset = msg.offset()

    try:
        # Idempotency check
        idempotency_key = generate_consumer_idempotency_key(topic, partition, offset)
        if check_idempotency(idempotency_key):
            log_info(
                "Consumer skipping duplicate",
                f"topic={topic} partition={partition} offset={offset}",
            )
            return

        # Deserialize envelope
        envelope = deserialize_envelope(avro_deserializer, msg.value(), topic)

        # Use the idempotency key from the envelope if available
        envelope_idem_key = envelope.get("idempotencyKey", idempotency_key)

        event_type = envelope.get("type", "")
        tenant_id = envelope.get("tenantId", "")
        dataschema = envelope.get("dataschema", "")

        # Create Kafka Log
        from connect.connect.doctype.fineract_kafka_log.fineract_kafka_log import (
            FineractKafkaLog,
        )

        log = FineractKafkaLog.log_consumed(
            idempotency_key=envelope_idem_key,
            event_type=event_type,
            topic=topic,
            partition=partition,
            offset=offset,
            tenant_id=tenant_id,
        )

        # Deserialize inner payload
        inner_data = envelope.get("data", b"")
        inner_payload = {}
        if inner_data and dataschema:
            try:
                inner_schema = get_schema(dataschema, settings)
                inner_payload = deserialize_inner_payload(inner_schema, inner_data)
            except Exception as e:
                log_error("Inner payload deserialization failed", str(e), exc=e)
                log.mark_dead_letter(f"Deserialization failed: {e}")
                frappe.db.commit()
                return

        # Store payload if configured
        if settings.log_payload_on_success:
            log.db_set(
                "payload_json",
                json.dumps(inner_payload, default=str),
                update_modified=False,
            )

        # Find matching handler
        handler = _find_handler(event_type)
        if not handler:
            log.mark_skipped(f"No handler for event type: {event_type}")
            frappe.db.commit()
            return

        log.db_set("handler_name", handler.handler_name, update_modified=False)

        # Evaluate handler condition
        if handler.condition:
            try:
                result = frappe.safe_eval(
                    handler.condition,
                    eval_globals={
                        "payload": inner_payload,
                        "envelope": envelope,
                        "frappe": frappe,
                    },
                )
                if not result:
                    log.mark_skipped("Handler condition returned falsy")
                    frappe.db.commit()
                    return
            except Exception as e:
                log.mark_failed(
                    f"Handler condition eval failed: {e}", traceback.format_exc()
                )
                frappe.db.commit()
                return

        # Dispatch actions
        _dispatch_actions(handler, inner_payload, envelope, log)
        log.mark_processed()
        frappe.db.commit()

        log_info(
            "Message consumed",
            f"topic={topic} event={event_type} handler={handler.handler_name}",
        )

    except Exception as e:
        log_error(
            "Consumer message processing error",
            f"topic={topic} partition={partition} offset={offset} error={e}",
            exc=e,
        )
        # Try to log the error
        try:
            from connect.connect.doctype.fineract_kafka_log.fineract_kafka_log import (
                FineractKafkaLog,
            )

            idem_key = generate_consumer_idempotency_key(topic, partition, offset)
            error_log = FineractKafkaLog.log_consumed(
                idempotency_key=idem_key,
                event_type="UNKNOWN",
                topic=topic,
                partition=partition,
                offset=offset,
            )
            error_log.mark_dead_letter(str(e))
            frappe.db.commit()
        except Exception:
            pass


def _find_handler(event_type: str):
    """Find an enabled event handler matching the business event type."""
    handlers = frappe.get_all(
        "Fineract Event Handler",
        filters={
            "enabled": 1,
            "business_event_type": event_type,
        },
        fields=["name"],
    )
    if handlers:
        return frappe.get_doc("Fineract Event Handler", handlers[0].name)
    return None


def _dispatch_actions(handler, payload: dict, envelope: dict, log):
    """Execute all enabled actions for a handler."""
    for action in handler.actions:
        if not action.enabled:
            continue

        try:
            if action.action_type == "Sync Job":
                _dispatch_sync_job(action, payload, envelope)
            elif action.action_type == "Method Call":
                _dispatch_method_call(action, payload, envelope)
            elif action.action_type in ("Create Document", "Update Document"):
                _dispatch_document_action(action, payload, envelope)
        except Exception as e:
            log_error(
                "Action dispatch failed",
                f"handler={handler.handler_name}, action={action.action_type}, error={e}",
                exc=e,
            )


def _dispatch_sync_job(action, payload: dict, envelope: dict):
    """Enqueue a Sync Job via Frappe Tweaks."""
    try:
        from frappe_tweaks.sync_job_type.utils import enqueue_sync_job

        context = {
            "payload": payload,
            "envelope": {k: v for k, v in envelope.items() if k != "data"},
        }

        enqueue_sync_job(
            sync_job_type=action.sync_job_type,
            context=context,
            queue=action.queue or "default",
        )
        log_info("Sync Job enqueued", f"type={action.sync_job_type}")
    except ImportError:
        log_error(
            "Frappe Tweaks not installed",
            "Cannot enqueue Sync Job without Frappe Tweaks",
        )
    except Exception as e:
        log_error("Sync Job enqueue failed", str(e), exc=e)
        raise


def _dispatch_method_call(action, payload: dict, envelope: dict):
    """Enqueue a method call."""
    frappe.enqueue(
        action.method_path,
        queue=action.queue or "default",
        timeout=300,
        payload=payload,
        envelope={k: v for k, v in envelope.items() if k != "data"},
    )
    log_info("Method call enqueued", f"method={action.method_path}")


def _dispatch_document_action(action, payload: dict, envelope: dict):
    """Enqueue a document create/update action."""
    frappe.enqueue(
        "connect.background_jobs.process_business_event.process_document_action",
        queue=action.queue or "default",
        timeout=300,
        action_type=action.action_type,
        target_doctype=action.target_doctype,
        field_mapping_json=action.field_mapping_json,
        correlation_field=action.correlation_field,
        payload=payload,
    )
    log_info(
        "Document action enqueued",
        f"type={action.action_type}, doctype={action.target_doctype}",
    )

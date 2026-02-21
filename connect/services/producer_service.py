"""Producer service — rule matching, event dispatch, message production.

Handles the full producer flow:
1. Event fires → matching rules found
2. Condition evaluated
3. Background job enqueued (after commit)
"""

import json
from datetime import date, datetime

import frappe

from connect.kafka.producer import KafkaProducerClient
from connect.kafka.schema_registry import SchemaRegistryService
from connect.kafka.serialization import (
	create_avro_serializer,
	serialize_envelope,
	serialize_inner_payload,
)
from connect.services.mapping_service import build_payload
from connect.services.schema_service import get_schema
from connect.utils.idempotency import (
	check_idempotency,
	generate_idempotency_key,
	generate_uuid,
)
from connect.utils.logging import log_error, log_info


def on_document_event(doc, method: str):
	"""Wildcard doc_events handler.

	Called for every document event. Performs fast early exit checks,
	then enqueues background jobs for matching rules.
	"""
	try:
		settings = frappe.get_single("Fineract Kafka Settings")
		if not settings.enabled:
			return

		# Fast early exit: check if this DocType has any active rules
		active_doctypes = settings.get_active_doctypes()
		if doc.doctype not in active_doctypes:
			return

		# Map Frappe method names to our event names
		event_map = {
			"after_insert": "after_insert",
			"on_update": "on_update",
			"on_submit": "on_submit",
			"on_cancel": "on_cancel",
			"on_trash": "on_trash",
		}

		event_name = event_map.get(method)

		if not event_name:
			return

		# Find matching rules
		rules = get_matching_rules(doc.doctype, event_name)
		if not rules:
			return

		for rule in rules:
			# Evaluate condition
			if rule.condition:
				try:
					result = frappe.safe_eval(rule.condition, eval_globals={"doc": doc, "frappe": frappe})
					if not result:
						continue
				except Exception as e:
					log_error(
						"Rule condition eval failed",
						f"rule={rule.rule_name}, error={e}",
						exc=e,
					)
					continue

			# Enqueue background job
			idempotency_key = generate_idempotency_key(
				doc.doctype, doc.name, event_name, rule.command_type, rule.rule_name
			)

			frappe.enqueue(
				"connect.background_jobs.produce_message.produce_fineract_command",
				queue="default",
				timeout=120,
				enqueue_after_commit=True,
				deduplicate=True,
				doctype=doc.doctype,
				docname=doc.name,
				rule_name=rule.rule_name,
				idempotency_key=idempotency_key,
			)

	except Exception as e:
		log_error(
			"Document event handler error",
			f"doctype={doc.doctype}, method={method}",
			exc=e,
		)


def get_matching_rules(doctype: str, event_name: str) -> list:
	"""Get all enabled emission rules matching a DocType and event."""
	rules = frappe.get_all(
		"Fineract Event Emission Rule",
		filters={
			"enabled": 1,
			"source_doctype": doctype,
			"document_event": event_name,
		},
		fields=[
			"name",
			"rule_name",
			"command_type",
			"command_category",
			"condition",
			"avro_schema_name",
			"topic_override",
			"tenant_id_override",
			"priority",
		],
		order_by="priority asc",
	)
	return rules


def produce_message(
	doc,
	rule_name: str,
	idempotency_key: str,
	settings=None,
):
	"""Core production logic: serialize and send a message to Kafka.

	Called from the background job after document and rule are loaded.
	"""
	if settings is None:
		settings = frappe.get_single("Fineract Kafka Settings")

	rule = frappe.get_doc("Fineract Event Emission Rule", rule_name)
	topic = rule.topic_override or settings.command_topic
	tenant_id = rule.tenant_id_override or settings.default_tenant_id

	# Idempotency check
	if check_idempotency(idempotency_key):
		log_info("Skipping duplicate", f"key={idempotency_key}, rule={rule_name}")
		return

	# Create Kafka Log (Pending)
	from connect.connect.doctype.fineract_kafka_log.fineract_kafka_log import (
		FineractKafkaLog,
	)

	log = FineractKafkaLog.log_produced(
		idempotency_key=idempotency_key,
		command_type=rule.command_type,
		topic=topic,
		tenant_id=tenant_id,
		source_doctype=doc.doctype,
		source_docname=doc.name,
		rule_name=rule_name,
	)

	try:
		# Build inner payload via field mappings
		payload_dict = build_payload(doc, rule.field_mappings)

		# Get inner Avro schema
		inner_schema = get_schema(rule.avro_schema_name, settings)

		# Serialize inner payload (raw Avro, no Confluent header)
		inner_bytes = serialize_inner_payload(inner_schema, payload_dict)

		# Build MessageV1 envelope
		envelope = {
			"id": 0,
			"source": settings.producer_source_name or "openerp-fineract",
			"type": rule.command_type,
			"category": rule.command_category,
			"createdAt": datetime.now().isoformat(),
			"businessDate": date.today().isoformat(),
			"tenantId": tenant_id,
			"idempotencyKey": idempotency_key,
			"dataschema": rule.avro_schema_name,
			"data": inner_bytes,
		}

		# Serialize envelope with Confluent wire format
		sr_config = settings.get_schema_registry_config()
		sr_service = SchemaRegistryService(sr_config)
		avro_serializer = create_avro_serializer(
			sr_service.client,
			auto_register=bool(settings.auto_register_schemas),
		)
		serialized_value = serialize_envelope(avro_serializer, envelope, topic)

		# Produce to Kafka
		producer_config = settings.get_producer_config()
		producer = KafkaProducerClient(producer_config)
		delivery = producer.produce(
			topic=topic,
			key=idempotency_key,
			value=serialized_value,
		)

		# Update log with delivery metadata
		if delivery:
			log.mark_delivered(
				partition=delivery.get("partition"),
				offset=delivery.get("offset"),
			)
			log.db_set("message_key", idempotency_key, update_modified=False)

		# Optionally log payload
		if settings.log_payload_on_success:
			log.db_set(
				"payload_json",
				json.dumps(payload_dict, default=str),
				update_modified=False,
			)

		log_info(
			"Message produced",
			f"topic={topic} command={rule.command_type} key={idempotency_key}",
		)
		frappe.db.commit()

	except Exception as e:
		import traceback

		log.mark_failed(str(e), traceback.format_exc())
		frappe.db.commit()
		raise

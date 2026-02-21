"""Custom bench commands for the connect app.

Provides:
- bench connect-consumer: Long-running Kafka consumer process
- bench connect-health: Health check for Kafka and Schema Registry
"""
import click
import frappe
from frappe.commands import get_site, pass_context


@click.command("connect-consumer")
@click.option("--site", help="Site name")
@click.option("--max-messages", default=0, type=int, help="Stop after N messages (0=unlimited)")
@pass_context
def connect_consumer(context, site=None, max_messages=0):
    """Start the Connect Kafka consumer.

    Long-running process that polls Kafka for business events
    and dispatches them to Frappe handlers.
    Managed by Supervisor in production.
    """
    site = site or get_site(context)
    click.echo(f"Starting Connect Kafka consumer for site: {site}")

    from connect.services.consumer_service import start_consumer

    start_consumer(site=site, max_messages=max_messages)


@click.command("connect-health")
@click.option("--site", help="Site name")
@pass_context
def connect_health(context, site=None):
    """Check health of Kafka and Schema Registry connections."""
    import json

    site = site or get_site(context)

    frappe.init(site=site)
    frappe.connect()

    try:
        from connect.utils.health import check_full_health

        result = check_full_health()
        click.echo(json.dumps(result, indent=2))

        if result["status"] == "ok":
            click.secho("All systems operational", fg="green")
        else:
            click.secho("Health check failed", fg="red")
            raise SystemExit(1)
    finally:
        frappe.destroy()


@click.command("connect-refresh-schemas")
@click.option("--site", help="Site name")
@pass_context
def connect_refresh_schemas(context, site=None):
    """Manually refresh the Avro schema cache from Schema Registry."""
    site = site or get_site(context)

    frappe.init(site=site)
    frappe.connect()

    try:
        from connect.services.schema_service import refresh_schema_cache

        click.echo("Refreshing schema cache...")
        refresh_schema_cache()
        click.secho("Schema cache refreshed", fg="green")
    finally:
        frappe.destroy()


commands = [
    connect_consumer,
    connect_health,
    connect_refresh_schemas,
]

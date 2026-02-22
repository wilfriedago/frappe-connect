# Connect — Kafka-Fineract Integration Bridge

Bidirectional event bridge between Frappe/ERPNext and Apache Fineract via Apache Kafka.

## Features

- **Producer:** Translates Frappe document lifecycle events into Fineract external commands
- **Consumer:** Translates Fineract business events back into Frappe document actions
- **Configuration-driven:** New commands/events added via DocTypes, not code
- **Two-tier Avro serialization:** Inner raw payload + Confluent Schema Registry envelope
- **Three-layer schema cache:** Redis → MariaDB → Schema Registry
- **Full observability:** Every message logged with status tracking and correlation

## Prerequisites

- `librdkafka` installed (`brew install librdkafka` on macOS, `apt-get install librdkafka-dev` on Debian)
- Apache Kafka + Confluent Schema Registry (see `docker-compose.kafka.yml`)
- Frappe Tweaks app installed (for Sync Job support)

## Installation

```bash
bench get-app connect
bench --site your-site install-app connect
bench --site your-site migrate
```

## Setup

1. Start Kafka infrastructure: `docker compose -f docker-compose.kafka.yml up -d`
2. Configure **Connect Settings** in Desk
3. Create **Connect Hooks** for producer
4. Create **Connect Event Handlers** for consumer
5. Start consumer: `bench connect-consumer --site your-site`


## Architecture

See [plan.md](../../plan.md) for the full architecture document.

## License

MIT

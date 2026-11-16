# Pix Fraud Streaming

Real-time fraud detection pipeline for simulated Pix transactions, built as a data engineering portfolio project.

A synthetic transaction generator, calibrated with public Pix statistics from the Central Bank of Brazil (BCB), publishes events to Redpanda. Spark Structured Streaming validates, deduplicates and scores each transaction, then writes the results to Delta Lake tables stored in MinIO. Metrics for latency and throughput are exposed in Prometheus and Grafana.

> All data in this project is synthetic. Real Pix transactions are confidential, so the generator uses public aggregated statistics from BCB to reproduce realistic volumes and distributions.

## Goals

- Process a continuous stream of Pix-like transactions with low latency
- Detect suspicious transactions in real time using stateful rules and an optional ML model
- Handle real-world streaming problems such as late events, duplicates and malformed messages
- Guarantee that reprocessing never creates duplicated records
- Measure and publish end-to-end latency and throughput

## Architecture

```
[Pix Generator (Python)]
   │  Avro + Schema Registry
   ▼
[Redpanda]  topic pix.transactions
   │
   ▼
[Spark Structured Streaming]
   ├── Avro deserialization ──fail──► topic pix.dlq
   ├── business validation ──fail──► topic pix.dlq
   ├── watermark on event_time + fraud rules
   ├── very late events ──► delta/late_events
   ▼
[Delta Lake on MinIO]
   bronze (raw) → silver (validated, deduplicated) → gold (alerts, aggregates)

[StreamingQueryListener] ──► Prometheus ──► Grafana
[OpenLineage listener]   ──► Marquez (data lineage)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Message broker | Redpanda (Kafka API compatible) |
| Schema management | Redpanda Schema Registry with Avro |
| Stream processing | Apache Spark Structured Streaming (PySpark) |
| Storage | Delta Lake on MinIO (S3 compatible) |
| Monitoring | Prometheus and Grafana |
| Data lineage | OpenLineage and Marquez |
| ML (optional) | scikit-learn and MLflow |
| Infrastructure | Docker Compose |

## Key Features

### Realistic data generation
- Volumes, ticket sizes, PF/PJ mix and geographic distribution calibrated with the [BCB Pix open data](https://dadosabertos.bcb.gov.br/dataset/pix)
- Fraud rate calibrated with the BCB MED (Special Return Mechanism) statistics
- Intentional injection of late events, duplicates and malformed messages to test the pipeline
- Every transaction carries a ground truth label used only for evaluation, never for detection

### Reliable ingestion
- Avro schemas registered with BACKWARD compatibility, including a documented schema evolution
- Dead-letter queue for messages that fail deserialization or validation, with error reason and original offset
- Idempotent sink using `foreachBatch` with Delta `MERGE` on `end_to_end_id`, so a restarted job never writes duplicates

### Event time handling
- Watermarks on `event_time` for windowed aggregations
- Late events are captured in a dedicated table and counted, instead of being silently dropped

### Real-time fraud rules
| Rule | Description |
|---|---|
| Velocity | Too many transactions from the same payer in a short window |
| Atypical amount | Amount far from the payer's own history (z-score kept in state) |
| Mule account | Recently created key receiving money from many different payers |
| Night time | High value transfers from individuals during night hours |

Rules keep state across micro-batches, so patterns spread over several batches are still detected.

### Observability
- Throughput and batch duration published on every micro-batch
- End-to-end latency percentiles (p50, p95, p99) measured from `event_time` to the Delta commit
- Consumer lag and dead-letter queue volume
- Versioned Grafana dashboard

## Project Structure

```
pix-fraud-streaming/
├── docker-compose.yml
├── schemas/              # Avro schemas (v1, v2)
├── generator/            # Pix transaction generator and BCB calibration script
├── streaming/            # Spark jobs (bronze, silver, gold, fraud rules, DLQ)
├── ml/                   # Optional model training
├── monitoring/           # Prometheus config and Grafana dashboards
├── tests/                # Idempotency, schema and rule tests
└── README.md
```

## Getting Started

### Prerequisites
- Docker and Docker Compose v2
- At least 8 GB of RAM available for Docker

### Run

```bash
git clone https://github.com/<your-user>/pix-fraud-streaming.git
cd pix-fraud-streaming
docker compose up -d
```

### Services

| Service | URL |
|---|---|
| Redpanda Console | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Spark UI | http://localhost:4040 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Marquez | http://localhost:3001 |

## Results

Results will be published here after load testing.

| Metric | Value |
|---|---|
| Sustained throughput | TBD |
| End-to-end latency p50 / p95 / p99 | TBD |
| Late events captured | TBD |
| Messages routed to DLQ | TBD |
| Precision and recall per fraud rule | TBD |

## Roadmap

- [ ] Docker Compose infrastructure
- [ ] Pix generator calibrated with BCB data
- [ ] Avro schemas and Schema Registry
- [ ] Bronze, silver and gold layers with idempotent sink
- [ ] Dead-letter queue and reprocessing script
- [ ] Watermarks and late event tracking
- [ ] Stateful fraud rules
- [ ] Prometheus metrics and Grafana dashboard
- [ ] Data lineage with OpenLineage
- [ ] Optional ML model
- [ ] Load test and published results

## References

- [BCB Pix Statistics (open data)](https://dadosabertos.bcb.gov.br/dataset/pix)
- [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake documentation](https://docs.delta.io/)
- [OpenLineage](https://openlineage.io/)

## Author

**Ricardo Júnio Santos Marinho**
Data Engineering Intern | Software Engineering student at Universidade Católica de Brasília
Portfolio: https://junio-marinho.is-a.dev/

# Week 7 — Streaming

Week 7 of the [DataTalks.Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/07-streaming).

Real-time data streaming using **Redpanda** (Kafka-compatible broker) and **Apache Flink** via PyFlink, with NYC Green Taxi trip data. Everything runs locally with Docker Compose.

## Stack

- **Redpanda** — single-node Kafka-compatible broker, topic `green-trips`
- **Apache Flink** — PyFlink Table API jobs (JobManager + TaskManager)
- **PostgreSQL** — sink for windowed aggregation results

## Getting Started

```bash
cd workshop
docker compose build
docker compose up -d

# create the topic
docker exec -it workshop-redpanda-1 rpk topic create green-trips

# produce events (synthetic realtime rides)
python src/producers/producer_realtime.py
# or bulk-load from parquet via notebooks/homework.ipynb
```

Ports: Redpanda `9092`, Flink Web UI `8081`, PostgreSQL `5432`.

## Flink Jobs (`workshop/src/job/`)

| Job | Window | Output table |
|---|---|---|
| `green_pass_through_job.py` | none | — |
| `green_aggregation_job.py` | Tumbling | `green_aggregated_events` (trip count + revenue per zone) |
| `green_5min_window_job.py` | Tumbling 5 min | `green_trips_5min` (trip count per zone) |
| `green_hourly_tips_job.py` | Tumbling 1 hour | `green_trips_hourly_tips` (total tips) |
| `green_session_window_job.py` | Session 5-min gap | `green_trips_session` (trip count per zone) |

Event time is derived from `lpep_pickup_datetime` with a 5-second watermark.

## Kafka Examples — Java (`kafka_examples/`)

Gradle project covering JSON producer/consumer, Kafka Streams (joins, windowed aggregations), Avro + Schema Registry, and custom SerDes.

```bash
cd kafka_examples && ./gradlew build
```


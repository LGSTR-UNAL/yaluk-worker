# yaluk-worker

A Kafka worker that runs YALUK lightning simulations on demand.

YALUK is an [ATP] external model that computes the voltages that lightning induces on transmission and distribution lines. The worker takes simulation requests from a Kafka topic and runs ATP (`tpbig`) with the YALUK model. It then publishes the peak values of each measured signal to another Kafka topic and records the result in PostgreSQL.

> **Note:** ATP and libyaluk are not included. You must supply your own copy of [ATP] and of libyaluk (from the [LGSTR-UNAL/libyaluk][libyaluk] repository). See [Requirements](#requirements).

## Pipeline

```text
              ┌───────────────────── S3 ─────────────────────┐
              │ requests bucket     resources bucket          │
              │ (case inputs)       (surge arrester models)   │
              └──────┬──────────────────────┬─────────────────┘
                     │ download             │ download
                     ▼                      ▼
 REQUEST_TOPIC ──► yaluk-worker ──► tpbig (ATP + YALUK) ──► case.pl4
   (Kafka)               │                                     │
                         │                                     ▼
                         │                  peak value per signal ──► RESULTS_TOPIC (Kafka)
                         │
                         ├──► PostgreSQL: simulation status = completed | failed
                         └──► S3 results bucket: case.lis (only if the simulation fails)
```

For each message on `REQUEST_TOPIC`, the worker does the following:

1. **Parses the request.** The worker discards messages that are not valid JSON or that have no `payload.id`. It logs each discarded message.
2. **Prepares a working directory** at `$WORK_DIR/<case>_<Case>` and downloads the inputs:

   | Source | Object key | Saved as |
   | --- | --- | --- |
   | `S3_REQUESTS_BUCKET` | `<case>.zip` | extracted into the working directory |
   | `S3_REQUESTS_BUCKET` | `<case>_<Case>.atp` | `case.atp` |
   | `S3_REQUESTS_BUCKET` | `<case>/corr_<Case, 5 digits>.txt` | `CaseFiles/corr_00001.txt` |
   | `S3_RESOURCES_BUCKET` | `<S3_SURGE_ARRESTERS_PREFIX>/<name>`, for each surge arrester in the request | `<name>` |
   | `YALUK_PATH` (local) | `startup` | `startup` |

3. **Runs the simulation:** `$TPBIG_PATH DISK case.atp s -r`, inside the working directory.
4. **Publishes the results.** The worker reads `CaseFiles/case.pl4`. For each signal it publishes one message to `RESULTS_TOPIC` with the peak absolute value and the time at which it happens.
5. **Updates PostgreSQL.** The worker sets `status` to `completed` or `failed` in `SIMULATION_TABLE`. If the simulation fails, it also uploads `case.lis` (the ATP listing) to `S3_RESULTS_BUCKET` as `<case>_<Case>.lis`, so you can see why it failed.
6. **Commits the Kafka offset** and deletes the working directory.

### Delivery guarantees

- Offsets are committed by hand, and only after a message has been processed. If the worker stops in the middle of a simulation, the message is processed again.
- If the simulation fails, the worker marks it as `failed` and commits the offset, so the request is not retried.
- If the PostgreSQL update itself fails, the worker does not commit the offset, and the message is processed again.
- `MAX_POLL_INTERVAL_MS` must be longer than your longest simulation. If it is not, Kafka assumes the worker is dead and hands the message to another consumer.

## Message formats

### Request (`REQUEST_TOPIC`)

```json
{
  "payload": {
    "id": "3f6c0b9e-2d4a-4c1e-9a57-0c1d2e3f4a5b",
    "case": "LN450",
    "Case": 12,
    "surge_arresters": ["arrester_a.lib", "arrester_b.lib"],
    "delete_tmp": true
  }
}
```

| Field | Description |
| --- | --- |
| `id` | Simulation id. It must match an existing row in `SIMULATION_TABLE`. |
| `case` | Case name, used to build the S3 object keys. Allowed characters: letters, digits, `_`, `.` and `-`. |
| `Case` | Case number, an integer. It selects `corr_<Case>.txt` and `<case>_<Case>.atp`. |
| `surge_arresters` | A list of surge arrester file names, or a string that contains a JSON list. Optional. |
| `delete_tmp` | Whether to delete the `.tmp`, `.bin` and `.dbg` files that ATP creates. Defaults to `true`. |

### Result (`RESULTS_TOPIC`)

The worker publishes one message per signal found in `case.pl4`. Each message uses the Kafka Connect JSON format, with the schema embedded:

```json
{
  "schema": { "type": "struct", "name": "results", "fields": ["..."] },
  "payload": {
    "simulation_id": "3f6c0b9e-2d4a-4c1e-9a57-0c1d2e3f4a5b",
    "t": 0.0000125,
    "measurement": "<signal name>",
    "value": 152340.5
  }
}
```

### Database

The worker does not create rows. It only updates the row whose `id` matches the request, so a row must already exist. The table needs at least these columns:

| Column | Written when |
| --- | --- |
| `id` | Used to find the row. |
| `status` | Always: set to `completed` or `failed`. |
| `duration` | The simulation completed. Value in seconds. |
| `error_message` | The simulation failed. |
| `completed_at` | Always: set to `NOW()`. |

## Requirements

### Infrastructure

- **Kafka:** a broker with a request topic and a results topic. [docker-compose.yaml](docker-compose.yaml) starts a single broker for local development.
- **S3-compatible object storage** (AWS S3, MinIO, etc.) with three buckets: requests, resources and results.
- **PostgreSQL**, with the simulation table described above.

### Runtime

- **Your own copy of ATP and of libyaluk.** This repository does not include or build either one, so you must supply both:
  - **ATP:** get your own licensed copy of [ATP] (the `tpbig` sources and libraries).
  - **libyaluk:** get the YALUK model library from the [LGSTR-UNAL/libyaluk][libyaluk] repository.

  Build `tpbig` with libyaluk linked in, following the instructions in [LGSTR-UNAL/libyaluk][libyaluk]. You need the resulting `tpbig` executable and its `startup` file. Copy them into the image or mount them into the container, and point `TPBIG_PATH` and `YALUK_PATH` at them.
- **Python 3.10 or later.** The Docker image uses Python 3.14.
- **librdkafka**, which `confluent-kafka` needs. On Debian or Ubuntu, install `librdkafka-dev`.
- The Python packages listed in [requirements.txt](requirements.txt).

## Configuration

All settings come from environment variables. [.env.example](.env.example) has a template. If a required variable is missing, the worker lists it and exits without starting.

### Kafka

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `KAFKA_HOST` | yes | | Bootstrap servers, e.g. `kafka:9092` |
| `CONSUMER_GROUP_ID` | yes | | Consumer group id. Workers in the same group share the requests between them. |
| `REQUEST_TOPIC` | yes | | Topic the worker reads simulation requests from |
| `RESULTS_TOPIC` | yes | | Topic the worker publishes results to |
| `KAFKA_AUTO_OFFSET_RESET` | no | `earliest` | Where a new consumer group starts reading |
| `MAX_POLL_INTERVAL_MS` | no | `3600000` | Longest time a single simulation may take, in milliseconds |
| `PRODUCER_FLUSH_TIMEOUT` | no | `60` | Seconds to wait for results to be delivered. If some results are still undelivered after that, the simulation is marked as failed. |

### S3

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `S3_ENDPOINT_URL` | no | AWS | Endpoint of an S3-compatible service such as MinIO |
| `S3_ACCESS_KEY_ID` | no | | Access key. If empty, the default AWS credential chain is used. |
| `S3_SECRET_ACCESS_KEY` | no | | Secret key |
| `S3_REQUESTS_BUCKET` | no | `yaluk-requests` | Bucket with the case inputs |
| `S3_RESOURCES_BUCKET` | no | `yaluk-resources` | Bucket with shared resources such as surge arrester models |
| `S3_RESULTS_BUCKET` | no | `yaluk-results` | Bucket where `case.lis` is uploaded when a simulation fails |
| `S3_SURGE_ARRESTERS_PREFIX` | no | `surge-arresters` | Key prefix of the surge arrester files in the resources bucket |

### PostgreSQL

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `POSTGRES_HOST` | yes | | Database host |
| `POSTGRES_PORT` | no | `5432` | Database port |
| `POSTGRES_DB` | yes | | Database name |
| `POSTGRES_USER` | yes | | Database user |
| `POSTGRES_PASSWORD` | yes | | Database password |
| `SIMULATION_TABLE` | no | `streaming.simulation` | Table to update, written as `schema.table` |
| `POSTGRES_POOL_MAX_SIZE` | no | `2` | Maximum number of connections in the pool |
| `POSTGRES_POOL_TIMEOUT` | no | `30` | Seconds to wait for the database at startup |

### ATP / YALUK and the worker

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `TPBIG_PATH` | yes | | Path to the `tpbig` executable |
| `YALUK_PATH` | yes | | Directory that contains the ATP `startup` file |
| `WORK_DIR` | no | `/tmp` | Parent directory for the per-simulation working directories |
| `LOG_LEVEL` | no | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR` |

## Running

### Locally

```bash
sudo apt install python3 python3-venv librdkafka-dev
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env    # then fill in the values
set -a && . ./.env && set +a
python src/main.py
```

To start a local Kafka broker, which is reachable from the host at `localhost:9093`:

```bash
docker compose up -d
```

### Docker

```bash
docker build -t yaluk-worker .
docker run --env-file .env \
  -v /path/to/libatp:/opt/yaluk/libatp:ro \
  yaluk-worker
```

The image does not include `tpbig`, so mount the directory that contains `tpbig` and `startup`. With the mount above, set `TPBIG_PATH=/opt/yaluk/libatp/tpbig` and `YALUK_PATH=/opt/yaluk/libatp`.

The worker stops cleanly on `SIGTERM` or `SIGINT`: it finishes the message in progress, then closes the Kafka consumer, the producer and the database pool.

## Project layout

```text
src/
├── main.py              # Kafka consumer loop and request processing
├── config.py            # Settings read from environment variables
├── pyyaluk/yaluk.py     # Runs tpbig for a case
└── read_PL4/
    ├── lib_readPL4.py   # Reader for ATP .pl4 files (github.com/ldemattos/readPL4)
    └── pl4_2_kafka.py   # Computes peak values and publishes them to Kafka
```

## License

[GPL-3.0](LICENSE)

## Copyright

2020, Laboratorio de Gestión de Sistemas en Tiempo Real, Facultad de Minas, Universidad Nacional de Colombia

## Contact

[![LGSTR Logo](docs/LGSTR_logo.png)](https://sites.google.com/unal.edu.co/lab-gstr/)

- Ernesto Pérez <eperezg@unal.edu.co>
- Andres Osorio <anosoriosa@unal.edu.co>

[Laboratorio de Gestión de Sistemas en Tiempo Real](https://sites.google.com/unal.edu.co/lab-gstr/) \
[Facultad de Minas](https://minas.medellin.unal.edu.co/) \
[Universidad Nacional de Colombia](https://unal.edu.co/)

[ATP]: https://www.emtp.org/index.php
[libyaluk]: https://github.com/LGSTR-UNAL/libyaluk

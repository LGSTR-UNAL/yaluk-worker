import json
import logging
import os
import re
import shutil
import signal
import sys
import zipfile
from dataclasses import dataclass
from typing import Any, TypedDict

import boto3
from config import MissingSettings, Settings
from confluent_kafka import Consumer, KafkaError, Message, Producer
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg_pool import ConnectionPool
from pyyaluk.yaluk import simulate
from read_PL4.pl4_2_kafka import produce_results

# Nombres permitidos para casos y archivos descargados: evita path traversal
# y caracteres especiales en rutas.
SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class SimulationRequest(TypedDict):
    id: str
    case: str
    Case: int
    surge_arresters: str | list[str]
    delete_tmp: bool


class InvalidRequest(Exception):
    pass


running = True


def stop(signum: int, _frame: Any) -> None:
    global running
    logging.info("Received signal %d, shutting down", signum)
    running = False


def safe_name(name: Any, field: str) -> str:
    name = str(name)
    if not SAFE_NAME.match(name) or ".." in name:
        raise InvalidRequest(f"Invalid value for {field}: {name!r}")
    return name


def safe_extract(zip_path: str, destination: str) -> None:
    destination = os.path.realpath(destination)
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.namelist():
            target = os.path.realpath(os.path.join(destination, member))
            if os.path.commonpath([destination, target]) != destination:
                raise InvalidRequest(f"Zip entry outside destination: {member!r}")
        archive.extractall(destination)


def parse_request(message: Message) -> SimulationRequest:
    try:
        value = json.loads(message.value())["payload"]
    except (TypeError, ValueError, KeyError) as e:
        raise InvalidRequest(f"Malformed message: {e}") from e
    if not isinstance(value, dict) or not value.get("id"):
        raise InvalidRequest("Message payload is empty or has no id")
    return value


class Database:
    def __init__(self, settings: Settings) -> None:
        # "schema.table" -> "schema"."table"
        self.table = sql.Identifier(*settings.simulation_table.split("."))
        self.pool = ConnectionPool(
            conninfo=make_conninfo(
                host=settings.postgres_host,
                port=settings.postgres_port,
                dbname=settings.postgres_db,
                user=settings.postgres_user,
                password=settings.postgres_password,
            ),
            kwargs={"autocommit": True},
            min_size=1,
            max_size=settings.postgres_pool_max_size,
            # Verifica la conexión antes de entregarla y descarta las rotas.
            check=ConnectionPool.check_connection,
            open=True,
        )
        # Falla al arrancar si la base de datos no está disponible.
        self.pool.wait(timeout=settings.postgres_pool_timeout)

    def execute(self, query: sql.Composable, params: tuple[Any, ...]) -> None:
        with self.pool.connection() as conn:
            conn.execute(query, params)

    def mark_completed(self, simulation_id: str, duration: float) -> None:
        self.execute(
            sql.SQL(
                """
                UPDATE {}
                SET status = 'completed',
                    duration = %s,
                    completed_at = NOW()
                WHERE id = %s
                """
            ).format(self.table),
            (duration, simulation_id),
        )

    def mark_failed(self, simulation_id: str, error_message: str) -> None:
        self.execute(
            sql.SQL(
                """
                UPDATE {}
                SET status = 'failed',
                    error_message = %s,
                    completed_at = NOW()
                WHERE id = %s
                """
            ).format(self.table),
            (error_message, simulation_id),
        )

    def close(self) -> None:
        self.pool.close()


@dataclass
class Worker:
    settings: Settings
    s3: Any
    producer: Producer
    db: Database


def upload_lis(worker: Worker, work_dir: str, name: str) -> None:
    lis_path = os.path.join(work_dir, "case.lis")
    if not os.path.exists(lis_path):
        logging.warning("No case.lis found in %s", work_dir)
        return
    with open(lis_path, "rb") as f:
        results = f.read()
    logging.info("[PHASE] case.lis leido: %d bytes", len(results))
    worker.s3.put_object(
        Bucket=worker.settings.results_bucket, Key=f"{name}.lis", Body=results
    )


def run_simulation(
    worker: Worker, value: SimulationRequest, work_dir: str, name: str
) -> float:
    settings = worker.settings
    s3 = worker.s3
    case = safe_name(value["case"], "case")
    try:
        case_number = int(value["Case"])
    except (KeyError, TypeError, ValueError) as e:
        raise InvalidRequest(f"Invalid value for Case: {e}") from e

    surge_arresters = value.get("surge_arresters", [])
    if isinstance(surge_arresters, str):
        surge_arresters = json.loads(surge_arresters)
    arresters = [safe_name(a, "surge_arresters") for a in surge_arresters]

    os.makedirs(work_dir, exist_ok=True)
    zip_path = os.path.join(work_dir, "inputs.zip")
    s3.download_file(settings.requests_bucket, f"{case}.zip", zip_path)
    safe_extract(zip_path, work_dir)

    for arrester in arresters:
        s3.download_file(
            settings.resources_bucket,
            f"{settings.surge_arresters_prefix}/{arrester}",
            os.path.join(work_dir, arrester),
        )

    s3.download_file(
        settings.requests_bucket, f"{name}.atp", os.path.join(work_dir, "case.atp")
    )
    shutil.copy(
        os.path.join(settings.yaluk_path, "startup"),
        os.path.join(work_dir, "startup"),
    )
    os.makedirs(os.path.join(work_dir, "CaseFiles"), exist_ok=True)
    s3.download_file(
        settings.requests_bucket,
        f"{case}/corr_{case_number:05d}.txt",
        # YALUK siempre lee la corriente del caso desde corr_00001.txt
        os.path.join(work_dir, "CaseFiles", "corr_00001.txt"),
    )

    duration, error, returncode = simulate(
        case,
        bool(value.get("delete_tmp", True)),
        work_dir,
        tpbig=settings.tpbig_path,
    )

    if not os.path.exists(os.path.join(work_dir, "CaseFiles", "case.pl4")):
        stderr = error.decode("utf-8", "replace").strip() if error else ""
        raise RuntimeError(
            f"ATP did not produce case.pl4 (exit code {returncode}). {stderr}".strip()
        )
    return duration


def process_message(worker: Worker, message: Message) -> None:
    db = worker.db
    try:
        value = parse_request(message)
    except InvalidRequest as e:
        logging.error("Discarding message: %s", e)
        return

    simulation_id = value["id"]
    logging.info(value)
    try:
        name = f"{safe_name(value.get('case'), 'case')}_{safe_name(value.get('Case'), 'Case')}"
    except InvalidRequest as e:
        logging.error("Simulation %s failed: %s", simulation_id, e)
        db.mark_failed(simulation_id, str(e))
        return
    work_dir = os.path.join(worker.settings.work_dir, name)

    try:
        duration = run_simulation(worker, value, work_dir, name)
        produce_results(
            os.path.join(work_dir, "CaseFiles", "case.pl4"),
            simulation_id,
            worker.producer,
            worker.settings.results_topic,
            worker.settings.producer_flush_timeout,
        )
    except Exception as e:
        logging.exception("Simulation %s failed: %s", simulation_id, e)
        try:
            upload_lis(worker, work_dir, name)
        except Exception:
            logging.exception("Error uploading case.lis for %s", simulation_id)
        db.mark_failed(simulation_id, str(e))
    else:
        db.mark_completed(simulation_id, duration)
        logging.info(f"Simulation {simulation_id} completed successfully.")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def main() -> None:
    try:
        settings = Settings.from_env()
    except (MissingSettings, ValueError) as e:
        logging.basicConfig()
        logging.critical("Invalid configuration: %s", e)
        sys.exit(1)

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
        ],
    )

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer_logger = logging.getLogger("confluent_kafka.consumer")
    producer_logger = logging.getLogger("confluent_kafka.producer")

    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
    )

    try:
        consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_host,
                "group.id": settings.consumer_group_id,
                "auto.offset.reset": settings.kafka_auto_offset_reset,
                # El offset se confirma solo después de procesar el mensaje.
                "enable.auto.commit": False,
                # Una simulación puede tardar más que el límite por defecto (5 min).
                "max.poll.interval.ms": settings.max_poll_interval_ms,
            },
            logger=consumer_logger,
        )
        logging.info(f"Kafka consumer created for {settings.kafka_host}")
    except Exception as e:
        logging.exception(f"Kafka consumer creation failed: {e}")
        sys.exit(1)

    try:
        consumer.subscribe([settings.request_topic])
        logging.info(f"Subscribed correctly to {settings.request_topic}")
    except Exception as e:
        logging.exception(f"Subscription to topic failed: {e}")
        sys.exit(1)

    try:
        producer: Producer = Producer(
            {
                "bootstrap.servers": settings.kafka_host,
                "queue.buffering.max.messages": 100000,
                "queue.buffering.max.kbytes": 262144,
                "linger.ms": 100,
                "compression.type": "lz4",
            },
            logger=producer_logger,
        )
        logging.info(f"Kafka producer created for {settings.kafka_host}")
    except Exception as e:
        logging.exception(f"Kafka producer creation failed: {e}")
        sys.exit(1)

    try:
        db = Database(settings)
        logging.info(
            f"Connected to PostgreSQL database {settings.postgres_db} at {settings.postgres_host}:{settings.postgres_port}"
        )
    except Exception as e:
        logging.exception(f"PostgreSQL connection failed: {e}")
        sys.exit(1)

    worker = Worker(settings=settings, s3=s3, producer=producer, db=db)

    try:
        while running:
            message = consumer.poll(10)
            producer.poll(0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logging.error("Kafka consumer error: %s", message.error())
                continue

            try:
                process_message(worker, message)
            except Exception:
                # Solo llega aquí si falla el UPDATE en PostgreSQL: no se
                # confirma el offset para que el mensaje se reprocese.
                logging.exception("Error processing message, it will be retried")
                continue
            consumer.commit(message=message, asynchronous=False)
    finally:
        logging.info("Closing Kafka and PostgreSQL connections")
        consumer.close()
        producer.flush(10)
        db.close()


if __name__ == "__main__":
    main()

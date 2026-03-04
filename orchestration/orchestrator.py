import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

import mlflow
from mlflow.tracking import MlflowClient


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def wait_for_tcp(host: str, port: int, timeout_sec: int = 180) -> None:
    start = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=3):
                return
        except OSError:
            if time.time() - start > timeout_sec:
                raise RuntimeError(f"Timed out waiting for {host}:{port}")
            time.sleep(2)


def run(cmd: list[str], cwd: str | None = None) -> None:
    print(f"\n$ {' '.join(cmd)}", flush=True)
    p = subprocess.run(cmd, cwd=cwd, stdout=sys.stdout, stderr=sys.stderr)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {p.returncode}: {' '.join(cmd)}")


def feast_apply(feast_repo: str) -> None:
    run(["feast", "apply"], cwd=feast_repo)


def feast_materialize_incremental(feast_repo: str, lookback_minutes: int) -> None:
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=lookback_minutes)
    fmt = "%Y-%m-%dT%H:%M:%S"

    run(
        [
            "feast",
            "materialize-incremental",
            start.strftime(fmt),
            end.strftime(fmt),
        ],
        cwd=feast_repo,
    )


def spark_train(project_dir: str) -> None:
    """
    Submit the Spark training job from inside the spark-master container.
    Requires /var/run/docker.sock mounted into the orchestrator container.
    """
    run(
        [
            "bash",
            "-lc",
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            f"{project_dir}/spark_sparkml.py",
        ]
    )


def promote_latest_to_production(model_name: str, tracking_uri: str) -> None:
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient(tracking_uri=tracking_uri)

    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        raise RuntimeError(f"No model versions found for model '{model_name}'")

    latest = max(versions, key=lambda v: int(v.version))

    client.transition_model_version_stage(
        name=model_name,
        version=latest.version,
        stage="Production",
        archive_existing_versions=True,
    )
    print(f"Promoted {model_name} v{latest.version} to Production", flush=True)


def main() -> None:
    feast_repo = env("FEAST_REPO", "/opt/project")
    tracking_uri = env("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    model_name = env("MODEL_NAME", "claims-approval-model")

    lookback = int(env("MATERIALIZE_LOOKBACK_MINUTES", "120"))
    materialize_every = int(env("MATERIALIZE_EVERY_MINUTES", "60"))

    train_hour = int(env("TRAIN_AT_UTC_HOUR", "2"))
    train_minute = int(env("TRAIN_AT_UTC_MINUTE", "0"))

    # In this setup, project_dir is the same mount as FEAST_REPO
    project_dir = feast_repo

    # Basic dependency waits (ports) - match your compose service names
    wait_for_tcp("redis", 6379, 240)
    wait_for_tcp("mlflow", 5000, 240)
    wait_for_tcp("kafka", 9092, 240)
    wait_for_tcp("spark-master", 7077, 240)

    # One-time init sequence
    print("\n=== INIT: feast apply + initial materialize ===", flush=True)
    feast_apply(feast_repo)
    feast_materialize_incremental(feast_repo, lookback)

    scheduler = BlockingScheduler(timezone=timezone.utc)

    # Periodic refresh into Redis online store
    scheduler.add_job(
        func=lambda: feast_materialize_incremental(feast_repo, lookback),
        trigger=IntervalTrigger(minutes=materialize_every),
        id="feast_materialize_incremental",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # Nightly training + promote
    def train_and_promote():
        print("\n=== TRAIN: spark submit ===", flush=True)
        spark_train(project_dir)
        print("\n=== PROMOTE: latest -> Production ===", flush=True)
        promote_latest_to_production(model_name, tracking_uri)

    scheduler.add_job(
        func=train_and_promote,
        trigger=CronTrigger(hour=train_hour, minute=train_minute),
        id="train_and_promote",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=1800,
    )

    print("\n=== Orchestrator running (UTC scheduling) ===", flush=True)
    scheduler.start()


if __name__ == "__main__":
    main()
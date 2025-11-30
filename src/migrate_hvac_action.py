#!/usr/bin/env python3
"""
Migrate hvac_action_str from InfluxDB to VictoriaMetrics.

Converts string values like "heating", "idle" to the numeric format used by
the Home Assistant Prometheus exporter:
  homeassistant_climate_action{action="heating", ...} = 1

Usage:
  python migrate_hvac_action.py --dry-run
  python migrate_hvac_action.py --start-date 2024-01-01 --end-date 2025-11-30
"""

import argparse
import logging
import os
from datetime import datetime, date, timedelta, timezone
from typing import Iterator, List, Optional, Dict, Any
from dataclasses import dataclass

from influxdb_client import InfluxDBClient
from vm_writer import VMWriter, VMDataPoint

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class HvacActionPoint:
    """Represents an hvac_action data point."""
    timestamp: datetime
    entity_id: str
    friendly_name: str
    action: str  # "heating", "idle", "cooling", etc.


def parse_args():
    parser = argparse.ArgumentParser(
        description="Migrate hvac_action_str from InfluxDB to VictoriaMetrics"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate without writing data"
    )

    parser.add_argument(
        "--influx-url",
        default=os.environ.get("INFLUX_URL", "http://influxdb-influxdb2.node-red.svc.cluster.local"),
        help="InfluxDB server URL"
    )

    parser.add_argument(
        "--influx-token",
        default=os.environ.get("INFLUX_TOKEN", ""),
        help="InfluxDB authentication token"
    )

    parser.add_argument(
        "--influx-org",
        default="influxdata",
        help="InfluxDB organization"
    )

    parser.add_argument(
        "--influx-bucket",
        default="home-assistant",
        help="InfluxDB bucket"
    )

    parser.add_argument(
        "--vm-url",
        default=os.environ.get("VM_URL", "http://victoria-metrics-victoria-metrics-single-server.victoria-metrics.svc.cluster.local:8428"),
        help="VictoriaMetrics server URL"
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default="2024-01-01",
        help="Start date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--end-date",
        type=str,
        default="2025-11-30",
        help="End date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for writing"
    )

    return parser.parse_args()


def query_hvac_action(
    client: InfluxDBClient,
    bucket: str,
    start: datetime,
    end: datetime
) -> Iterator[HvacActionPoint]:
    """Query hvac_action_str from InfluxDB."""

    query_api = client.query_api()

    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    query = f'''
    from(bucket: "{bucket}")
      |> range(start: {start_str}, stop: {end_str})
      |> filter(fn: (r) => r["domain"] == "climate")
      |> filter(fn: (r) => r._field == "hvac_action_str")
    '''

    tables = query_api.query(query)

    for table in tables:
        for record in table.records:
            action = record.get_value()

            # Skip empty or invalid actions
            if not action or not isinstance(action, str):
                continue

            entity_id = record.values.get("entity_id", "unknown")
            friendly_name = record.values.get("friendly_name", entity_id)

            yield HvacActionPoint(
                timestamp=record.get_time(),
                entity_id=entity_id,
                friendly_name=friendly_name,
                action=action.lower()  # Normalize to lowercase
            )


# All possible HVAC actions (from Home Assistant)
ALL_HVAC_ACTIONS = ["heating", "idle", "cooling", "off", "drying", "fan", "preheating", "defrosting"]


def build_vm_datapoints(point: HvacActionPoint) -> List[VMDataPoint]:
    """
    Build VMDataPoints from HvacActionPoint.

    Creates one data point for each possible action:
    - Active action gets value=1
    - All other actions get value=0

    This matches the format used by the Home Assistant Prometheus exporter.
    """
    timestamp_ms = int(point.timestamp.timestamp() * 1000)
    datapoints = []

    for action in ALL_HVAC_ACTIONS:
        labels = {
            "entity": f"climate.{point.entity_id}",
            "domain": "climate",
            "friendly_name": point.friendly_name,
            "action": action,
            "job": "influxdb-migration",
            "instance": "influxdb-migration"
        }

        # Value is 1 if this is the active action, 0 otherwise
        value = 1.0 if action == point.action else 0.0

        datapoints.append(VMDataPoint(
            metric_name="homeassistant_climate_action",
            labels=labels,
            value=value,
            timestamp_ms=timestamp_ms
        ))

    return datapoints


def generate_date_range(start: date, end: date) -> List[date]:
    """Generate list of dates from start to end (inclusive)."""
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def main():
    args = parse_args()

    logger.info("=" * 80)
    logger.info("HVAC Action Migration: InfluxDB -> VictoriaMetrics")
    logger.info("=" * 80)

    if args.dry_run:
        logger.info("MODE: DRY-RUN (no data will be written)")
    else:
        logger.info("MODE: PRODUCTION")

    # Parse dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    logger.info(f"Date range: {start_date} to {end_date}")

    # Connect to InfluxDB
    client = InfluxDBClient(
        url=args.influx_url,
        token=args.influx_token,
        org=args.influx_org,
        timeout=300_000
    )
    logger.info(f"Connected to InfluxDB: {args.influx_url}")

    # Initialize VM writer
    vm_writer = VMWriter(
        url=args.vm_url,
        dry_run=args.dry_run,
        batch_size=args.batch_size
    )

    # Process each day
    total_records = 0
    total_batches = 0
    dates = generate_date_range(start_date, end_date)

    for i, day in enumerate(dates):
        logger.info(f"[{i+1}/{len(dates)}] Processing {day}...")

        start_dt = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
        end_dt = start_dt + timedelta(days=1)

        batch: List[VMDataPoint] = []
        day_records = 0

        for point in query_hvac_action(client, args.influx_bucket, start_dt, end_dt):
            vm_points = build_vm_datapoints(point)  # Returns 8 points (one per action)
            batch.extend(vm_points)
            day_records += 1

            if len(batch) >= args.batch_size:
                vm_writer.write_batch(batch)
                total_batches += 1
                batch = []

        # Write remaining
        if batch:
            vm_writer.write_batch(batch)
            total_batches += 1

        total_records += day_records
        if day_records > 0:
            logger.info(f"  {day}: {day_records} records")

    client.close()

    logger.info("=" * 80)
    logger.info("MIGRATION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total records: {total_records:,}")
    logger.info(f"Total batches: {total_batches}")

    return 0


if __name__ == "__main__":
    exit(main())

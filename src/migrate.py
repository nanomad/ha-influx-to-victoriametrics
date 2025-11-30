#!/usr/bin/env python3
"""
Main Migration Orchestrator for InfluxDB to VictoriaMetrics

Coordinates the migration of Home Assistant time-series data from InfluxDB to VictoriaMetrics
with support for resumable progress, dry-run validation, and error recovery.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, date, timedelta, timezone
from typing import List, Tuple

from influx_reader import InfluxDBReader, InfluxDataPoint
from vm_writer import VMWriter, VMDataPoint
from progress import ProgressTracker, MigrationProgress
from mapping import get_vm_metric_name_strict, build_vm_labels, load_schema, is_ignored


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Migrate InfluxDB to VictoriaMetrics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry-run to validate mappings (ALWAYS run this first!)
  python migrate.py --dry-run

  # Perform actual migration
  python migrate.py

  # Reset progress and start fresh
  python migrate.py --reset

  # Migrate only climate domain with extended fields (current_temperature, temperature)
  python migrate.py --domains climate --extended-fields --dry-run

  # Migrate climate, cover, and light with extended fields
  python migrate.py --domains climate,cover,light --extended-fields

  # Override connection details
  python migrate.py --influx-url http://custom:8086 --vm-url http://custom:8428
        """
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate without writing data to VictoriaMetrics"
    )

    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset progress and start fresh (backs up old state file)"
    )

    parser.add_argument(
        "--influx-url",
        default=os.environ.get("INFLUX_URL", "http://influxdb-influxdb2.node-red.svc.cluster.local"),
        help="InfluxDB server URL (default: %(default)s)"
    )

    parser.add_argument(
        "--influx-token",
        default=os.environ.get("INFLUX_TOKEN", ""),
        help="InfluxDB authentication token (or set INFLUX_TOKEN env var)"
    )

    parser.add_argument(
        "--influx-org",
        default="influxdata",
        help="InfluxDB organization (default: %(default)s)"
    )

    parser.add_argument(
        "--influx-bucket",
        default="home-assistant",
        help="InfluxDB bucket to migrate (default: %(default)s)"
    )

    parser.add_argument(
        "--vm-url",
        default=os.environ.get("VM_URL", "http://victoria-metrics-victoria-metrics-single-server.victoria-metrics.svc.cluster.local:8428"),
        help="VictoriaMetrics server URL (default: %(default)s)"
    )

    parser.add_argument(
        "--state-dir",
        default="../state",
        help="Directory for progress state file (default: %(default)s)"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Number of records per batch (default: %(default)s)"
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default="2025-05-01",
        help="Start date for migration (YYYY-MM-DD, default: %(default)s)"
    )

    parser.add_argument(
        "--end-date",
        type=str,
        default="2025-11-28",
        help="End date for migration (YYYY-MM-DD, default: %(default)s)"
    )

    parser.add_argument(
        "--domains",
        type=str,
        default=None,
        help="Comma-separated list of domains to migrate (e.g., 'climate,cover,light'). Default: all domains"
    )

    parser.add_argument(
        "--extended-fields",
        action="store_true",
        help="Enable extended field migration for climate (current_temperature, temperature), cover (current_position), light (brightness)"
    )

    return parser.parse_args()


def generate_date_range(start: date, end: date) -> List[date]:
    """
    Generate list of dates from start to end (inclusive).

    Args:
        start: Start date
        end: End date

    Returns:
        List of dates
    """
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def perform_dry_run_validation(
    influx: InfluxDBReader,
    start_date: date,
    end_date: date
) -> Tuple[int, int, int, List[str]]:
    """
    Validate all mappings in dry-run mode.

    Args:
        influx: InfluxDB reader
        start_date: Start date to validate
        end_date: End date to validate

    Returns:
        Tuple of (total_records, valid_records, skipped_records, error_messages)
    """
    logger.info("=== DRY-RUN VALIDATION MODE ===")
    logger.info("Validating all InfluxDB records against known VictoriaMetrics metrics")

    total_records = 0
    valid_records = 0
    skipped_records = 0
    errors = []

    # Track unique unmapped and skipped combinations
    unmapped_combinations = set()
    skipped_combinations = set()

    for current_date in generate_date_range(start_date, end_date):
        logger.info(f"Validating {current_date}...")

        day_records = 0
        day_skipped = 0
        for point in influx.query_day(current_date):
            total_records += 1
            day_records += 1

            # Try to map to VM metric
            try:
                metric_name = get_vm_metric_name_strict(
                    point.domain,
                    point.measurement,
                    point.entity_id,
                    field=point.field
                )

                # None means the record is ignored/skipped
                if metric_name is None:
                    skipped_records += 1
                    day_skipped += 1
                    combination_key = (point.domain, point.measurement, point.field)
                    if combination_key not in skipped_combinations:
                        skipped_combinations.add(combination_key)
                        logger.info(f"  SKIPPED: domain='{point.domain}', measurement='{point.measurement}', field='{point.field}' (ignored in schema)")
                else:
                    valid_records += 1

            except ValueError as e:
                # Record unmapped combination
                combination_key = (point.domain, point.measurement, point.entity_id, point.field)

                if combination_key not in unmapped_combinations:
                    unmapped_combinations.add(combination_key)
                    error_msg = (
                        f"UNMAPPED: domain='{point.domain}', "
                        f"measurement='{point.measurement}', "
                        f"entity_id='{point.entity_id}', "
                        f"field='{point.field}'"
                    )
                    errors.append(error_msg)
                    logger.warning(error_msg)

        logger.info(f"  Validated {day_records:,} records for {current_date} ({day_skipped:,} skipped)")

    return total_records, valid_records, skipped_records, errors


def migrate_day(
    influx: InfluxDBReader,
    vm_writer: VMWriter,
    day: date,
    batch_size: int,
    dry_run: bool
) -> Tuple[int, int, int]:
    """
    Migrate a single day of data.

    Args:
        influx: InfluxDB reader
        vm_writer: VictoriaMetrics writer
        day: Date to migrate
        batch_size: Batch size for writing
        dry_run: Whether in dry-run mode

    Returns:
        Tuple of (records_migrated, batches_sent, records_skipped)
    """
    batch: List[VMDataPoint] = []
    records_count = 0
    batches_sent = 0
    skipped_count = 0

    for point in influx.query_day(day):
        # Transform to VictoriaMetrics format
        try:
            metric_name = get_vm_metric_name_strict(
                point.domain,
                point.measurement,
                point.entity_id,
                field=point.field
            )
        except ValueError as e:
            logger.error(f"Failed to map record: {e}")
            raise

        # Skip if metric is ignored
        if metric_name is None:
            skipped_count += 1
            continue

        labels = build_vm_labels(
            point.domain,
            point.entity_id,
            point.friendly_name
        )

        # Convert timestamp to milliseconds
        timestamp_ms = int(point.timestamp.timestamp() * 1000)

        vm_point = VMDataPoint(
            metric_name=metric_name,
            labels=labels,
            value=point.value,
            timestamp_ms=timestamp_ms
        )

        batch.append(vm_point)
        records_count += 1

        # Write batch when it reaches batch_size
        if len(batch) >= batch_size:
            vm_writer.write_batch(batch)
            batches_sent += 1
            batch = []

    # Write remaining records
    if batch:
        vm_writer.write_batch(batch)
        batches_sent += 1

    return records_count, batches_sent, skipped_count


def main():
    """Main migration orchestrator."""
    args = parse_args()

    logger.info("=" * 80)
    logger.info("InfluxDB to VictoriaMetrics Migration")
    logger.info("=" * 80)

    if args.dry_run:
        logger.info("MODE: DRY-RUN (validation only, no writes)")
    else:
        logger.info("MODE: PRODUCTION (will write data)")

    # Parse domains filter
    domains = None
    if args.domains:
        domains = [d.strip() for d in args.domains.split(",")]
        logger.info(f"Filtering to domains: {domains}")

    if args.extended_fields:
        logger.info("Extended fields enabled for climate, cover, light")

    # Initialize progress tracker
    tracker = ProgressTracker(args.state_dir)

    # Handle reset flag
    if args.reset:
        logger.info("Resetting progress (--reset flag specified)...")
        tracker.reset(backup=True)

    # Load existing progress
    progress = tracker.load()

    # Check existing progress status
    if progress:
        if progress.status == "completed":
            logger.info("Migration already completed!")
            logger.info(f"  Records migrated: {progress.records_migrated:,}")
            logger.info(f"  Batches sent: {progress.batches_sent}")
            logger.info("Use --reset to start fresh")
            return 0

        if progress.status == "failed":
            logger.error("Previous migration failed!")
            logger.error(f"  Errors: {progress.errors}")
            logger.error("Use --reset to start fresh or fix the errors")
            return 1

        if progress.status == "in_progress":
            logger.info("Resuming from previous migration...")
            logger.info(f"  Last migrated date: {progress.last_migrated_date}")
            logger.info(f"  Records migrated so far: {progress.records_migrated:,}")

    # Load schema mapping (fetches known metrics from VictoriaMetrics)
    try:
        logger.info("Loading schema mapping and fetching known metrics from VictoriaMetrics...")
        load_schema(vm_url=args.vm_url)
    except Exception as e:
        logger.error(f"Failed to load schema: {e}")
        return 1

    # Connect to InfluxDB
    try:
        logger.info(f"Connecting to InfluxDB: {args.influx_url}")
        influx = InfluxDBReader(
            url=args.influx_url,
            token=args.influx_token,
            org=args.influx_org,
            bucket=args.influx_bucket,
            domains=domains,
            use_extended_fields=args.extended_fields
        )
    except Exception as e:
        logger.error(f"Failed to connect to InfluxDB: {e}")
        return 1

    # Parse date range from CLI arguments
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        oldest_ts = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        newest_ts = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)
        logger.info(f"Migration date range:")
        logger.info(f"  Start: {start_date}")
        logger.info(f"  End: {end_date}")
        logger.info(f"  Days: {(end_date - start_date).days + 1}")
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        influx.close()
        return 1

    # Estimate total records (avoid slow count query)
    total_records = 55_000_000  # Approximate from previous analysis
    logger.info(f"  Estimated total records: ~{total_records:,}")

    # Create or update progress
    if not progress:
        progress = tracker.create_new(
            total_records=total_records,
            oldest=oldest_ts,
            newest=newest_ts,
            dry_run=args.dry_run
        )
        tracker.save(progress)

    # Initialize VictoriaMetrics writer
    vm_writer = VMWriter(
        url=args.vm_url,
        dry_run=args.dry_run,
        batch_size=args.batch_size
    )

    # Health check (skip in dry-run)
    if not args.dry_run:
        logger.info("Checking VictoriaMetrics health...")
        if not vm_writer.health_check():
            logger.error("VictoriaMetrics health check failed!")
            influx.close()
            vm_writer.close()
            return 1
        logger.info("VictoriaMetrics is healthy")

    # Determine date range to migrate
    start_date = oldest_ts.date()
    end_date = newest_ts.date()

    # Resume from last migrated date if available
    if progress.last_migrated_date:
        resume_date = date.fromisoformat(progress.last_migrated_date)
        start_date = resume_date + timedelta(days=1)
        logger.info(f"Resuming from {start_date} (last completed: {resume_date})")

    # If dry-run, perform validation
    if args.dry_run:
        try:
            total, valid, skipped, errors = perform_dry_run_validation(
                influx,
                start_date,
                end_date
            )

            logger.info("")
            logger.info("=" * 80)
            logger.info("DRY-RUN VALIDATION RESULTS")
            logger.info("=" * 80)
            logger.info(f"Total records validated: {total:,}")
            logger.info(f"Successfully mapped: {valid:,}")
            logger.info(f"Skipped (ignored): {skipped:,}")
            logger.info(f"Failed to map: {len(errors):,}")

            if errors:
                logger.error("")
                logger.error("VALIDATION FAILED!")
                logger.error(f"{len(errors)} unique unmapped combinations found:")
                for i, error in enumerate(errors[:20], 1):  # Show first 20
                    logger.error(f"  {i}. {error}")
                if len(errors) > 20:
                    logger.error(f"  ... and {len(errors) - 20} more")
                logger.error("")
                logger.error("Fix SCHEMA_MAPPING.yaml to include all unmapped metrics")
                logger.error("before running actual migration.")

                influx.close()
                vm_writer.close()
                return 1
            else:
                logger.info("")
                logger.info("SUCCESS! All records can be mapped to known VM metrics.")
                if skipped > 0:
                    logger.info(f"Note: {skipped:,} records will be skipped (ignored domains/measurements).")
                logger.info("You can now run the migration without --dry-run flag.")

                influx.close()
                vm_writer.close()
                return 0

        except Exception as e:
            logger.error(f"Dry-run validation failed: {e}")
            influx.close()
            vm_writer.close()

            if progress:
                tracker.mark_failed(progress, str(e))

            return 1

    # Perform actual migration
    try:
        logger.info("")
        logger.info("=" * 80)
        logger.info("STARTING MIGRATION")
        logger.info("=" * 80)

        dates_to_migrate = generate_date_range(start_date, end_date)
        total_dates = len(dates_to_migrate)

        logger.info(f"Migrating {total_dates} days: {start_date} to {end_date}")

        total_skipped = 0
        for idx, current_date in enumerate(dates_to_migrate, 1):
            logger.info(f"[{idx}/{total_dates}] Processing {current_date}...")

            try:
                records, batches, skipped = migrate_day(
                    influx,
                    vm_writer,
                    current_date,
                    args.batch_size,
                    args.dry_run
                )

                total_skipped += skipped

                # Update progress after each day
                tracker.update(progress, current_date, records, batches)

                logger.info(f"  Completed {current_date}: {records:,} records, {batches} batches, {skipped:,} skipped")

            except Exception as e:
                logger.error(f"Failed to migrate {current_date}: {e}")
                tracker.mark_failed(progress, f"Failed on {current_date}: {str(e)}")
                raise

        # Mark as completed
        tracker.mark_completed(progress)

        logger.info("")
        logger.info("=" * 80)
        logger.info("MIGRATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total records migrated: {progress.records_migrated:,}")
        logger.info(f"Total batches sent: {progress.batches_sent}")
        logger.info(f"Total records skipped: {total_skipped:,}")
        logger.info(f"Failed records: {progress.records_failed}")
        logger.info("")

        return 0

    except Exception as e:
        logger.error(f"Migration failed: {e}")

        if progress:
            tracker.mark_failed(progress, str(e))

        return 1

    finally:
        # Clean up connections
        influx.close()
        vm_writer.close()


if __name__ == "__main__":
    sys.exit(main())

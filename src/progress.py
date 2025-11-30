"""
Progress Tracking Module for InfluxDB to VictoriaMetrics Migration

Provides resumable migration capabilities by persisting progress to a JSON state file.
Supports atomic writes, backup, and comprehensive status tracking.
"""

import json
import logging
import os
import shutil
from dataclasses import dataclass, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List

logger = logging.getLogger(__name__)


@dataclass
class MigrationProgress:
    """
    Represents the current state of a migration.

    Attributes:
        started_at: When the migration started (ISO format)
        last_updated: When progress was last updated (ISO format)
        status: Current status - "not_started", "in_progress", "completed", "failed"
        total_records: Total number of records to migrate
        oldest_timestamp: Oldest data point timestamp (ISO format)
        newest_timestamp: Newest data point timestamp (ISO format)
        last_migrated_date: Last fully migrated date (YYYY-MM-DD), None if not started
        records_migrated: Total number of records migrated so far
        records_failed: Total number of records that failed
        batches_sent: Total number of batches sent to VictoriaMetrics
        errors: List of error messages encountered
        dry_run: Whether this is a dry-run migration
    """
    started_at: str
    last_updated: str
    status: str
    total_records: int
    oldest_timestamp: str
    newest_timestamp: str
    last_migrated_date: Optional[str]
    records_migrated: int
    records_failed: int
    batches_sent: int
    errors: List[str]
    dry_run: bool

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'MigrationProgress':
        """Create instance from dictionary."""
        return cls(**data)


class ProgressTracker:
    """
    Manages migration progress persistence and recovery.

    Handles atomic writes to prevent corruption, automatic backup,
    and resumption from failure points.
    """

    PROGRESS_FILENAME = "progress.json"
    BACKUP_SUFFIX = ".backup"

    def __init__(self, state_dir: str):
        """
        Initialize progress tracker.

        Args:
            state_dir: Directory where progress.json will be stored
        """
        self.state_dir = Path(state_dir)
        self.progress_file = self.state_dir / self.PROGRESS_FILENAME
        self.backup_file = self.state_dir / f"{self.PROGRESS_FILENAME}{self.BACKUP_SUFFIX}"

        # Ensure state directory exists
        self.state_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"ProgressTracker initialized: {self.state_dir}")

    def load(self) -> Optional[MigrationProgress]:
        """
        Load existing progress from file.

        Returns:
            MigrationProgress if file exists and is valid, None otherwise
        """
        if not self.progress_file.exists():
            logger.info("No existing progress file found")
            return None

        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            progress = MigrationProgress.from_dict(data)
            logger.info(f"Loaded progress: status={progress.status}, "
                       f"records_migrated={progress.records_migrated:,}, "
                       f"last_migrated_date={progress.last_migrated_date}")
            return progress

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.error(f"Failed to load progress file: {e}")
            logger.warning("Progress file is corrupted or invalid")
            return None

    def save(self, progress: MigrationProgress) -> None:
        """
        Save progress to file using atomic write.

        Writes to a temporary file first, then renames to prevent corruption.

        Args:
            progress: MigrationProgress to save
        """
        # Update last_updated timestamp
        progress.last_updated = datetime.utcnow().isoformat() + 'Z'

        # Write to temporary file first
        temp_file = self.progress_file.with_suffix('.tmp')

        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(progress.to_dict(), f, indent=2, ensure_ascii=False)

            # Atomic rename (overwrites existing file)
            temp_file.replace(self.progress_file)

            logger.debug(f"Progress saved: status={progress.status}, "
                        f"records={progress.records_migrated:,}")

        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
            # Clean up temp file if it exists
            if temp_file.exists():
                temp_file.unlink()
            raise

    def reset(self, backup: bool = True) -> None:
        """
        Reset progress, optionally backing up old file.

        Args:
            backup: If True, backup existing progress file before resetting
        """
        if not self.progress_file.exists():
            logger.info("No progress file to reset")
            return

        if backup:
            # Create backup with timestamp
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            backup_file = self.state_dir / f"progress_{timestamp}.backup.json"

            try:
                shutil.copy2(self.progress_file, backup_file)
                logger.info(f"Backed up progress to {backup_file}")
            except Exception as e:
                logger.error(f"Failed to backup progress file: {e}")
                raise

        # Remove current progress file
        try:
            self.progress_file.unlink()
            logger.info("Progress file reset")
        except Exception as e:
            logger.error(f"Failed to delete progress file: {e}")
            raise

    def create_new(
        self,
        total_records: int,
        oldest: datetime,
        newest: datetime,
        dry_run: bool
    ) -> MigrationProgress:
        """
        Create new progress for a fresh migration.

        Args:
            total_records: Total number of records to migrate
            oldest: Oldest timestamp in the data
            newest: Newest timestamp in the data
            dry_run: Whether this is a dry-run migration

        Returns:
            New MigrationProgress instance
        """
        now = datetime.utcnow().isoformat() + 'Z'

        progress = MigrationProgress(
            started_at=now,
            last_updated=now,
            status="not_started",
            total_records=total_records,
            oldest_timestamp=oldest.isoformat() + 'Z',
            newest_timestamp=newest.isoformat() + 'Z',
            last_migrated_date=None,
            records_migrated=0,
            records_failed=0,
            batches_sent=0,
            errors=[],
            dry_run=dry_run
        )

        logger.info(f"Created new progress: total_records={total_records:,}, "
                   f"date_range={oldest.date()} to {newest.date()}, dry_run={dry_run}")

        return progress

    def update(
        self,
        progress: MigrationProgress,
        migrated_date: date,
        records: int,
        batches: int
    ) -> None:
        """
        Update progress after processing a day.

        Automatically saves to file after updating.

        Args:
            progress: MigrationProgress to update
            migrated_date: The date that was just migrated
            records: Number of records migrated for this date
            batches: Number of batches sent for this date
        """
        progress.last_migrated_date = migrated_date.isoformat()
        progress.records_migrated += records
        progress.batches_sent += batches
        progress.status = "in_progress"

        # Save to file
        self.save(progress)

        # Calculate progress percentage
        if progress.total_records > 0:
            pct = (progress.records_migrated / progress.total_records) * 100
            logger.info(f"Progress updated: {migrated_date} completed, "
                       f"{progress.records_migrated:,}/{progress.total_records:,} "
                       f"records ({pct:.1f}%)")
        else:
            logger.info(f"Progress updated: {migrated_date} completed, "
                       f"{progress.records_migrated:,} records")

    def mark_completed(self, progress: MigrationProgress) -> None:
        """
        Mark migration as completed.

        Args:
            progress: MigrationProgress to mark as completed
        """
        progress.status = "completed"
        self.save(progress)

        logger.info(f"Migration marked as completed: "
                   f"{progress.records_migrated:,} records migrated, "
                   f"{progress.batches_sent} batches sent")

    def mark_failed(self, progress: MigrationProgress, error: str) -> None:
        """
        Mark migration as failed with error message.

        Args:
            progress: MigrationProgress to mark as failed
            error: Error message describing the failure
        """
        progress.status = "failed"
        progress.errors.append(error)
        self.save(progress)

        logger.error(f"Migration marked as failed: {error}")

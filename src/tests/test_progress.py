"""
Tests for Progress Tracking Module

Tests the progress.py module including MigrationProgress dataclass and ProgressTracker.
"""

import json
import pytest
import tempfile
import shutil
from datetime import datetime, date
from pathlib import Path

from progress import MigrationProgress, ProgressTracker


@pytest.fixture
def temp_state_dir():
    """Create a temporary directory for state files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup after test
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def tracker(temp_state_dir):
    """Create a ProgressTracker instance with temp directory."""
    return ProgressTracker(temp_state_dir)


@pytest.fixture
def sample_progress():
    """Create a sample MigrationProgress instance."""
    return MigrationProgress(
        started_at="2025-11-30T10:00:00Z",
        last_updated="2025-11-30T10:00:00Z",
        status="not_started",
        total_records=1000000,
        oldest_timestamp="2025-05-01T00:00:00Z",
        newest_timestamp="2025-11-30T23:59:59Z",
        last_migrated_date=None,
        records_migrated=0,
        records_failed=0,
        batches_sent=0,
        errors=[],
        dry_run=False
    )


class TestMigrationProgress:
    """Tests for MigrationProgress dataclass."""

    def test_to_dict(self, sample_progress):
        """Test converting MigrationProgress to dictionary."""
        data = sample_progress.to_dict()

        assert isinstance(data, dict)
        assert data['status'] == 'not_started'
        assert data['total_records'] == 1000000
        assert data['records_migrated'] == 0
        assert data['dry_run'] is False

    def test_from_dict(self):
        """Test creating MigrationProgress from dictionary."""
        data = {
            'started_at': "2025-11-30T10:00:00Z",
            'last_updated': "2025-11-30T10:00:00Z",
            'status': "in_progress",
            'total_records': 500000,
            'oldest_timestamp': "2025-05-01T00:00:00Z",
            'newest_timestamp': "2025-11-30T23:59:59Z",
            'last_migrated_date': "2025-06-15",
            'records_migrated': 250000,
            'records_failed': 10,
            'batches_sent': 25,
            'errors': ["Some error"],
            'dry_run': True
        }

        progress = MigrationProgress.from_dict(data)

        assert progress.status == "in_progress"
        assert progress.total_records == 500000
        assert progress.records_migrated == 250000
        assert progress.last_migrated_date == "2025-06-15"
        assert progress.dry_run is True
        assert len(progress.errors) == 1

    def test_round_trip_conversion(self, sample_progress):
        """Test converting to dict and back preserves data."""
        data = sample_progress.to_dict()
        restored = MigrationProgress.from_dict(data)

        assert restored.status == sample_progress.status
        assert restored.total_records == sample_progress.total_records
        assert restored.records_migrated == sample_progress.records_migrated
        assert restored.dry_run == sample_progress.dry_run


class TestProgressTracker:
    """Tests for ProgressTracker class."""

    def test_init_creates_state_dir(self, temp_state_dir):
        """Test that initializing tracker creates state directory."""
        state_dir = Path(temp_state_dir) / "subdir" / "state"
        tracker = ProgressTracker(str(state_dir))

        assert state_dir.exists()
        assert state_dir.is_dir()

    def test_load_nonexistent_returns_none(self, tracker):
        """Test loading when no progress file exists returns None."""
        progress = tracker.load()
        assert progress is None

    def test_save_and_load_round_trip(self, tracker, sample_progress):
        """Test saving and loading progress preserves data."""
        # Save progress
        tracker.save(sample_progress)

        # Verify file exists
        assert tracker.progress_file.exists()

        # Load it back
        loaded = tracker.load()

        assert loaded is not None
        assert loaded.status == sample_progress.status
        assert loaded.total_records == sample_progress.total_records
        assert loaded.records_migrated == sample_progress.records_migrated

    def test_save_updates_last_updated(self, tracker, sample_progress):
        """Test that saving updates the last_updated timestamp."""
        original_updated = sample_progress.last_updated

        # Save (should update last_updated)
        tracker.save(sample_progress)

        # The in-memory object should have been updated
        assert sample_progress.last_updated != original_updated

        # Load and verify
        loaded = tracker.load()
        assert loaded.last_updated != original_updated

    def test_save_is_atomic(self, tracker, sample_progress):
        """Test that save uses atomic write (temp file + rename)."""
        tracker.save(sample_progress)

        # Verify no temp file left behind
        temp_file = tracker.progress_file.with_suffix('.tmp')
        assert not temp_file.exists()

        # Verify actual file exists
        assert tracker.progress_file.exists()

    def test_load_corrupted_file_returns_none(self, tracker, temp_state_dir):
        """Test that loading a corrupted JSON file returns None."""
        # Create corrupted JSON file
        with open(tracker.progress_file, 'w') as f:
            f.write("{ invalid json")

        progress = tracker.load()
        assert progress is None

    def test_create_new(self, tracker):
        """Test creating new progress."""
        oldest = datetime(2025, 5, 1, 0, 0, 0)
        newest = datetime(2025, 11, 30, 23, 59, 59)

        progress = tracker.create_new(
            total_records=1000000,
            oldest=oldest,
            newest=newest,
            dry_run=False
        )

        assert progress.status == "not_started"
        assert progress.total_records == 1000000
        assert progress.records_migrated == 0
        assert progress.batches_sent == 0
        assert progress.last_migrated_date is None
        assert progress.dry_run is False
        assert len(progress.errors) == 0

    def test_update_increments_and_saves(self, tracker, sample_progress):
        """Test that update increments counters and saves to file."""
        # Save initial progress
        tracker.save(sample_progress)

        # Update with migration data
        migrated_date = date(2025, 6, 1)
        tracker.update(sample_progress, migrated_date, records=50000, batches=5)

        # Check in-memory object
        assert sample_progress.last_migrated_date == "2025-06-01"
        assert sample_progress.records_migrated == 50000
        assert sample_progress.batches_sent == 5
        assert sample_progress.status == "in_progress"

        # Verify saved to file
        loaded = tracker.load()
        assert loaded.last_migrated_date == "2025-06-01"
        assert loaded.records_migrated == 50000
        assert loaded.batches_sent == 5

    def test_update_accumulates(self, tracker, sample_progress):
        """Test that multiple updates accumulate correctly."""
        tracker.save(sample_progress)

        # First day
        tracker.update(sample_progress, date(2025, 6, 1), records=10000, batches=1)
        assert sample_progress.records_migrated == 10000
        assert sample_progress.batches_sent == 1

        # Second day
        tracker.update(sample_progress, date(2025, 6, 2), records=15000, batches=2)
        assert sample_progress.records_migrated == 25000
        assert sample_progress.batches_sent == 3

        # Third day
        tracker.update(sample_progress, date(2025, 6, 3), records=20000, batches=2)
        assert sample_progress.records_migrated == 45000
        assert sample_progress.batches_sent == 5

    def test_mark_completed(self, tracker, sample_progress):
        """Test marking migration as completed."""
        sample_progress.status = "in_progress"
        sample_progress.records_migrated = 1000000

        tracker.mark_completed(sample_progress)

        assert sample_progress.status == "completed"

        # Verify saved
        loaded = tracker.load()
        assert loaded.status == "completed"
        assert loaded.records_migrated == 1000000

    def test_mark_failed(self, tracker, sample_progress):
        """Test marking migration as failed."""
        error_msg = "Database connection timeout"

        tracker.mark_failed(sample_progress, error_msg)

        assert sample_progress.status == "failed"
        assert error_msg in sample_progress.errors

        # Verify saved
        loaded = tracker.load()
        assert loaded.status == "failed"
        assert error_msg in loaded.errors

    def test_reset_without_backup(self, tracker, sample_progress):
        """Test reset without backup removes file."""
        # Save a progress file
        tracker.save(sample_progress)
        assert tracker.progress_file.exists()

        # Reset without backup
        tracker.reset(backup=False)

        # Verify file removed
        assert not tracker.progress_file.exists()

    def test_reset_with_backup(self, tracker, sample_progress):
        """Test reset with backup creates timestamped backup."""
        # Save a progress file
        tracker.save(sample_progress)
        assert tracker.progress_file.exists()

        # Reset with backup
        tracker.reset(backup=True)

        # Verify original file removed
        assert not tracker.progress_file.exists()

        # Verify backup file created
        backup_files = list(tracker.state_dir.glob("progress_*.backup.json"))
        assert len(backup_files) == 1
        assert backup_files[0].exists()

        # Verify backup content
        with open(backup_files[0], 'r') as f:
            data = json.load(f)
        assert data['status'] == sample_progress.status

    def test_reset_nonexistent_file_no_error(self, tracker):
        """Test that resetting when no file exists doesn't error."""
        # Should not raise exception
        tracker.reset(backup=False)
        tracker.reset(backup=True)

    def test_multiple_saves_same_file(self, tracker, sample_progress):
        """Test multiple saves overwrite correctly."""
        # First save
        sample_progress.status = "not_started"
        tracker.save(sample_progress)

        # Update and save again
        sample_progress.status = "in_progress"
        sample_progress.records_migrated = 50000
        tracker.save(sample_progress)

        # Load and verify latest state
        loaded = tracker.load()
        assert loaded.status == "in_progress"
        assert loaded.records_migrated == 50000

    def test_concurrent_tracker_instances(self, temp_state_dir, sample_progress):
        """Test that multiple tracker instances share the same state file."""
        tracker1 = ProgressTracker(temp_state_dir)
        tracker2 = ProgressTracker(temp_state_dir)

        # Save with tracker1
        tracker1.save(sample_progress)

        # Load with tracker2
        loaded = tracker2.load()
        assert loaded is not None
        assert loaded.status == sample_progress.status

    def test_dry_run_flag_preserved(self, tracker):
        """Test that dry_run flag is correctly saved and loaded."""
        oldest = datetime(2025, 5, 1)
        newest = datetime(2025, 11, 30)

        # Create with dry_run=True
        progress = tracker.create_new(
            total_records=100000,
            oldest=oldest,
            newest=newest,
            dry_run=True
        )
        tracker.save(progress)

        # Load and verify
        loaded = tracker.load()
        assert loaded.dry_run is True

        # Create with dry_run=False
        tracker.reset(backup=False)
        progress2 = tracker.create_new(
            total_records=100000,
            oldest=oldest,
            newest=newest,
            dry_run=False
        )
        tracker.save(progress2)

        # Load and verify
        loaded2 = tracker.load()
        assert loaded2.dry_run is False

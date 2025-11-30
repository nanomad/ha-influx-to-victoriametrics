"""
VictoriaMetrics Writer Module

Handles writing time-series data to VictoriaMetrics using the Prometheus import format.
Supports batch writing, retry logic, and dry-run mode for validation.
"""

import logging
import time
from dataclasses import dataclass
from typing import Dict, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WriteError(Exception):
    """Custom exception for VictoriaMetrics write errors"""
    def __init__(self, status_code: int, body: str):
        self.status_code = status_code
        self.body = body
        super().__init__(f"Write failed with status {status_code}: {body}")


@dataclass
class VMDataPoint:
    """
    Represents a single VictoriaMetrics data point.

    Attributes:
        metric_name: Metric name in Prometheus format (e.g., "homeassistant_sensor_temperature_celsius")
        labels: Dictionary of label key-value pairs (e.g., {"entity": "sensor.temp", "domain": "sensor"})
        value: Numeric measurement value
        timestamp_ms: Timestamp in milliseconds since Unix epoch
    """
    metric_name: str
    labels: Dict[str, str]
    value: float
    timestamp_ms: int


class VMWriter:
    """
    VictoriaMetrics writer that batches and writes data points using the Prometheus text format.

    Supports dry-run mode for validation without actual writes, retry logic for transient errors,
    and comprehensive statistics tracking.
    """

    def __init__(self, url: str, dry_run: bool = False, batch_size: int = 10000):
        """
        Initialize the VictoriaMetrics writer.

        Args:
            url: Base URL of VictoriaMetrics server (e.g., "http://vm:8428")
            dry_run: If True, validate but don't write data
            batch_size: Number of points to write per batch (default: 10000)
        """
        self._url = url.rstrip('/')
        self._import_url = f"{self._url}/api/v1/import/prometheus"
        self._health_url = f"{self._url}/health"
        self._dry_run = dry_run
        self._batch_size = batch_size

        # Statistics tracking
        self._points_written = 0
        self._batches_sent = 0

        # Configure session with retry logic
        self._session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,  # Exponential backoff: 2s, 4s, 8s
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST", "GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

        if self._dry_run:
            logger.info("VMWriter initialized in DRY-RUN mode - no data will be written")
        else:
            logger.info(f"VMWriter initialized: {self._url}, batch_size={self._batch_size}")

    def format_prometheus_line(self, point: VMDataPoint) -> str:
        """
        Format a single data point as a Prometheus text format line.

        The format is: metric_name{label1="value1",label2="value2"} value timestamp_ms
        Labels are properly escaped for special characters.

        Args:
            point: VMDataPoint to format

        Returns:
            Prometheus text format line
        """
        # Escape label values (handle quotes, backslashes, newlines)
        def escape_label_value(value: str) -> str:
            value = str(value)  # Ensure string type
            value = value.replace('\\', '\\\\')  # Escape backslashes first
            value = value.replace('"', '\\"')    # Escape quotes
            value = value.replace('\n', '\\n')   # Escape newlines
            return value

        # Build label string
        if point.labels:
            label_parts = [f'{key}="{escape_label_value(value)}"'
                          for key, value in sorted(point.labels.items())]
            label_string = '{' + ','.join(label_parts) + '}'
        else:
            label_string = ''

        # Format: metric_name{labels} value timestamp_ms
        return f"{point.metric_name}{label_string} {point.value} {point.timestamp_ms}"

    def write_batch(self, points: List[VMDataPoint]) -> int:
        """
        Write a batch of data points to VictoriaMetrics.

        In dry-run mode, validates formatting but doesn't actually write.
        Logs sample data points for verification.

        Args:
            points: List of VMDataPoint objects to write

        Returns:
            Number of points written (or would-be-written in dry-run)

        Raises:
            WriteError: If the write fails with an HTTP error
            ConnectionError: If unable to connect to VictoriaMetrics
        """
        if not points:
            return 0

        # Format all points as Prometheus text
        lines = [self.format_prometheus_line(point) for point in points]
        payload = '\n'.join(lines)

        if self._dry_run:
            # Dry-run mode: log samples but don't write
            sample_count = min(3, len(lines))
            logger.info(f"[DRY-RUN] Would write batch of {len(points)} points:")
            for i in range(sample_count):
                logger.info(f"  Sample {i+1}: {lines[i][:200]}...")  # Truncate long lines

            if len(lines) > sample_count:
                logger.info(f"  ... and {len(lines) - sample_count} more points")

            # Update statistics
            self._points_written += len(points)
            self._batches_sent += 1

            return len(points)

        # Real write mode
        try:
            logger.debug(f"Writing batch of {len(points)} points to {self._import_url}")

            response = self._session.post(
                self._import_url,
                data=payload.encode('utf-8'),
                headers={'Content-Type': 'text/plain'},
                timeout=30
            )

            # Check for HTTP errors
            if response.status_code >= 400:
                error_body = response.text[:500]  # Limit error message size
                raise WriteError(response.status_code, error_body)

            # Success
            self._points_written += len(points)
            self._batches_sent += 1

            logger.info(f"Successfully wrote batch of {len(points)} points "
                       f"(total: {self._points_written:,} points in {self._batches_sent} batches)")

            return len(points)

        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error writing to VictoriaMetrics: {e}")
            raise ConnectionError(f"Failed to connect to {self._import_url}") from e
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout writing to VictoriaMetrics: {e}")
            raise ConnectionError(f"Timeout connecting to {self._import_url}") from e
        except WriteError:
            # Re-raise WriteError as-is
            raise
        except Exception as e:
            logger.error(f"Unexpected error writing to VictoriaMetrics: {e}")
            raise

    def health_check(self) -> bool:
        """
        Check if VictoriaMetrics is reachable and healthy.

        Calls the /health endpoint to verify connectivity.

        Returns:
            True if VictoriaMetrics is healthy, False otherwise
        """
        try:
            response = self._session.get(self._health_url, timeout=5)

            if response.status_code == 200:
                logger.info(f"Health check passed: {self._health_url}")
                return True
            else:
                logger.warning(f"Health check failed with status {response.status_code}")
                return False

        except requests.exceptions.RequestException as e:
            logger.error(f"Health check failed: {e}")
            return False

    @property
    def is_dry_run(self) -> bool:
        """Returns whether the writer is in dry-run mode."""
        return self._dry_run

    @property
    def points_written(self) -> int:
        """Returns total number of points written (or would-be-written in dry-run)."""
        return self._points_written

    @property
    def batches_sent(self) -> int:
        """Returns total number of batches sent."""
        return self._batches_sent

    def reset_stats(self):
        """Reset statistics counters."""
        self._points_written = 0
        self._batches_sent = 0
        logger.info("Statistics reset")

    def close(self):
        """Close the underlying HTTP session."""
        self._session.close()
        logger.debug("VMWriter session closed")

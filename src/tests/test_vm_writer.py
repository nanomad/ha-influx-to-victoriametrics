"""
Tests for the vm_writer module.

This module tests the VictoriaMetrics writer functionality with mocked HTTP requests,
ensuring proper data formatting, batch writing, error handling, and dry-run mode.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
import sys
from pathlib import Path

# Add parent directory to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from vm_writer import VMWriter, VMDataPoint, WriteError


class TestVMDataPoint:
    """Tests for the VMDataPoint dataclass."""

    def test_dataclass_creation(self):
        """Test creating a VMDataPoint."""
        point = VMDataPoint(
            metric_name="homeassistant_sensor_temperature_celsius",
            labels={"entity": "sensor.temp", "domain": "sensor"},
            value=21.5,
            timestamp_ms=1732960800000
        )
        assert point.metric_name == "homeassistant_sensor_temperature_celsius"
        assert point.labels == {"entity": "sensor.temp", "domain": "sensor"}
        assert point.value == 21.5
        assert point.timestamp_ms == 1732960800000

    def test_dataclass_equality(self):
        """Test that two VMDataPoints with same values are equal."""
        point1 = VMDataPoint("test_metric", {"label": "value"}, 10.0, 1000000)
        point2 = VMDataPoint("test_metric", {"label": "value"}, 10.0, 1000000)
        assert point1 == point2


class TestVMWriterInit:
    """Tests for VMWriter initialization."""

    def test_init_normal_mode(self):
        """Test VMWriter initialization in normal mode."""
        writer = VMWriter("http://localhost:8428", dry_run=False, batch_size=5000)
        assert writer._url == "http://localhost:8428"
        assert writer._import_url == "http://localhost:8428/api/v1/import/prometheus"
        assert writer._health_url == "http://localhost:8428/health"
        assert writer._dry_run is False
        assert writer._batch_size == 5000
        assert writer._points_written == 0
        assert writer._batches_sent == 0

    def test_init_dry_run_mode(self):
        """Test VMWriter initialization in dry-run mode."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        assert writer._dry_run is True
        assert writer.is_dry_run is True

    def test_init_strips_trailing_slash(self):
        """Test that trailing slash is stripped from URL."""
        writer = VMWriter("http://localhost:8428/", dry_run=True)
        assert writer._url == "http://localhost:8428"
        assert writer._import_url == "http://localhost:8428/api/v1/import/prometheus"


class TestFormatPrometheusLine:
    """Tests for format_prometheus_line() method."""

    def test_basic_format(self):
        """Test basic Prometheus line formatting."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        point = VMDataPoint(
            metric_name="test_metric",
            labels={"label1": "value1"},
            value=123.45,
            timestamp_ms=1732960800000
        )
        result = writer.format_prometheus_line(point)
        assert result == 'test_metric{label1="value1"} 123.45 1732960800000'

    def test_format_with_multiple_labels(self):
        """Test formatting with multiple labels (should be sorted)."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        point = VMDataPoint(
            metric_name="test_metric",
            labels={
                "entity": "sensor.temp",
                "domain": "sensor",
                "friendly_name": "Temperature"
            },
            value=21.5,
            timestamp_ms=1732960800000
        )
        result = writer.format_prometheus_line(point)
        # Labels should be sorted alphabetically
        expected = 'test_metric{domain="sensor",entity="sensor.temp",friendly_name="Temperature"} 21.5 1732960800000'
        assert result == expected

    def test_format_without_labels(self):
        """Test formatting without labels."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        point = VMDataPoint(
            metric_name="test_metric",
            labels={},
            value=100.0,
            timestamp_ms=1732960800000
        )
        result = writer.format_prometheus_line(point)
        assert result == 'test_metric 100.0 1732960800000'

    def test_format_escapes_quotes(self):
        """Test that quotes in label values are escaped."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        point = VMDataPoint(
            metric_name="test_metric",
            labels={"friendly_name": 'Test "quoted" value'},
            value=10.0,
            timestamp_ms=1732960800000
        )
        result = writer.format_prometheus_line(point)
        assert 'friendly_name="Test \\"quoted\\" value"' in result

    def test_format_escapes_backslashes(self):
        """Test that backslashes in label values are escaped."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        point = VMDataPoint(
            metric_name="test_metric",
            labels={"path": "C:\\Users\\test"},
            value=10.0,
            timestamp_ms=1732960800000
        )
        result = writer.format_prometheus_line(point)
        assert 'path="C:\\\\Users\\\\test"' in result

    def test_format_escapes_newlines(self):
        """Test that newlines in label values are escaped."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        point = VMDataPoint(
            metric_name="test_metric",
            labels={"description": "Line 1\nLine 2"},
            value=10.0,
            timestamp_ms=1732960800000
        )
        result = writer.format_prometheus_line(point)
        assert 'description="Line 1\\nLine 2"' in result

    def test_format_with_special_characters(self):
        """Test formatting with multiple special characters."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        point = VMDataPoint(
            metric_name="test_metric",
            labels={"name": 'Test "value" with\\backslash and\nnewline'},
            value=10.0,
            timestamp_ms=1732960800000
        )
        result = writer.format_prometheus_line(point)
        expected = 'test_metric{name="Test \\"value\\" with\\\\backslash and\\nnewline"} 10.0 1732960800000'
        assert result == expected

    def test_format_integer_value(self):
        """Test formatting with integer value."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        point = VMDataPoint(
            metric_name="test_metric",
            labels={"entity": "sensor.test"},
            value=42,
            timestamp_ms=1732960800000
        )
        result = writer.format_prometheus_line(point)
        assert " 42 " in result

    def test_format_negative_value(self):
        """Test formatting with negative value."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        point = VMDataPoint(
            metric_name="test_metric",
            labels={"entity": "sensor.test"},
            value=-15.3,
            timestamp_ms=1732960800000
        )
        result = writer.format_prometheus_line(point)
        assert " -15.3 " in result


class TestWriteBatchDryRun:
    """Tests for write_batch() in dry-run mode."""

    def test_write_batch_dry_run(self):
        """Test write_batch in dry-run mode."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        points = [
            VMDataPoint("metric1", {"label": "value1"}, 10.0, 1000000),
            VMDataPoint("metric2", {"label": "value2"}, 20.0, 2000000),
        ]

        result = writer.write_batch(points)

        assert result == 2
        assert writer.points_written == 2
        assert writer.batches_sent == 1

    def test_write_batch_dry_run_empty(self):
        """Test write_batch in dry-run mode with empty list."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        result = writer.write_batch([])

        assert result == 0
        assert writer.points_written == 0
        assert writer.batches_sent == 0

    def test_write_batch_dry_run_multiple_batches(self):
        """Test write_batch tracks statistics across multiple batches."""
        writer = VMWriter("http://localhost:8428", dry_run=True)

        batch1 = [VMDataPoint("metric1", {}, 10.0, 1000000)]
        batch2 = [
            VMDataPoint("metric2", {}, 20.0, 2000000),
            VMDataPoint("metric3", {}, 30.0, 3000000),
        ]

        writer.write_batch(batch1)
        writer.write_batch(batch2)

        assert writer.points_written == 3
        assert writer.batches_sent == 2


class TestWriteBatchRealMode:
    """Tests for write_batch() in real write mode."""

    @patch('vm_writer.requests.Session')
    def test_write_batch_success(self, mock_session_class):
        """Test successful write_batch in real mode."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        writer = VMWriter("http://localhost:8428", dry_run=False)
        points = [
            VMDataPoint("metric1", {"label": "value1"}, 10.0, 1000000),
            VMDataPoint("metric2", {"label": "value2"}, 20.0, 2000000),
        ]

        result = writer.write_batch(points)

        assert result == 2
        assert writer.points_written == 2
        assert writer.batches_sent == 1

        # Verify POST was called correctly
        mock_session.post.assert_called_once()
        call_args = mock_session.post.call_args
        assert call_args[0][0] == "http://localhost:8428/api/v1/import/prometheus"
        assert call_args[1]['headers']['Content-Type'] == 'text/plain'

    @patch('vm_writer.requests.Session')
    def test_write_batch_http_error(self, mock_session_class):
        """Test write_batch raises WriteError on HTTP error."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        writer = VMWriter("http://localhost:8428", dry_run=False)
        points = [VMDataPoint("metric1", {}, 10.0, 1000000)]

        with pytest.raises(WriteError) as exc_info:
            writer.write_batch(points)

        assert exc_info.value.status_code == 500
        assert "Internal Server Error" in exc_info.value.body

    @patch('vm_writer.requests.Session')
    def test_write_batch_connection_error(self, mock_session_class):
        """Test write_batch raises ConnectionError on connection failure."""
        mock_session = MagicMock()
        import requests
        mock_session.post.side_effect = requests.exceptions.ConnectionError("Connection refused")
        mock_session_class.return_value = mock_session

        writer = VMWriter("http://localhost:8428", dry_run=False)
        points = [VMDataPoint("metric1", {}, 10.0, 1000000)]

        with pytest.raises(ConnectionError) as exc_info:
            writer.write_batch(points)

        assert "Failed to connect" in str(exc_info.value)

    @patch('vm_writer.requests.Session')
    def test_write_batch_timeout_error(self, mock_session_class):
        """Test write_batch raises ConnectionError on timeout."""
        mock_session = MagicMock()
        import requests
        mock_session.post.side_effect = requests.exceptions.Timeout("Request timeout")
        mock_session_class.return_value = mock_session

        writer = VMWriter("http://localhost:8428", dry_run=False)
        points = [VMDataPoint("metric1", {}, 10.0, 1000000)]

        with pytest.raises(ConnectionError) as exc_info:
            writer.write_batch(points)

        assert "Timeout" in str(exc_info.value)

    @patch('vm_writer.requests.Session')
    def test_write_batch_formats_payload_correctly(self, mock_session_class):
        """Test that write_batch formats the payload correctly."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        writer = VMWriter("http://localhost:8428", dry_run=False)
        points = [
            VMDataPoint("metric1", {"entity": "sensor.temp"}, 21.5, 1000000),
            VMDataPoint("metric2", {"entity": "sensor.humidity"}, 65.0, 2000000),
        ]

        writer.write_batch(points)

        # Get the actual payload sent
        call_args = mock_session.post.call_args
        payload = call_args[1]['data'].decode('utf-8')

        # Verify payload contains both lines
        assert 'metric1{entity="sensor.temp"} 21.5 1000000' in payload
        assert 'metric2{entity="sensor.humidity"} 65.0 2000000' in payload
        assert '\n' in payload


class TestHealthCheck:
    """Tests for health_check() method."""

    @patch('vm_writer.requests.Session')
    def test_health_check_success(self, mock_session_class):
        """Test successful health check."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        writer = VMWriter("http://localhost:8428", dry_run=True)
        result = writer.health_check()

        assert result is True
        mock_session.get.assert_called_once_with("http://localhost:8428/health", timeout=5)

    @patch('vm_writer.requests.Session')
    def test_health_check_failure(self, mock_session_class):
        """Test failed health check."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        writer = VMWriter("http://localhost:8428", dry_run=True)
        result = writer.health_check()

        assert result is False

    @patch('vm_writer.requests.Session')
    def test_health_check_connection_error(self, mock_session_class):
        """Test health check with connection error."""
        mock_session = MagicMock()
        import requests
        mock_session.get.side_effect = requests.exceptions.RequestException("Connection error")
        mock_session_class.return_value = mock_session

        writer = VMWriter("http://localhost:8428", dry_run=True)
        result = writer.health_check()

        assert result is False


class TestVMWriterProperties:
    """Tests for VMWriter properties and utility methods."""

    def test_is_dry_run_property(self):
        """Test is_dry_run property."""
        writer_dry = VMWriter("http://localhost:8428", dry_run=True)
        writer_real = VMWriter("http://localhost:8428", dry_run=False)

        assert writer_dry.is_dry_run is True
        assert writer_real.is_dry_run is False

    def test_points_written_property(self):
        """Test points_written property."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        assert writer.points_written == 0

        writer.write_batch([VMDataPoint("m1", {}, 10.0, 1000)])
        assert writer.points_written == 1

    def test_batches_sent_property(self):
        """Test batches_sent property."""
        writer = VMWriter("http://localhost:8428", dry_run=True)
        assert writer.batches_sent == 0

        writer.write_batch([VMDataPoint("m1", {}, 10.0, 1000)])
        assert writer.batches_sent == 1

    def test_reset_stats(self):
        """Test reset_stats() method."""
        writer = VMWriter("http://localhost:8428", dry_run=True)

        # Write some data
        writer.write_batch([VMDataPoint("m1", {}, 10.0, 1000)])
        writer.write_batch([VMDataPoint("m2", {}, 20.0, 2000)])

        assert writer.points_written == 2
        assert writer.batches_sent == 2

        # Reset statistics
        writer.reset_stats()

        assert writer.points_written == 0
        assert writer.batches_sent == 0

    @patch('vm_writer.requests.Session')
    def test_close(self, mock_session_class):
        """Test close() method."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        writer = VMWriter("http://localhost:8428", dry_run=True)
        writer.close()

        mock_session.close.assert_called_once()


class TestWriteError:
    """Tests for WriteError exception."""

    def test_write_error_creation(self):
        """Test creating a WriteError."""
        error = WriteError(500, "Internal Server Error")
        assert error.status_code == 500
        assert error.body == "Internal Server Error"
        assert "500" in str(error)
        assert "Internal Server Error" in str(error)

    def test_write_error_is_exception(self):
        """Test that WriteError is a proper exception."""
        error = WriteError(400, "Bad Request")
        assert isinstance(error, Exception)

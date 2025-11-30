"""
Tests for the influx_reader module.

This module tests the InfluxDB reader functionality with mocked InfluxDB responses,
ensuring proper data extraction and error handling without requiring a real connection.
"""

import pytest
import sys
from datetime import datetime, date, timedelta, timezone
from unittest.mock import Mock, MagicMock
from pathlib import Path

# Add parent directory to path so we can import local modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import influx_reader
from influx_reader import InfluxDBReader, InfluxDataPoint, QueryError


class TestInfluxDataPoint:
    """Tests for the InfluxDataPoint dataclass."""

    def test_dataclass_creation(self):
        """Test creating an InfluxDataPoint."""
        point = InfluxDataPoint(
            timestamp=datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc),
            domain="sensor",
            entity_id="temperature_living_room",
            friendly_name="Living Room Temperature",
            measurement="°C",
            value=21.5
        )
        assert point.domain == "sensor"
        assert point.entity_id == "temperature_living_room"
        assert point.friendly_name == "Living Room Temperature"
        assert point.measurement == "°C"
        assert point.value == 21.5
        assert isinstance(point.timestamp, datetime)

    def test_dataclass_equality(self):
        """Test that two InfluxDataPoints with same values are equal."""
        point1 = InfluxDataPoint(
            timestamp=datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc),
            domain="sensor",
            entity_id="temp",
            friendly_name="Temp",
            measurement="°C",
            value=21.5
        )
        point2 = InfluxDataPoint(
            timestamp=datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc),
            domain="sensor",
            entity_id="temp",
            friendly_name="Temp",
            measurement="°C",
            value=21.5
        )
        assert point1 == point2


class TestInfluxDBReaderInit:
    """Tests for InfluxDBReader initialization."""

    def test_successful_initialization(self, mocker):
        """Test successful InfluxDBReader initialization."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api

        # Patch InfluxDBClient in the influx_reader module's namespace
        mock_client_class = mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        reader = InfluxDBReader(
            url="http://localhost:8086",
            token="test-token",
            org="test-org",
            bucket="test-bucket"
        )

        assert reader.url == "http://localhost:8086"
        assert reader.token == "test-token"
        assert reader.org == "test-org"
        assert reader.bucket == "test-bucket"
        assert reader._client is not None
        assert reader._query_api is not None

        mock_client_class.assert_called_once_with(
            url="http://localhost:8086",
            token="test-token",
            org="test-org",
            timeout=300_000
        )

    def test_initialization_connection_error(self, mocker):
        """Test that initialization raises ConnectionError on failure."""
        mocker.patch.object(influx_reader, 'InfluxDBClient', side_effect=Exception("Connection refused"))

        with pytest.raises(ConnectionError) as exc_info:
            InfluxDBReader(
                url="http://localhost:8086",
                token="test-token",
                org="test-org",
                bucket="test-bucket"
            )

        assert "Failed to connect to InfluxDB" in str(exc_info.value)


class TestInfluxDBReaderGetTimeRange:
    """Tests for get_time_range() method."""

    def test_get_time_range_success(self, mocker):
        """Test successful time range query."""
        # Setup mocks
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        # Create mock records
        min_time = datetime(2025, 5, 3, 0, 0, 0, tzinfo=timezone.utc)
        max_time = datetime(2025, 11, 28, 23, 59, 59, tzinfo=timezone.utc)

        mock_min_record = MagicMock()
        mock_min_record.get_time.return_value = min_time

        mock_max_record = MagicMock()
        mock_max_record.get_time.return_value = max_time

        mock_min_table = MagicMock()
        mock_min_table.records = [mock_min_record]

        mock_max_table = MagicMock()
        mock_max_table.records = [mock_max_record]

        # Setup query_api to return different results for min and max queries
        mock_query_api.query.side_effect = [[mock_min_table], [mock_max_table]]

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")
        result_min, result_max = reader.get_time_range()

        assert result_min == min_time
        assert result_max == max_time
        assert mock_query_api.query.call_count == 2

    def test_get_time_range_empty_bucket(self, mocker):
        """Test get_time_range raises QueryError for empty bucket."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        # Return empty results
        mock_query_api.query.return_value = []

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")

        with pytest.raises(QueryError) as exc_info:
            reader.get_time_range()

        assert "Could not determine time range" in str(exc_info.value)

    def test_get_time_range_query_failure(self, mocker):
        """Test get_time_range handles query failures."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        mock_query_api.query.side_effect = Exception("Query failed")

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")

        with pytest.raises(QueryError) as exc_info:
            reader.get_time_range()

        assert "Failed to get time range" in str(exc_info.value)


class TestInfluxDBReaderQueryRange:
    """Tests for query_range() method."""

    def test_query_range_success(self, mocker, mock_flux_record):
        """Test successful range query with data."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        # Create test data - query_stream returns records directly, not tables
        records = [
            mock_flux_record(
                datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc),
                "sensor",
                "temperature_living_room",
                "Living Room Temperature",
                "°C",
                21.5
            ),
            mock_flux_record(
                datetime(2025, 11, 30, 12, 1, 0, tzinfo=timezone.utc),
                "sensor",
                "humidity_bathroom",
                "Bathroom Humidity",
                "%",
                65.3
            )
        ]

        mock_query_api.query_stream.return_value = records

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")

        start = datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 11, 30, 13, 0, 0, tzinfo=timezone.utc)

        results = list(reader.query_range(start, end))

        assert len(results) == 2
        assert results[0].domain == "sensor"
        assert results[0].entity_id == "temperature_living_room"
        assert results[0].value == 21.5
        assert results[1].measurement == "%"

    def test_query_range_empty_result(self, mocker):
        """Test query_range with no results."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        mock_query_api.query_stream.return_value = []

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")

        start = datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 11, 30, 13, 0, 0, tzinfo=timezone.utc)

        results = list(reader.query_range(start, end))
        assert len(results) == 0

    def test_query_range_handles_missing_friendly_name(self, mocker, mock_flux_record):
        """Test query_range uses entity_id as fallback for missing friendly_name."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        # Create record without friendly_name - query_stream returns records directly
        record = mock_flux_record(
            datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc),
            "sensor",
            "test_sensor",
            None,  # No friendly_name
            "°C",
            21.5
        )

        mock_query_api.query_stream.return_value = [record]

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")

        start = datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 11, 30, 13, 0, 0, tzinfo=timezone.utc)

        results = list(reader.query_range(start, end))

        assert len(results) == 1
        assert results[0].friendly_name == "test_sensor"

    def test_query_range_adds_utc_timezone(self, mocker):
        """Test that query_range adds UTC timezone to naive datetimes."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        mock_query_api.query_stream.return_value = []

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")

        # Use naive datetimes
        start = datetime(2025, 11, 30, 12, 0, 0)
        end = datetime(2025, 11, 30, 13, 0, 0)

        list(reader.query_range(start, end))

        # Verify query was called with UTC formatted strings
        call_args = mock_query_api.query_stream.call_args[0][0]
        assert "2025-11-30T12:00:00Z" in call_args
        assert "2025-11-30T13:00:00Z" in call_args

    def test_query_range_error(self, mocker):
        """Test query_range raises QueryError on failure."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        mock_query_api.query_stream.side_effect = Exception("Query failed")

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")

        start = datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 11, 30, 13, 0, 0, tzinfo=timezone.utc)

        with pytest.raises(QueryError) as exc_info:
            list(reader.query_range(start, end))

        assert "Failed to query range" in str(exc_info.value)


class TestInfluxDBReaderQueryDay:
    """Tests for query_day() method."""

    def test_query_day(self, mocker):
        """Test query_day calls query_range with correct datetime range."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        mock_query_api.query_stream.return_value = []

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")

        test_date = date(2025, 11, 30)
        list(reader.query_day(test_date))

        # Verify query was called with the full day range
        call_args = mock_query_api.query_stream.call_args[0][0]
        assert "2025-11-30T00:00:00Z" in call_args
        assert "2025-12-01T00:00:00Z" in call_args


class TestInfluxDBReaderCountRecords:
    """Tests for count_records() method."""

    def test_count_records_no_range(self, mocker):
        """Test count_records with no time range."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        mock_record = MagicMock()
        mock_record.get_value.return_value = 55070155

        mock_table = MagicMock()
        mock_table.records = [mock_record]

        mock_query_api.query.return_value = [mock_table]

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")
        count = reader.count_records()

        assert count == 55070155
        call_args = mock_query_api.query.call_args[0][0]
        assert "range(start: 0)" in call_args

    def test_count_records_with_start(self, mocker):
        """Test count_records with start time only."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        mock_record = MagicMock()
        mock_record.get_value.return_value = 1000

        mock_table = MagicMock()
        mock_table.records = [mock_record]

        mock_query_api.query.return_value = [mock_table]

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")
        start = datetime(2025, 11, 30, 0, 0, 0, tzinfo=timezone.utc)
        count = reader.count_records(start=start)

        assert count == 1000
        call_args = mock_query_api.query.call_args[0][0]
        assert "2025-11-30T00:00:00Z" in call_args

    def test_count_records_with_range(self, mocker):
        """Test count_records with start and end times."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        mock_record = MagicMock()
        mock_record.get_value.return_value = 500

        mock_table = MagicMock()
        mock_table.records = [mock_record]

        mock_query_api.query.return_value = [mock_table]

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")
        start = datetime(2025, 11, 30, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc)
        count = reader.count_records(start=start, end=end)

        assert count == 500

    def test_count_records_error(self, mocker):
        """Test count_records raises QueryError on failure."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        mock_query_api.query.side_effect = Exception("Query failed")

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")

        with pytest.raises(QueryError) as exc_info:
            reader.count_records()

        assert "Failed to count records" in str(exc_info.value)


class TestInfluxDBReaderClose:
    """Tests for close() method."""

    def test_close(self, mocker):
        """Test that close() closes the client."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mocker.patch.object(influx_reader, 'InfluxDBClient', return_value=mock_client)

        reader = InfluxDBReader("http://localhost:8086", "token", "org", "bucket")
        reader.close()

        mock_client.close.assert_called_once()

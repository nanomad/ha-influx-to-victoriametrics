"""
InfluxDB client for querying Home Assistant historical data.

This module provides a streaming interface to read time-series data from InfluxDB 2.x,
specifically designed for the Home Assistant to VictoriaMetrics migration.
"""

from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Iterator, Tuple, Optional, List
import logging

from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import FluxTable, FluxRecord


logger = logging.getLogger(__name__)


class QueryError(Exception):
    """Raised when an InfluxDB query fails."""
    pass


@dataclass
class InfluxDataPoint:
    """Represents a single data point from InfluxDB."""
    timestamp: datetime  # UTC timestamp
    domain: str          # e.g., "sensor"
    entity_id: str       # e.g., "temperature_living_room"
    friendly_name: str   # e.g., "Living Room Temperature"
    measurement: str     # e.g., "°C" (the unit)
    field: str           # e.g., "value", "current_temperature", "brightness"
    value: float         # numeric value


# Default fields to query - 'value' is the standard HA field
DEFAULT_FIELDS = ["value"]

# Extended fields for specific domains that have additional time-series data
EXTENDED_FIELDS = {
    "climate": ["value", "current_temperature", "temperature"],
    "cover": ["value", "current_position"],
    "light": ["value", "brightness"],
}


class InfluxDBReader:
    """
    Streaming reader for InfluxDB 2.x Home Assistant data.

    Provides methods to query historical time-series data with memory-efficient
    streaming iteration.
    """

    def __init__(
        self,
        url: str,
        token: str,
        org: str,
        bucket: str,
        domains: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
        use_extended_fields: bool = False
    ):
        """
        Initialize InfluxDB connection.

        Args:
            url: InfluxDB server URL
            token: Authentication token
            org: Organization name
            bucket: Bucket name
            domains: Optional list of domains to filter (None = all domains)
            fields: Optional list of fields to query (None = use defaults)
            use_extended_fields: If True, use EXTENDED_FIELDS per domain
        """
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.domains = domains
        self.fields = fields
        self.use_extended_fields = use_extended_fields
        self._client = None
        self._query_api = None

        try:
            self._client = InfluxDBClient(
                url=url,
                token=token,
                org=org,
                timeout=300_000  # 5 minute timeout for large queries
            )
            self._query_api = self._client.query_api()
            logger.info(f"Connected to InfluxDB at {url}")
            if domains:
                logger.info(f"Filtering to domains: {domains}")
            if use_extended_fields:
                logger.info(f"Using extended fields per domain")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to InfluxDB: {e}")

    def _get_fields_for_domain(self, domain: Optional[str] = None) -> List[str]:
        """Get the list of fields to query for a given domain."""
        if self.fields:
            return self.fields
        if self.use_extended_fields and domain and domain in EXTENDED_FIELDS:
            return EXTENDED_FIELDS[domain]
        return DEFAULT_FIELDS

    def _build_field_filter(self, domain: Optional[str] = None) -> str:
        """Build Flux filter expression for fields."""
        fields = self._get_fields_for_domain(domain)
        if len(fields) == 1:
            return f'r._field == "{fields[0]}"'
        conditions = " or ".join(f'r._field == "{f}"' for f in fields)
        return f"({conditions})"

    def _build_domain_filter(self) -> str:
        """Build Flux filter expression for domains."""
        if not self.domains:
            return ""
        if len(self.domains) == 1:
            return f'|> filter(fn: (r) => r["domain"] == "{self.domains[0]}")'
        conditions = " or ".join(f'r["domain"] == "{d}"' for d in self.domains)
        return f"|> filter(fn: (r) => {conditions})"

    def get_time_range(self) -> Tuple[datetime, datetime]:
        """
        Get the time range of data in the bucket.

        Returns:
            Tuple of (oldest_timestamp, newest_timestamp) in UTC

        Raises:
            QueryError: If the query fails
        """
        flux_query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: 0)
          |> filter(fn: (r) => r._field == "value")
          |> group()
          |> keep(columns: ["_time"])
          |> min(column: "_time")
        '''

        flux_query_max = f'''
        from(bucket: "{self.bucket}")
          |> range(start: 0)
          |> filter(fn: (r) => r._field == "value")
          |> group()
          |> keep(columns: ["_time"])
          |> max(column: "_time")
        '''

        try:
            # Query for minimum time
            result_min = self._query_api.query(flux_query)
            min_time = None
            for table in result_min:
                for record in table.records:
                    min_time = record.get_time()
                    break
                if min_time:
                    break

            # Query for maximum time
            result_max = self._query_api.query(flux_query_max)
            max_time = None
            for table in result_max:
                for record in table.records:
                    max_time = record.get_time()
                    break
                if max_time:
                    break

            if not min_time or not max_time:
                raise QueryError("Could not determine time range - bucket may be empty")

            logger.info(f"Data range: {min_time} to {max_time}")
            return (min_time, max_time)

        except Exception as e:
            raise QueryError(f"Failed to get time range: {e}")

    def query_day(self, date: date) -> Iterator[InfluxDataPoint]:
        """
        Query all data points for a specific day.

        Args:
            date: The date to query (will query 00:00:00 to 23:59:59 UTC)

        Yields:
            InfluxDataPoint objects for all value records in the day

        Raises:
            QueryError: If the query fails
        """
        # Convert date to datetime range (entire day in UTC)
        start = datetime.combine(date, datetime.min.time(), tzinfo=timezone.utc)
        end = start + timedelta(days=1)

        yield from self.query_range(start, end)

    def query_range(self, start: datetime, end: datetime) -> Iterator[InfluxDataPoint]:
        """
        Query all data points in a time range.

        Args:
            start: Start time (inclusive)
            end: End time (exclusive)

        Yields:
            InfluxDataPoint objects for all records in the range

        Raises:
            QueryError: If the query fails
        """
        # Ensure timestamps are in UTC
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        # Format timestamps for Flux
        start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Build field filter - include all extended fields if enabled
        if self.use_extended_fields:
            all_fields = set(DEFAULT_FIELDS)
            for domain_fields in EXTENDED_FIELDS.values():
                all_fields.update(domain_fields)
            field_conditions = " or ".join(f'r._field == "{f}"' for f in sorted(all_fields))
            field_filter = f"|> filter(fn: (r) => {field_conditions})"
        elif self.fields:
            if len(self.fields) == 1:
                field_filter = f'|> filter(fn: (r) => r._field == "{self.fields[0]}")'
            else:
                field_conditions = " or ".join(f'r._field == "{f}"' for f in self.fields)
                field_filter = f"|> filter(fn: (r) => {field_conditions})"
        else:
            field_filter = '|> filter(fn: (r) => r._field == "value")'

        # Build domain filter
        domain_filter = self._build_domain_filter()

        flux_query = f'''
        from(bucket: "{self.bucket}")
          |> range(start: {start_str}, stop: {end_str})
          {field_filter}
          {domain_filter}
        '''

        logger.debug(f"Querying range: {start_str} to {end_str}")

        try:
            # Stream results using query_stream for memory efficiency
            # query_stream returns an iterator of FluxRecord objects directly
            records = self._query_api.query_stream(flux_query)

            count = 0
            skipped = 0
            for record in records:
                # Extract data from record
                timestamp = record.get_time()
                measurement = record.get_measurement()
                value = record.get_value()
                field = record.get_field()

                # Get tags (with fallback for missing values)
                domain = record.values.get("domain", "unknown")
                entity_id = record.values.get("entity_id", "unknown")
                friendly_name = record.values.get("friendly_name")

                # When using extended fields, filter to only valid domain+field combos
                if self.use_extended_fields:
                    valid_fields = self._get_fields_for_domain(domain)
                    if field not in valid_fields:
                        skipped += 1
                        continue

                # Use entity_id as fallback for missing friendly_name
                if not friendly_name:
                    friendly_name = entity_id

                count += 1
                yield InfluxDataPoint(
                    timestamp=timestamp,
                    domain=domain,
                    entity_id=entity_id,
                    friendly_name=friendly_name,
                    measurement=measurement,
                    field=field,
                    value=float(value)
                )

            logger.debug(f"Yielded {count} data points, skipped {skipped}")

        except Exception as e:
            raise QueryError(f"Failed to query range {start_str} to {end_str}: {e}")

    def count_records(self, start: Optional[datetime] = None, end: Optional[datetime] = None) -> int:
        """
        Count the number of value records in a time range.

        Args:
            start: Start time (inclusive), defaults to earliest data
            end: End time (exclusive), defaults to latest data

        Returns:
            Number of value records in the range

        Raises:
            QueryError: If the query fails
        """
        # Build time range
        if start is None and end is None:
            range_clause = "range(start: 0)"
        elif start is not None and end is None:
            start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
            range_clause = f"range(start: {start_str})"
        elif start is None and end is not None:
            end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
            range_clause = f"range(start: 0, stop: {end_str})"
        else:
            start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
            range_clause = f"range(start: {start_str}, stop: {end_str})"

        flux_query = f'''
        from(bucket: "{self.bucket}")
          |> {range_clause}
          |> filter(fn: (r) => r._field == "value")
          |> count()
        '''

        try:
            result = self._query_api.query(flux_query)

            total_count = 0
            for table in result:
                for record in table.records:
                    # The count() function returns the count in the _value field
                    count_value = record.get_value()
                    if count_value is not None:
                        total_count += int(count_value)

            logger.info(f"Record count: {total_count}")
            return total_count

        except Exception as e:
            raise QueryError(f"Failed to count records: {e}")

    def close(self):
        """Close the InfluxDB connection."""
        if self._client:
            self._client.close()
            logger.info("InfluxDB connection closed")

"""
Pytest configuration and shared fixtures for the migration test suite.

This module provides reusable fixtures for creating test data and mocking
external dependencies like InfluxDB and VictoriaMetrics.
"""

import pytest
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

# Add parent directory to path so we can import local modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from influxdb_client.client.flux_table import FluxTable, FluxRecord
from influx_reader import InfluxDataPoint
from vm_writer import VMDataPoint


# Mock VictoriaMetrics metrics for all tests
MOCK_VM_METRICS = {
    "homeassistant_sensor_temperature_celsius",
    "homeassistant_sensor_battery_percent",
    "homeassistant_sensor_humidity_percent",
    "homeassistant_sensor_power_w",
    "homeassistant_sensor_energy_kwh",
    "homeassistant_sensor_voltage_v",
    "homeassistant_sensor_current_a",
    "homeassistant_sensor_pressure_bar",
    "homeassistant_sensor_illuminance_lx",
    "homeassistant_sensor_distance_m",
    "homeassistant_sensor_distance_km",
    "homeassistant_sensor_speed_km_per_h",
    "homeassistant_sensor_duration_s",
    "homeassistant_sensor_duration_min",
    "homeassistant_sensor_duration_h",
    "homeassistant_sensor_signal_strength_dbm",
    "homeassistant_sensor_signal_strength_percent",
    "homeassistant_sensor_unit_percent",
    "homeassistant_sensor_cpu_percent",
    "homeassistant_sensor_memory_percent",
    "homeassistant_sensor_disk_percent",
    "homeassistant_sensor_moisture_percent",
    "homeassistant_sensor_cloud_coverage_percent",
    "homeassistant_sensor_state",
    "homeassistant_sensor_unit_u0xb0",
    "homeassistant_sensor_unit_u0x20ac_per_kwh",
    "homeassistant_binary_sensor_state",
    "homeassistant_switch_state",
    "homeassistant_light_brightness_percent",
    "homeassistant_climate_current_temperature_celsius",
    "homeassistant_climate_mode",
    "homeassistant_cover_state",
    "homeassistant_lock_state",
    "homeassistant_device_tracker_state",
    "homeassistant_person_state",
    "homeassistant_alarm_control_panel_state",
    "homeassistant_update_state",
    "homeassistant_number_state",
    "homeassistant_input_number_state",
    # Add more as needed to reach 80+
    "homeassistant_sensor_power_kw",
    "homeassistant_sensor_energy_wh",
    "homeassistant_sensor_voltage_mv",
    "homeassistant_sensor_apparent_power_va",
    "homeassistant_sensor_pressure_hpa",
    "homeassistant_sensor_temperature_k",
    "homeassistant_sensor_temperature_mk",
    "homeassistant_sensor_duration_d",
    "homeassistant_sensor_unit_floors",
    "homeassistant_sensor_unit_steps",
    "homeassistant_sensor_unit_items",
    "homeassistant_sensor_unit_rpm",
    "homeassistant_sensor_unit_gco2eq_per_kwh",
    "homeassistant_sensor_energy_consumption_wh_per_km",
    "homeassistant_sensor_volume_m3",
    "homeassistant_sensor_volume_flow_rate_m3_per_h",
    "homeassistant_sensor_area_m2",
    "homeassistant_sensor_speed_m_per_s",
    "homeassistant_sensor_unit_percent_available",
    "homeassistant_number_state_a",
    "homeassistant_number_state_w",
    "homeassistant_number_state_celsius",
    "homeassistant_number_state_percent",
    "homeassistant_number_state_kwh",
    "homeassistant_number_state_h",
    "homeassistant_number_state_s",
    "homeassistant_input_number_state_a",
    "homeassistant_input_number_state_w",
    # Padding to reach 80+
    "homeassistant_climate_action",
    "homeassistant_climate_fan_mode",
    "homeassistant_climate_preset_mode",
    "homeassistant_climate_target_temperature_celsius",
    "homeassistant_cover_position",
    "homeassistant_entity_available",
    "homeassistant_last_updated_time_seconds",
    "homeassistant_state_change_total",
    "homeassistant_state_change_created",
    "homeassistant_automation_triggered_count_total",
    "homeassistant_automation_triggered_count_created",
    "homeassistant_switch_attr_brightness_pct",
    "homeassistant_switch_attr_color_temp_kelvin",
}


@pytest.fixture(autouse=True)
def mock_vm_metrics_fetch():
    """Auto-use fixture to mock fetch_vm_metrics for all tests."""
    with patch('mapping.fetch_vm_metrics', return_value=MOCK_VM_METRICS):
        # Reset the cached metrics so load_schema re-fetches
        import mapping
        mapping._KNOWN_VM_METRICS = None
        mapping._SCHEMA_MAPPING = None
        yield


@pytest.fixture
def sample_influx_point():
    """Fixture for a sample InfluxDB data point."""
    return InfluxDataPoint(
        timestamp=datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc),
        domain="sensor",
        entity_id="temperature_living_room",
        friendly_name="Living Room Temperature",
        measurement="°C",
        value=21.5
    )


@pytest.fixture
def sample_vm_point():
    """Fixture for a sample VictoriaMetrics data point."""
    return VMDataPoint(
        metric_name="homeassistant_sensor_temperature_celsius",
        labels={
            "entity": "sensor.temperature_living_room",
            "domain": "sensor",
            "friendly_name": "Living Room Temperature",
            "job": "influxdb-migration",
            "instance": "influxdb-migration"
        },
        value=21.5,
        timestamp_ms=1732968000000
    )


@pytest.fixture
def batch_influx_points():
    """Fixture for a batch of InfluxDB data points."""
    return [
        InfluxDataPoint(
            timestamp=datetime(2025, 11, 30, 12, 0, 0, tzinfo=timezone.utc),
            domain="sensor",
            entity_id="temperature_living_room",
            friendly_name="Living Room Temperature",
            measurement="°C",
            value=21.5
        ),
        InfluxDataPoint(
            timestamp=datetime(2025, 11, 30, 12, 1, 0, tzinfo=timezone.utc),
            domain="sensor",
            entity_id="humidity_bathroom",
            friendly_name="Bathroom Humidity",
            measurement="%",
            value=65.3
        ),
        InfluxDataPoint(
            timestamp=datetime(2025, 11, 30, 12, 2, 0, tzinfo=timezone.utc),
            domain="binary_sensor",
            entity_id="motion_hallway",
            friendly_name="Hallway Motion",
            measurement="units",
            value=1.0
        ),
    ]


@pytest.fixture
def batch_vm_points():
    """Fixture for a batch of VictoriaMetrics data points."""
    return [
        VMDataPoint(
            metric_name="homeassistant_sensor_temperature_celsius",
            labels={
                "entity": "sensor.temperature_living_room",
                "domain": "sensor",
                "friendly_name": "Living Room Temperature",
                "job": "influxdb-migration",
                "instance": "influxdb-migration"
            },
            value=21.5,
            timestamp_ms=1732968000000
        ),
        VMDataPoint(
            metric_name="homeassistant_sensor_humidity_percent",
            labels={
                "entity": "sensor.humidity_bathroom",
                "domain": "sensor",
                "friendly_name": "Bathroom Humidity",
                "job": "influxdb-migration",
                "instance": "influxdb-migration"
            },
            value=65.3,
            timestamp_ms=1732968060000
        ),
        VMDataPoint(
            metric_name="homeassistant_binary_sensor_state",
            labels={
                "entity": "binary_sensor.motion_hallway",
                "domain": "binary_sensor",
                "friendly_name": "Hallway Motion",
                "job": "influxdb-migration",
                "instance": "influxdb-migration"
            },
            value=1.0,
            timestamp_ms=1732968120000
        ),
    ]


@pytest.fixture
def mock_flux_record():
    """Fixture for creating mock InfluxDB FluxRecord objects."""
    def _create_record(timestamp, domain, entity_id, friendly_name, measurement, value):
        """
        Create a mock FluxRecord with necessary attributes.

        Args:
            timestamp: datetime object
            domain: HomeAssistant domain
            entity_id: Entity ID
            friendly_name: Friendly name
            measurement: Unit/measurement
            value: Numeric value

        Returns:
            Mock FluxRecord object
        """
        record = FluxRecord(0)
        record.values = {
            "_time": timestamp,
            "_measurement": measurement,
            "_value": value,
            "domain": domain,
            "entity_id": entity_id,
            "friendly_name": friendly_name,
            "_field": "value"
        }
        return record

    return _create_record


@pytest.fixture
def mock_flux_table(mock_flux_record):
    """Fixture for creating mock InfluxDB FluxTable objects."""
    def _create_table(records_data):
        """
        Create a mock FluxTable with FluxRecord objects.

        Args:
            records_data: List of tuples (timestamp, domain, entity_id, friendly_name, measurement, value)

        Returns:
            Mock FluxTable object
        """
        table = FluxTable()
        table.records = [mock_flux_record(*data) for data in records_data]
        return table

    return _create_table

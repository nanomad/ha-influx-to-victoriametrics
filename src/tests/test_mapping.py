"""
Tests for the mapping module.

This module tests the InfluxDB to VictoriaMetrics metric name mapping logic,
including schema loading, metric name generation, label building, and validation.
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path so we can import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import mapping


class TestSchemaLoading:
    """Tests for YAML schema loading functionality."""

    def test_yaml_schema_loads_successfully(self):
        """Test that the YAML schema file loads without errors."""
        schema = mapping.load_schema()
        assert schema is not None
        assert isinstance(schema, dict)

    def test_missing_yaml_file_raises_error(self):
        """Test that loading a non-existent YAML file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError) as exc_info:
            mapping.load_schema("/nonexistent/path/to/schema.yaml")
        assert "not found" in str(exc_info.value).lower()

    def test_known_metrics_is_set(self):
        """Test that KNOWN_VM_METRICS is loaded as a set."""
        known_metrics = mapping._get_known_metrics()
        assert isinstance(known_metrics, set)

    def test_known_metrics_count(self):
        """Test that KNOWN_VM_METRICS contains a reasonable number of metrics."""
        known_metrics = mapping._get_known_metrics()
        # Should have at least 80 metrics (this is a sanity check, not exact)
        assert len(known_metrics) >= 80

    def test_known_metrics_contains_key_metrics(self):
        """Test that KNOWN_VM_METRICS contains essential metric names."""
        known_metrics = mapping._get_known_metrics()
        key_metrics = [
            "homeassistant_sensor_temperature_celsius",
            "homeassistant_sensor_battery_percent",
            "homeassistant_binary_sensor_state",
            "homeassistant_switch_state"
        ]
        assert all(metric in known_metrics for metric in key_metrics)


class TestGetVMMetricName:
    """Tests for get_vm_metric_name() function."""

    def test_temperature_sensor(self):
        """Test temperature sensor mapping (°C)."""
        result = mapping.get_vm_metric_name("sensor", "°C", "temperature_living_room")
        assert result == "homeassistant_sensor_temperature_celsius"

    def test_power_sensor(self):
        """Test power sensor mapping (W)."""
        result = mapping.get_vm_metric_name("sensor", "W", "power_consumption")
        assert result == "homeassistant_sensor_power_w"

    def test_energy_sensor(self):
        """Test energy sensor mapping (kWh)."""
        result = mapping.get_vm_metric_name("sensor", "kWh", "total_energy")
        assert result == "homeassistant_sensor_energy_kwh"

    def test_binary_sensor(self):
        """Test binary sensor mapping."""
        result = mapping.get_vm_metric_name("binary_sensor", "units", "motion_detected")
        assert result == "homeassistant_binary_sensor_state"

    def test_switch(self):
        """Test switch state mapping."""
        result = mapping.get_vm_metric_name("switch", "units", "living_room_light")
        assert result == "homeassistant_switch_state"

    def test_signal_strength(self):
        """Test signal strength mapping (dBm)."""
        result = mapping.get_vm_metric_name("sensor", "dBm", "wifi_signal")
        assert result == "homeassistant_sensor_signal_strength_dbm"

    def test_distance(self):
        """Test distance mapping (km)."""
        result = mapping.get_vm_metric_name("sensor", "km", "distance_traveled")
        assert result == "homeassistant_sensor_distance_km"

    def test_unknown_unit_fallback(self):
        """Test fallback for unknown unit."""
        result = mapping.get_vm_metric_name("sensor", "unknown_unit", "test_sensor")
        assert result == "homeassistant_sensor_state"

    def test_unknown_domain_fallback(self):
        """Test fallback for unknown domain."""
        result = mapping.get_vm_metric_name("unknown_domain", "units", "test_entity")
        assert result == "homeassistant_unknown_domain_state"


class TestSpecialPercentHandling:
    """Tests for special % unit handling based on entity_id patterns."""

    def test_battery_percentage(self):
        """Test battery percentage mapping."""
        result = mapping.get_vm_metric_name("sensor", "%", "battery_level")
        assert result == "homeassistant_sensor_battery_percent"

    def test_humidity_percentage(self):
        """Test humidity percentage mapping."""
        result = mapping.get_vm_metric_name("sensor", "%", "humidity_sensor")
        assert result == "homeassistant_sensor_humidity_percent"

    def test_moisture_percentage(self):
        """Test moisture percentage mapping."""
        result = mapping.get_vm_metric_name("sensor", "%", "soil_moisture")
        assert result == "homeassistant_sensor_moisture_percent"

    def test_cpu_percentage(self):
        """Test CPU percentage mapping."""
        result = mapping.get_vm_metric_name("sensor", "%", "cpu_usage")
        assert result == "homeassistant_sensor_cpu_percent"

    def test_generic_percentage(self):
        """Test generic percentage mapping (no specific pattern)."""
        result = mapping.get_vm_metric_name("sensor", "%", "completion_rate")
        assert result == "homeassistant_sensor_unit_percent"

    @pytest.mark.parametrize("entity_id,expected", [
        ("battery_level", "homeassistant_sensor_battery_percent"),
        ("humidity_room", "homeassistant_sensor_humidity_percent"),
        ("generic_percent", "homeassistant_sensor_unit_percent"),
        ("cpu_load", "homeassistant_sensor_cpu_percent"),
        ("soil_moisture_plant", "homeassistant_sensor_moisture_percent"),
    ])
    def test_percent_special_handling_parametrized(self, entity_id, expected):
        """Test percent handling with various entity_id patterns."""
        result = mapping.get_vm_metric_name("sensor", "%", entity_id)
        assert result == expected


class TestBuildVMLabels:
    """Tests for build_vm_labels() function."""

    def test_basic_label_building(self):
        """Test basic label building with standard inputs."""
        result = mapping.build_vm_labels("sensor", "temp_room", "Room Temp")
        expected = {
            'entity': 'sensor.temp_room',
            'domain': 'sensor',
            'friendly_name': 'Room Temp',
            'job': 'influxdb-migration',
            'instance': 'home-assistant.node-red.svc.cluster.local:8123'
        }
        assert result == expected

    def test_label_building_binary_sensor(self):
        """Test label building for binary_sensor domain."""
        result = mapping.build_vm_labels("binary_sensor", "motion_hall", "Hall Motion")
        expected = {
            'entity': 'binary_sensor.motion_hall',
            'domain': 'binary_sensor',
            'friendly_name': 'Hall Motion',
            'job': 'influxdb-migration',
            'instance': 'home-assistant.node-red.svc.cluster.local:8123'
        }
        assert result == expected

    def test_entity_has_domain_prefix(self):
        """Test that entity label has domain prefix."""
        result = mapping.build_vm_labels("switch", "living_light", "Living Room Light")
        assert result.get('entity') == "switch.living_light"

    def test_all_required_labels_present(self):
        """Test that all required labels are present."""
        result = mapping.build_vm_labels("sensor", "test", "Test Sensor")
        required_labels = {'entity', 'domain', 'friendly_name', 'job', 'instance'}
        assert all(label in result for label in required_labels)


class TestValidateMetricName:
    """Tests for validate_metric_name() function."""

    def test_validate_known_temperature_metric(self):
        """Test validation of known temperature metric."""
        result = mapping.validate_metric_name("homeassistant_sensor_temperature_celsius")
        assert result is True

    def test_validate_known_binary_sensor_metric(self):
        """Test validation of known binary sensor metric."""
        result = mapping.validate_metric_name("homeassistant_binary_sensor_state")
        assert result is True

    def test_validate_unknown_metric(self):
        """Test validation of unknown metric returns False."""
        result = mapping.validate_metric_name("homeassistant_sensor_unknown_metric")
        assert result is False

    def test_validate_invalid_metric_name(self):
        """Test validation of completely invalid metric name."""
        result = mapping.validate_metric_name("not_a_homeassistant_metric")
        assert result is False


class TestGetVMMetricNameStrict:
    """Tests for get_vm_metric_name_strict() function."""

    def test_strict_validation_temperature_sensor(self):
        """Test strict validation with known temperature metric."""
        result = mapping.get_vm_metric_name_strict("sensor", "°C", "temp_room")
        assert result == "homeassistant_sensor_temperature_celsius"

    def test_strict_validation_binary_sensor(self):
        """Test strict validation with known binary sensor metric."""
        result = mapping.get_vm_metric_name_strict("binary_sensor", "units", "motion")
        assert result == "homeassistant_binary_sensor_state"

    def test_strict_validation_power_sensor(self):
        """Test strict validation with known power metric."""
        result = mapping.get_vm_metric_name_strict("sensor", "W", "power_usage")
        assert result == "homeassistant_sensor_power_w"

    def test_strict_validation_raises_for_unknown_metric(self):
        """Test that strict validation raises ValueError for unknown metric."""
        with pytest.raises(ValueError) as exc_info:
            mapping.get_vm_metric_name_strict("totally_unknown_domain", "units", "test_sensor")

        error_msg = str(exc_info.value)
        assert "homeassistant_totally_unknown_domain_state" in error_msg
        assert "domain='totally_unknown_domain'" in error_msg
        assert "measurement='units'" in error_msg
        assert "entity_id='test_sensor'" in error_msg

    def test_strict_validation_raises_with_correct_message(self):
        """Test that strict validation error contains all relevant details."""
        with pytest.raises(ValueError) as exc_info:
            mapping.get_vm_metric_name_strict("fake_domain_xyz", "units", "entity")

        assert "fake_domain_xyz" in str(exc_info.value)


class TestDryRunValidate:
    """Tests for dry_run_validate() function."""

    def test_dry_run_all_valid_records(self):
        """Test dry run validation with all valid records."""
        valid_records = [
            ("sensor", "°C", "temp_living"),
            ("sensor", "W", "power_usage"),
            ("binary_sensor", "units", "motion_detected")
        ]
        success_count, errors = mapping.dry_run_validate(valid_records)
        assert success_count == 3
        assert len(errors) == 0

    def test_dry_run_mixed_valid_invalid_records(self):
        """Test dry run validation with mixed valid/invalid records."""
        mixed_records = [
            ("sensor", "°C", "temp_room"),              # Valid
            ("totally_unknown_domain", "units", "bad1"), # Invalid
            ("binary_sensor", "units", "motion"),        # Valid
            ("fake_domain_xyz", "units", "entity"),      # Invalid
            ("sensor", "kWh", "energy")                  # Valid
        ]
        success_count, errors = mapping.dry_run_validate(mixed_records)
        assert success_count == 3
        assert len(errors) == 2

    def test_dry_run_all_invalid_records(self):
        """Test dry run validation with all invalid records."""
        invalid_records = [
            ("unknown_domain_a", "units", "bad1"),
            ("unknown_domain_b", "units", "bad2"),
            ("unknown_domain_c", "units", "bad3")
        ]
        success_count, errors = mapping.dry_run_validate(invalid_records)
        assert success_count == 0
        assert len(errors) == 3

    def test_dry_run_empty_list(self):
        """Test dry run validation with empty list."""
        empty_records = []
        success_count, errors = mapping.dry_run_validate(empty_records)
        assert success_count == 0
        assert len(errors) == 0

    def test_dry_run_error_messages_contain_details(self):
        """Test that dry run error messages contain useful information."""
        test_records = [("really_bad_domain", "units", "test_entity")]
        success_count, errors = mapping.dry_run_validate(test_records)

        assert len(errors) == 1
        error_msg = errors[0]
        assert "domain='really_bad_domain'" in error_msg
        assert "measurement='units'" in error_msg
        assert "entity_id='test_entity'" in error_msg

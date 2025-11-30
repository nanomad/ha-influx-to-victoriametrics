#!/usr/bin/env python3
"""
InfluxDB to VictoriaMetrics Metric Name Mapping Module

This module provides functions to map InfluxDB schema (domain, measurement, entity_id)
to VictoriaMetrics metric names and labels according to the SCHEMA_MAPPING.yaml rules.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Optional, List, Tuple, Set
import yaml
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Path to the YAML schema file - check multiple locations
def _find_schema_path() -> Path:
    """Find the schema file in possible locations."""
    candidates = [
        Path(__file__).parent / "SCHEMA_MAPPING.yaml",  # Same dir (container)
        Path(__file__).parent.parent / "SCHEMA_MAPPING.yaml",  # Parent dir (dev)
    ]
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]  # Default to first candidate for error message

SCHEMA_PATH = _find_schema_path()

# Global variables to hold loaded schema data
_SCHEMA_MAPPING = None
_KNOWN_VM_METRICS = None

# Default VM URL (can be overridden via env var or parameter)
DEFAULT_VM_URL = os.environ.get(
    "VM_URL",
    "http://victoria-metrics-victoria-metrics-single-server.victoria-metrics.svc.cluster.local:8428"
)


def fetch_vm_metrics(vm_url: str = DEFAULT_VM_URL) -> Set[str]:
    """
    Fetch all metric names from VictoriaMetrics that match homeassistant_* pattern.

    Args:
        vm_url: VictoriaMetrics server URL

    Returns:
        Set of metric names

    Raises:
        ConnectionError: If unable to connect to VictoriaMetrics
    """
    # Use label values API to get all metric names starting with homeassistant_
    api_url = f"{vm_url}/api/v1/label/__name__/values"
    params = {"match[]": "{__name__=~'homeassistant_.*'}"}

    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "success":
            raise ConnectionError(f"VM API returned error: {data.get('error', 'Unknown error')}")

        metrics = set(data.get("data", []))
        logger.info(f"Fetched {len(metrics)} homeassistant_* metrics from VictoriaMetrics")
        return metrics

    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Failed to fetch metrics from VictoriaMetrics: {e}")


def load_schema(path: Optional[str] = None, vm_url: Optional[str] = None) -> Dict:
    """
    Load the schema mapping from YAML file and fetch known metrics from VictoriaMetrics.

    Args:
        path: Optional path to the YAML schema file. If None, uses default SCHEMA_PATH.
        vm_url: Optional VictoriaMetrics URL to fetch known metrics from.
                If None, uses VM_URL env var or default.

    Returns:
        Parsed YAML schema as a dictionary

    Raises:
        FileNotFoundError: If the YAML file does not exist
        yaml.YAMLError: If the YAML file is invalid
        ConnectionError: If unable to connect to VictoriaMetrics
    """
    global _SCHEMA_MAPPING, _KNOWN_VM_METRICS

    schema_file = Path(path) if path else SCHEMA_PATH

    if not schema_file.exists():
        raise FileNotFoundError(
            f"Schema mapping file not found: {schema_file}\n"
            f"Expected location: {schema_file.absolute()}"
        )

    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise yaml.YAMLError(
            f"Failed to parse YAML schema file {schema_file}: {e}"
        ) from e

    # Validate required sections exist
    required_sections = ['labels', 'metric_mappings', 'special_mappings']
    missing_sections = [s for s in required_sections if s not in schema]
    if missing_sections:
        raise ValueError(
            f"Invalid schema structure. Missing required sections: {missing_sections}"
        )

    # Cache the schema
    _SCHEMA_MAPPING = schema

    # Fetch known metrics from VictoriaMetrics (required - must be reachable)
    effective_vm_url = vm_url or DEFAULT_VM_URL
    _KNOWN_VM_METRICS = fetch_vm_metrics(effective_vm_url)

    logger.info(f"Loaded schema from {schema_file}")
    return schema


def _get_schema() -> Dict:
    """
    Get the loaded schema, loading it if necessary.

    Returns:
        The loaded schema dictionary
    """
    if _SCHEMA_MAPPING is None:
        load_schema()
    return _SCHEMA_MAPPING


def _get_known_metrics() -> set:
    """
    Get the set of known VM metrics, loading the schema if necessary.

    Returns:
        Set of known VM metric names
    """
    if _KNOWN_VM_METRICS is None:
        load_schema()
    return _KNOWN_VM_METRICS


def is_ignored(domain: str, measurement: str) -> bool:
    """
    Check if a domain/measurement combination should be ignored (skipped).

    Args:
        domain: HomeAssistant domain
        measurement: InfluxDB measurement (unit)

    Returns:
        True if this combination should be skipped, False otherwise
    """
    schema = _get_schema()
    domain_mappings = schema.get('metric_mappings', {}).get(domain, {})

    if measurement in domain_mappings:
        mapping_info = domain_mappings[measurement]
        return mapping_info.get('ignore', False)

    return False


# Sentinel value to indicate a record should be ignored
IGNORE_METRIC = "__IGNORE__"


def get_field_metric(domain: str, field: str) -> Optional[str]:
    """
    Get VM metric name for a specific (domain, field) combination from field_mappings.

    Args:
        domain: HomeAssistant domain (e.g., "climate", "cover")
        field: InfluxDB field name (e.g., "current_temperature", "brightness")

    Returns:
        VictoriaMetrics metric name if found in field_mappings, None otherwise
    """
    schema = _get_schema()
    field_mappings = schema.get('field_mappings', {}).get(domain, {})

    if field in field_mappings:
        mapping_info = field_mappings[field]
        if mapping_info.get('ignore', False):
            return IGNORE_METRIC
        return mapping_info.get('metric')

    return None


def get_vm_metric_name(
    domain: str,
    measurement: str,
    entity_id: str,
    field: str = "value"
) -> Optional[str]:
    """
    Maps InfluxDB domain, measurement (unit), entity_id, and field to a VictoriaMetrics metric name.

    Args:
        domain: HomeAssistant domain (e.g., "sensor", "binary_sensor")
        measurement: InfluxDB measurement (unit), e.g., "°C", "%", "units"
        entity_id: Entity ID without domain prefix (e.g., "temperature_living_room")
        field: InfluxDB field name (default: "value")

    Returns:
        VictoriaMetrics metric name (e.g., "homeassistant_sensor_temperature_celsius")
        None if the mapping is marked as ignored

    Example:
        >>> get_vm_metric_name("sensor", "°C", "temperature_living_room")
        'homeassistant_sensor_temperature_celsius'
        >>> get_vm_metric_name("climate", "units", "thermostat", field="current_temperature")
        'homeassistant_climate_current_temperature_celsius'
    """
    # For non-'value' fields, check field_mappings first
    if field != "value":
        field_metric = get_field_metric(domain, field)
        if field_metric == IGNORE_METRIC:
            return None
        if field_metric:
            return field_metric
        # If no field mapping found, log warning and skip
        logger.warning(
            f"No field mapping for domain='{domain}', field='{field}'. Skipping."
        )
        return None

    # For 'value' field, use the standard metric_mappings
    schema = _get_schema()

    # Get domain mappings
    domain_mappings = schema.get('metric_mappings', {}).get(domain, {})

    # Check if measurement has a direct mapping
    if measurement in domain_mappings:
        mapping_info = domain_mappings[measurement]

        # Check if this mapping should be ignored
        if mapping_info.get('ignore', False):
            return None

        metric = mapping_info.get('metric')

        # Check if special mapping is required (for ambiguous units like %)
        if mapping_info.get('special_mapping_required', False):
            # Handle special % mapping based on entity_id patterns
            special_metric = _apply_special_mapping(measurement, entity_id)
            if special_metric == IGNORE_METRIC:
                return None  # Signal to skip this record
            if special_metric:
                return special_metric

        if metric:
            return metric

    # Fallback: generate default metric name
    fallback_metric = f"homeassistant_{domain}_state"
    logger.warning(
        f"No mapping found for domain='{domain}', measurement='{measurement}', "
        f"entity_id='{entity_id}'. Using fallback: {fallback_metric}"
    )
    return fallback_metric


def is_new_metric_allowed(domain: str, measurement: str) -> bool:
    """
    Check if a domain/measurement combination is allowed to create new metrics.

    Args:
        domain: HomeAssistant domain
        measurement: InfluxDB measurement (unit)

    Returns:
        True if this combination has allow_new: true in schema, False otherwise
    """
    schema = _get_schema()
    domain_mappings = schema.get('metric_mappings', {}).get(domain, {})

    if measurement in domain_mappings:
        mapping_info = domain_mappings[measurement]
        return mapping_info.get('allow_new', False)

    return False


def _apply_special_mapping(measurement: str, entity_id: str) -> Optional[str]:
    """
    Apply special mapping rules for ambiguous units based on entity_id patterns.

    Args:
        measurement: The measurement/unit (e.g., "%")
        entity_id: Entity ID to check against patterns

    Returns:
        Metric name if pattern matches
        IGNORE_METRIC sentinel if pattern is marked as ignore
        None if no pattern matches
    """
    schema = _get_schema()
    special_mappings = schema.get('special_mappings', {}).get(measurement, {})
    rules = special_mappings.get('rules', [])

    # Convert entity_id to lowercase for case-insensitive matching
    entity_id_lower = entity_id.lower()

    for rule in rules:
        pattern = rule.get('pattern', '').lower()

        # Skip the default rule for now
        if pattern == 'default':
            continue

        # Check if pattern is in entity_id
        if pattern in entity_id_lower:
            # Check if this pattern is marked as ignore
            if rule.get('ignore', False):
                return IGNORE_METRIC
            return rule.get('metric')

    # Return default if no specific pattern matched
    for rule in rules:
        if rule.get('pattern', '').lower() == 'default':
            if rule.get('ignore', False):
                return IGNORE_METRIC
            return rule.get('metric')

    return None


def build_vm_labels(domain: str, entity_id: str, friendly_name: str) -> Dict[str, str]:
    """
    Build VictoriaMetrics labels for a given entity.

    Args:
        domain: HomeAssistant domain (e.g., "sensor", "binary_sensor")
        entity_id: Entity ID without domain prefix (e.g., "temp_room")
        friendly_name: Human-readable entity name (e.g., "Room Temp")

    Returns:
        Dictionary of labels for VictoriaMetrics

    Example:
        >>> build_vm_labels("sensor", "temp_room", "Room Temp")
        {'entity': 'sensor.temp_room', 'domain': 'sensor', 'friendly_name': 'Room Temp', 'job': 'influxdb-migration', 'instance': 'influxdb-migration'}
    """
    schema = _get_schema()
    labels = {}

    # Computed labels
    computed_labels = schema.get('labels', {}).get('computed', {})
    if 'entity' in computed_labels:
        # Build entity with domain prefix according to template
        template = computed_labels['entity'].get('template', '{domain}.{entity_id}')
        labels['entity'] = template.format(domain=domain, entity_id=entity_id)

    # Direct mappings
    labels['domain'] = domain
    labels['friendly_name'] = friendly_name

    # Static labels
    static_labels = schema.get('labels', {}).get('static', {})
    labels.update(static_labels)

    return labels


def validate_metric_name(metric_name: str) -> bool:
    """
    Validates whether a metric name exists in the known VM metrics list.

    Args:
        metric_name: The VictoriaMetrics metric name to validate

    Returns:
        True if metric_name is in KNOWN_VM_METRICS, False otherwise

    Example:
        >>> validate_metric_name("homeassistant_sensor_temperature_celsius")
        True
        >>> validate_metric_name("homeassistant_sensor_unknown_metric")
        False
    """
    known_metrics = _get_known_metrics()
    return metric_name in known_metrics


def get_vm_metric_name_strict(
    domain: str,
    measurement: str,
    entity_id: str,
    field: str = "value"
) -> Optional[str]:
    """
    Maps InfluxDB domain, measurement (unit), entity_id, and field to a VictoriaMetrics metric name
    with strict validation. Raises ValueError if the resulting metric is not in KNOWN_VM_METRICS.

    Args:
        domain: HomeAssistant domain (e.g., "sensor", "binary_sensor")
        measurement: InfluxDB measurement (unit), e.g., "°C", "%", "units"
        entity_id: Entity ID without domain prefix (e.g., "temperature_living_room")
        field: InfluxDB field name (default: "value")

    Returns:
        VictoriaMetrics metric name (e.g., "homeassistant_sensor_temperature_celsius")
        None if the mapping is marked as ignored (record should be skipped)

    Raises:
        ValueError: If the generated metric name is not in KNOWN_VM_METRICS

    Example:
        >>> get_vm_metric_name_strict("sensor", "°C", "temp_room")
        'homeassistant_sensor_temperature_celsius'
        >>> get_vm_metric_name_strict("climate", "units", "thermostat", field="current_temperature")
        'homeassistant_climate_current_temperature_celsius'
    """
    # Get the metric name using the standard function
    metric_name = get_vm_metric_name(domain, measurement, entity_id, field=field)

    # None means the mapping is ignored - skip this record
    if metric_name is None:
        return None

    # Validate it's in the known metrics list (unless allow_new is set)
    if not validate_metric_name(metric_name):
        # Check if this mapping allows creating new metrics
        if is_new_metric_allowed(domain, measurement):
            logger.info(f"Creating new metric '{metric_name}' (allow_new=true)")
            return metric_name
        raise ValueError(
            f"Unknown VM metric '{metric_name}' generated from "
            f"domain='{domain}', measurement='{measurement}', entity_id='{entity_id}', "
            f"field='{field}'. Not in known VM metrics list."
        )

    return metric_name


def dry_run_validate(records: List[Tuple[str, str, str]]) -> Tuple[int, List[str]]:
    """
    Validates a list of InfluxDB records against known VM metrics without raising exceptions.

    Args:
        records: List of (domain, measurement, entity_id) tuples to validate

    Returns:
        Tuple of (success_count, list_of_error_messages)
        - success_count: Number of records that mapped to known VM metrics
        - list_of_error_messages: List of error strings for unmapped records

    Example:
        >>> records = [
        ...     ("sensor", "°C", "temp_room"),
        ...     ("sensor", "unknown", "foo"),
        ...     ("binary_sensor", "units", "motion")
        ... ]
        >>> success_count, errors = dry_run_validate(records)
        >>> success_count
        2
        >>> len(errors)
        1
    """
    success_count = 0
    errors = []

    for domain, measurement, entity_id in records:
        try:
            # Try to get the metric name with strict validation
            get_vm_metric_name_strict(domain, measurement, entity_id)
            success_count += 1
        except ValueError as e:
            # Collect the error message
            error_msg = (
                f"Failed to map record: domain='{domain}', measurement='{measurement}', "
                f"entity_id='{entity_id}' - {str(e)}"
            )
            errors.append(error_msg)

    return success_count, errors

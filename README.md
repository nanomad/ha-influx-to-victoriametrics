# ha-influx-to-victoriametrics

Migrate Home Assistant historical data from InfluxDB to VictoriaMetrics.

## Installation

```bash
cd src
pip install -r requirements.txt
```

## Usage

```bash
# Dry-run (validation only, recommended first)
python migrate.py --dry-run

# Actual migration
python migrate.py

# Reset progress and start fresh
python migrate.py --reset

# Migrate specific domains
python migrate.py --domains climate,cover,light --extended-fields

# Custom date range
python migrate.py --start-date 2025-01-01 --end-date 2025-06-01
```

## CLI Options

```
python migrate.py [OPTIONS]

Options:
  --dry-run             Validate without writing data
  --reset               Reset progress and start fresh
  --domains DOMAINS     Comma-separated list of domains to migrate
  --extended-fields     Include extra fields (current_temperature, etc.)
  --start-date DATE     Start date (YYYY-MM-DD)
  --end-date DATE       End date (YYYY-MM-DD)
  --influx-url URL      InfluxDB server URL
  --influx-token TOKEN  InfluxDB auth token (or set INFLUX_TOKEN env var)
  --vm-url URL          VictoriaMetrics server URL
  --state-dir DIR       Directory for progress state
```

## Project Structure

```
├── SCHEMA_MAPPING.yaml    # Metric name mapping rules
└── src/
    ├── migrate.py         # Main orchestrator
    ├── mapping.py         # Schema transformation logic
    ├── influx_reader.py   # InfluxDB client
    ├── vm_writer.py       # VictoriaMetrics writer
    ├── progress.py        # Resumable progress tracking
    ├── requirements.txt   # Python dependencies
    └── tests/             # Unit tests
```

## Schema Mapping

The `SCHEMA_MAPPING.yaml` file defines how InfluxDB data transforms to VictoriaMetrics metrics:

- **Metric names**: `{domain} + {unit}` → `homeassistant_{domain}_{type}_{unit}`
- **Labels**: entity, domain, friendly_name, job, instance
- **Special handling**: Ambiguous units (%) use entity patterns to determine metric type

## Rollback

Delete migrated data without affecting new data:

```bash
curl -X POST 'http://victoria-metrics:8428/api/v1/admin/tsdb/delete_series' \
  -d 'match[]={job="influxdb-migration"}'
```

## License

MIT

# Zerobus PC Metrics Collector

Continuously samples system telemetry (CPU, memory, disk I/O, network, battery) every second and streams records to a Databricks Delta table via the [Zerobus Ingest SDK](https://docs.databricks.com/en/ingestion/zerobus/index.html) over gRPC.

## Prerequisites

- Python 3.8+
- A Databricks workspace with Unity Catalog
- A Databricks service principal with write access to the target table
- A Zerobus gRPC endpoint

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file in the project root:

```
DATABRICKS_CLIENT_ID=your-service-principal-client-id
DATABRICKS_CLIENT_SECRET=your-service-principal-client-secret
```

Edit the configuration constants at the top of `main.py`:

| Constant | Description |
|---|---|
| `SERVER_ENDPOINT` | Zerobus gRPC endpoint hostname |
| `WORKSPACE_URL` | Databricks workspace URL |
| `TABLE_NAME` | Unity Catalog target table (`catalog.schema.table`) |
| `COLLECTION_INTERVAL_SECONDS` | Sampling frequency (default: `1`) |

## Usage

```bash
python main.py
```

The script runs indefinitely, printing a `.` for each record ingested. Press `Ctrl+C` to stop — the gRPC stream is closed cleanly on exit.

## Metrics Collected

| Category | Fields |
|---|---|
| CPU | usage %, frequency (current/max), temperature, core/thread count |
| Memory | total/used/available GB, usage %, swap used/% |
| Disk I/O | cumulative read/write bytes and operation counts |
| Network | bytes sent/received, packets, errors, drops |
| Battery | charge %, plugged-in status, seconds remaining |
| System | process count, hostname, timestamp (microseconds) |

> **Note:** CPU temperature is always `None` on Windows — `psutil` does not expose sensor data on that platform.

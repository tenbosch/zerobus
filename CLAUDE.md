# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

A PC metrics collector that continuously samples system telemetry (CPU, memory, disk I/O, network, battery) every second and streams records to a Databricks Delta table via the Zerobus Ingest SDK over gRPC.

## Running the Collector

```bash
# Install dependencies
pip install -r requirements.txt

# Run (requires .env with credentials — see below)
python main.py
```

## Configuration

All configuration is at the top of `main.py`:

- `SERVER_ENDPOINT` — Zerobus gRPC endpoint
- `WORKSPACE_URL` — Databricks workspace URL
- `TABLE_NAME` — Unity Catalog target table (`catalog.schema.table`)
- `COLLECTION_INTERVAL_SECONDS` — sampling frequency (default: 1s)

Credentials are loaded from `.env`:

```
DATABRICKS_CLIENT_ID=your-service-principal-client-id
DATABRICKS_CLIENT_SECRET=your-service-principal-client-secret
```

## Architecture

`main.py` is the entire application — a single-file script with three concerns:

1. **Metrics collection** (`collect_metrics()`) — uses `psutil` to snapshot CPU, memory, disk I/O, network counters, battery, and process count. CPU temperature is read from platform-specific sensor keys; it is always `None` on Windows.

2. **Zerobus SDK setup** — `ZerobusSdk` authenticates via service principal (CLIENT_ID/SECRET), then `create_stream()` opens a persistent gRPC stream to the target Delta table with `RecordType.JSON`.

3. **Ingest loop** (`main()`) — calls `collect_metrics()` → `stream.ingest_record(metrics)` → sleeps 1s, indefinitely. ACK waiting is currently commented out.

## Key SDK Imports

```python
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
```

The sync client blocks on `ingest_record()`; an async variant exists in `zerobus.sdk.async_`.

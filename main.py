"""
PC Environmental Metrics → Databricks Zerobus Ingest
Collects system metrics and streams them via the official Zerobus Python SDK.

Install dependencies:
    pip install psutil databricks-zerobus-ingest-sdk python-dotenv

Create a .env file in the same directory with:
    DATABRICKS_CLIENT_ID=your-service-principal-client-id
    DATABRICKS_CLIENT_SECRET=your-service-principal-client-secret
"""

import datetime
import json
import os
import socket
import time

import psutil
from dotenv import load_dotenv
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

# ─── Load configuration from .env ─────────────────────────────────────────────
load_dotenv()
CLIENT_ID     = os.getenv("DATABRICKS_CLIENT_ID")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")
SERVER_ENDPOINT = os.getenv("ZEROBUS_SERVER_ENDPOINT")
WORKSPACE_URL   = os.getenv("DATABRICKS_WORKSPACE_URL")
TABLE_NAME      = os.getenv("DATABRICKS_TABLE_NAME")
COLLECTION_INTERVAL_SECONDS = int(os.getenv("COLLECTION_INTERVAL_SECONDS", "1"))

missing = [k for k, v in {
    "DATABRICKS_CLIENT_ID": CLIENT_ID,
    "DATABRICKS_CLIENT_SECRET": CLIENT_SECRET,
    "ZEROBUS_SERVER_ENDPOINT": SERVER_ENDPOINT,
    "DATABRICKS_WORKSPACE_URL": WORKSPACE_URL,
    "DATABRICKS_TABLE_NAME": TABLE_NAME,
}.items() if not v]
if missing:
    raise ValueError(f"Missing required .env variables: {', '.join(missing)}")
# ──────────────────────────────────────────────────────────────────────────────

HOSTNAME = socket.gethostname()


def collect_metrics() -> dict:
    """Collect environmental metrics from the local machine."""
    cpu_freq = psutil.cpu_freq()
    mem      = psutil.virtual_memory()
    swap     = psutil.swap_memory()
    disk_io  = psutil.disk_io_counters()
    net_io   = psutil.net_io_counters()
    battery  = psutil.sensors_battery()

    # CPU temperatures (platform-dependent; None if unavailable)
    cpu_temp = None
    try:
        temps = psutil.sensors_temperatures()
        if "coretemp" in temps:       # Intel Linux
            cpu_temp = temps["coretemp"][0].current
        elif "cpu_thermal" in temps:  # Raspberry Pi / some Linux
            cpu_temp = temps["cpu_thermal"][0].current
        elif "k10temp" in temps:      # AMD Linux
            cpu_temp = temps["k10temp"][0].current
    except AttributeError:
        pass  # Windows: psutil doesn't expose sensor temps
    
    cur_datetime = int(datetime.datetime.now().timestamp()*1000000)
    return {
        # Identity
        "timestamp":            cur_datetime,
        "hostname":             HOSTNAME,

        # CPU
        "cpu_percent_total":    psutil.cpu_percent(interval=1),
        "cpu_freq_current_mhz": cpu_freq.current if cpu_freq else None,
        "cpu_freq_max_mhz":     cpu_freq.max     if cpu_freq else None,
        "cpu_temp_celsius":     cpu_temp,
        "cpu_core_count":       psutil.cpu_count(logical=False),
        "cpu_thread_count":     psutil.cpu_count(logical=True),

        # Memory
        "mem_total_gb":         round(mem.total     / 1e9, 2),
        "mem_used_gb":          round(mem.used      / 1e9, 2),
        "mem_available_gb":     round(mem.available / 1e9, 2),
        "mem_percent":          mem.percent,
        "swap_used_gb":         round(swap.used     / 1e9, 2),
        "swap_percent":         swap.percent,

        # Disk I/O
        "disk_read_mb":         round(disk_io.read_bytes  / 1e9, 2) if disk_io else None,
        "disk_write_mb":        round(disk_io.write_bytes / 1e9, 2) if disk_io else None,
        "disk_read_count":      disk_io.read_count        if disk_io else None,
        "disk_write_count":     disk_io.write_count       if disk_io else None,

        # Network
        "net_bytes_sent_mb":    round(net_io.bytes_sent / 1e6, 2),
        "net_bytes_recv_mb":    round(net_io.bytes_recv / 1e6, 2),
        "net_packets_sent":     net_io.packets_sent,
        "net_packets_recv":     net_io.packets_recv,
        "net_errin":            net_io.errin,
        "net_errout":           net_io.errout,
        "net_dropin":           net_io.dropin,
        "net_dropout":          net_io.dropout,

        # Battery (None on desktops)
        "battery_percent":      battery.percent       if battery else None,
        "battery_plugged_in":   battery.power_plugged if battery else None,
        "battery_secs_left":    battery.secsleft      if battery else None,

        # System load
        "process_count":        len(psutil.pids()),
    }


def main():
    print(f"Starting PC metrics collection for host: {HOSTNAME}")
    print(f"Sending to: {TABLE_NAME} every {COLLECTION_INTERVAL_SECONDS}s\n")

    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
    table_properties = TableProperties(TABLE_NAME)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)

    stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)

    try:
        while True:
            metrics = collect_metrics()

            ack = stream.ingest_record(metrics)
            #ack.wait_for_ack()
            print(".", end="", flush=True)  # Print a dot for each successful ingest
            time.sleep(COLLECTION_INTERVAL_SECONDS)
    finally:
        stream.close()
        print("Stream closed.")


if __name__ == "__main__":
    main()
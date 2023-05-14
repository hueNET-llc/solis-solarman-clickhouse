# solis-solarman-clickhouse #
A Solis Solarman Data Logging Stick exporter for ClickHouse using pysolarmanv5

Configuration is done via environment variables and targets.json

## Compatibility ##
⚠️ This exporter is NOT compatible with the latest non-Solarman logging sticks, as they do not expose a local Modbus TCP server.

An official FAQ of how to differentiate between Solarman vs. Solis logging sticks can be found [here](https://usservice.solisinverters.com/support/solutions/articles/73000593957-solis-data-loggers-explained).

This exporter has been verified to work with a "WIFI(EN2-8M)" logging stick and single-phase, 3-MPPT "Solis 4G Series B2" inverter.

## Environment Variables ##
```
=== Exporter ===
DATA_QUEUE_LIMIT    -   ClickHouse insert queue max size (default: "50")
FETCH_INTERVAL      -   Default Modbus fetch interval in seconds (default: "30")
FETCH_TIMEOUT       -   Default Modbus fetch timeout in seconds (default: "60")
LOG_LEVEL           -   Logging verbosity (default: "20"), levels: 10 (debug) / 20 (info) / 30 (warning) / 40 (error) / 50 (critical)

=== ClickHouse ===
CLICKHOUSE_URL      -   ClickHouse URL (i.e. "http://192.168.0.69:8123")
CLICKHOUSE_USER     -   ClickHouse login username
CLICKHOUSE_PASS     -   ClickHouse login password
CLICKHOUSE_DB       -   ClickHouse database
CLICKHOUSE_TABLE    -   ClickHouse table to insert to (default: "apc_ups")
```

## targets.json ##

Used to configure logging stick targets. Example config in `targets.example.json`

```
[
    {
        name: Inverter name (string),
        ip: Logging stick IP (string),
        port: Logging stick Modbus TCP port (int, optional, default: "8999"),
        serial_number: Logging stick serial number (int),
        mb_slave_id: Inverter Modbus slave ID (int, optional, default: "1"),
        interval: Modbus fetch interval in seconds (int, optional, default: "30")
        timeout: Modbus timeout duration in seconds (int, optional, default: "60"),
        error_correction: Enable Solarman V5 error correction (bool, optional, default: "False")
    }
]
```
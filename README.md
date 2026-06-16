# Raw Byte Meter

Raw Byte Meter is a Kafka consumer service that estimates how much uncompressed serialized payload data is entering Kafka topics.

For each consumed record, it counts:

```text
key bytes + value bytes + header key/value bytes
```

It publishes JSON summaries to a Kafka `summary.topic`.

This measures uncompressed Kafka payload bytes. It does not measure compressed broker/network bytes, Kafka protocol overhead, broker disk usage, replication storage, or deserialized object size.

## Files

```text
raw-bytes-meter-1.0.0.jar     runnable fat jar
config.properties             runtime config
raw-byte-meter.service         optional systemd service
README.md                     this file
```

## Build

From the repo:

```bash
mvn clean package
```

Deploy this jar:

```text
target/raw-bytes-meter-1.0.0.jar
```

Do not deploy `target/original-raw-bytes-meter-1.0.0.jar`; that is the thin jar without bundled dependencies.

## Config

Start from:

```bash
cp config.example.properties config.properties
```

Typical config:

```properties
bootstrap.servers=broker1:9092,broker2:9092
application.id=raw-byte-meter-prod

topics.mode=discover
source.topics=
exclude.topics=raw-byte-summary

summary.topic=raw-byte-summary
summary.interval.seconds=60
hourly.enabled=true

auto.offset.reset=latest
log.interval.seconds=30
commit.interval.seconds=30
```

Important settings:

- `bootstrap.servers`: Kafka brokers.
- `application.id`: consumer group id. Keep stable in production.
- `topics.mode=list`: use explicit `source.topics`.
- `topics.mode=discover`: discover non-internal topics at startup.
- `source.topics`: comma-separated topics when using list mode.
- `exclude.topics`: topics to skip during discovery. Always include `summary.topic`.
- `summary.topic`: topic where JSON metrics are written.
- `summary.interval.seconds`: short reporting window.
- `hourly.enabled`: also emit hourly rollups.
- `auto.offset.reset=latest`: measure new data only for a fresh group.
- `auto.offset.reset=earliest`: replay retained data for a fresh group.
- `commit.interval.seconds`: how often offsets are committed.

Discovery excludes topics beginning with `_` automatically. `exclude.topics` also supports exact names and trailing `*` prefix patterns:

```properties
exclude.topics=raw-byte-summary,connect-*,_*
```

Security properties are passed through to Kafka clients:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="USERNAME" password="PASSWORD";
ssl.truststore.location=/etc/raw-byte-meter/client.truststore.jks
ssl.truststore.password=changeit
```

## Run Manually

Preview discovered topics:

```bash
java -jar raw-bytes-meter-1.0.0.jar discover --config config.properties
```

Run the meter:

```bash
java -jar raw-bytes-meter-1.0.0.jar measure --config config.properties
```

Expected logs:

```text
raw-byte-meter measure starting: group=raw-byte-meter-prod summary.topic=raw-byte-summary
topics=25 interval=60s hourly=true auto.offset.reset=latest commit.interval=30s
status records=123456 total_bytes=987654321 open_interval_windows=1 open_hourly_windows=1 assignments=40
summary produced type=interval source=orders bytes=992310 records=1234 partition=0 offset=42
```

## Summary Output

The app writes one JSON record per topic per completed window.

Example:

```json
{
  "window_type": "interval",
  "window_start": "2026-06-10T18:30:00Z",
  "window_end": "2026-06-10T18:31:00Z",
  "topic": "orders",
  "record_count": 1234,
  "key_bytes": 1200,
  "value_bytes": 987654,
  "header_bytes": 3456,
  "total_bytes": 992310
}
```

`window_type` is either:

- `interval`: controlled by `summary.interval.seconds`
- `hourly`: one-hour rollup when `hourly.enabled=true`

Message key format:

```text
<window_type>:<window_start>:<topic>
```

## Assembler

The meter writes one JSON record per topic per window to `summary.topic`. The `assemble` mode compiles all per-topic summaries for a given window into a single JSON report and writes it to `reports.topic`.

Run it:

```bash
java -jar raw-bytes-meter-1.0.0.jar assemble --config config.properties
```

A window is flushed and emitted once no new summary record for it has arrived for `assemble.idle.seconds`. Since the meter writes every topic for a window in one batch, a short idle window (default 5s) is normally enough to catch every topic before emitting.

Example report:

```json
{
  "window_type": "interval",
  "window_start": "2026-06-10T18:00:00Z",
  "window_end": "2026-06-10T19:00:00Z",
  "topics": [
    {
      "topic": "topicA",
      "record_count": 1234,
      "key_bytes": 1200,
      "value_bytes": 987654,
      "header_bytes": 3456,
      "total_bytes": 992310
    },
    {
      "topic": "topicB",
      "record_count": 5678,
      "key_bytes": 1000,
      "value_bytes": 987854,
      "header_bytes": 3956,
      "total_bytes": 992810
    }
  ]
}
```

Message key format: `<window_type>:<window_start>`.

Run `assemble` as a separate, long-running process from `measure`, with its own consumer group (`assembler.application.id`) so it doesn't interfere with the meter's offsets. Create `reports.topic` before starting unless broker auto-create is enabled.

## systemd Deployment

Recommended layout:

```text
/opt/raw-byte-meter/
  raw-bytes-meter-1.0.0.jar
  config.properties

/etc/systemd/system/raw-byte-meter.service
```

Create service user and install files:

```bash
sudo useradd --system --home /opt/raw-byte-meter --shell /sbin/nologin raw-byte-meter
sudo mkdir -p /opt/raw-byte-meter
sudo cp raw-bytes-meter-1.0.0.jar config.properties /opt/raw-byte-meter/
sudo chown -R raw-byte-meter:raw-byte-meter /opt/raw-byte-meter
sudo chmod 750 /opt/raw-byte-meter
sudo chmod 640 /opt/raw-byte-meter/config.properties
```

Install the service:

```bash
sudo cp raw-byte-meter.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now raw-byte-meter
```

Watch logs:

```bash
sudo journalctl -u raw-byte-meter -f
```

Restart after config or jar changes:

```bash
sudo systemctl restart raw-byte-meter
```

Example service:

```ini
[Unit]
Description=Raw Byte Meter Kafka payload metering service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=raw-byte-meter
Group=raw-byte-meter
WorkingDirectory=/opt/raw-byte-meter
ExecStart=/usr/bin/java -jar /opt/raw-byte-meter/raw-bytes-meter-1.0.0.jar measure --config /opt/raw-byte-meter/config.properties
Restart=always
RestartSec=15
SuccessExitStatus=143
TimeoutStopSec=45
StandardOutput=journal
StandardError=journal
SyslogIdentifier=raw-byte-meter

[Install]
WantedBy=multi-user.target
```

The repo also includes `systemd/raw-byte-meter.service`.

## Kafka Permissions

The service principal needs:

- read/describe on source topics
- write/describe on `summary.topic`
- permission to use the consumer group named by `application.id`
- topic-list/describe permission if using `topics.mode=discover`

Create `summary.topic` before starting the service unless broker auto-create is enabled.

## Troubleshooting

`UNKNOWN_TOPIC_OR_PARTITION`

The configured source topics do not exist or the service cannot see them. Run:

```bash
java -jar raw-bytes-meter-1.0.0.jar discover --config config.properties
```

Then use real topic names or set:

```properties
topics.mode=discover
```

No summary records

Check that:

- source topics have new traffic
- `summary.topic` exists
- the service can read source topics
- the service can write `summary.topic`
- `assignments` in logs is greater than zero
- `summary.interval.seconds` is short enough for testing

Offset commit warnings

Example:

```text
WARN offset commit failed 3 consecutive time(s); will retry: ...
```

Counting continues while the service is running. If the service restarts before offsets commit successfully, it may reread some records and overcount that period.

Check consumer group ACLs, broker/coordinator health, and network stability. You can raise:

```properties
commit.interval.seconds=60
```

Service will not start

Check:

```bash
sudo journalctl -u raw-byte-meter -n 100
```

Common causes are wrong jar path, wrong config path, Java missing, invalid config, or Kafka auth failure.

High lag

If logs show increasing `consumer_lag_records`, one instance may not be keeping up. Start with one instance for simplest totals. Multiple instances with the same `application.id` split partitions, but downstream consumers may need to aggregate multiple summary records for the same topic/window.

## Notes

- Keep `application.id` stable in production.
- Use `auto.offset.reset=latest` for live metering.
- Use a fresh `application.id` with `earliest` only for intentional replay/backfill.
- Always exclude the summary topic from discovery.
- The service can run on any host that can reach Kafka; it does not need to run on a broker.

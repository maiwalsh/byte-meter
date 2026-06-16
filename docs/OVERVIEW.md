# Raw Byte Meter — Overview

This document is a plain-language introduction to **Raw Byte Meter** for people who haven't used it before. For full technical/deployment details, see [`README.md`](../README.md) in the repo root.

## What is it?

Raw Byte Meter is a small monitoring service that watches data flowing through our **Kafka** topics and measures how much raw (uncompressed) data is moving through each one.

Think of it as a "data usage meter" for Kafka — similar to how a utility meter tracks electricity or water usage, this service tracks bytes of data passing through each topic over time.

## Why does it exist?

Kafka doesn't give an easy, ongoing answer to "how much data did topic X carry in the last hour/day?" Raw Byte Meter fills that gap by:

- Counting the bytes of every message (key + value + headers) as it passes through
- Summarizing those counts into regular time windows (default: every 60 seconds)
- Optionally rolling those up into hourly totals
- Publishing the results as simple JSON records to a dedicated Kafka topic, so dashboards, alerts, or billing/chargeback processes can consume them

**Important caveat:** it measures *uncompressed message payload size* only. It does not measure compressed network bytes, broker disk usage, replication overhead, or Kafka protocol overhead — so its numbers won't exactly match infrastructure-level metrics, but they're a good proxy for "how much application data is flowing."

## Is it running?

**Yes — this is now running as a managed service** (via `systemd`) on its host, meaning it:

- Starts automatically on boot
- Automatically restarts if it crashes
- Runs continuously in the background without anyone needing to launch it manually

You don't need to do anything to "use" it day to day — it's already measuring traffic and publishing summaries.

## What does the output look like?

Once a minute (configurable), for each topic being watched, it emits a small JSON record like this to a Kafka topic (by default `raw-byte-summary`):

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

This tells you: in the one-minute window from 18:30 to 18:31, the `orders` topic carried 1,234 messages totaling about 992 KB of raw data.

If hourly rollups are enabled, you'll also see one record per topic per hour with `"window_type": "hourly"`.

## How can I use this data?

Anyone with read access to the `raw-byte-summary` topic can consume these records to:

- Build dashboards of data volume per topic over time
- Set up alerts for unusual spikes or drops in traffic
- Estimate relative "cost" or load contribution of different topics/teams
- Spot topics that have gone silent (no records being produced)

You don't need to interact with Raw Byte Meter directly — just read from its output topic like any other Kafka consumer.

## Who maintains it / where do I look for more?

- **Source code & technical docs:** see the repository README ([`README.md`](../README.md)), which covers configuration, deployment, Kafka permissions, and troubleshooting.
- **Service status:** ask whoever administers the host it runs on, or check `systemctl status raw-byte-meter` / `journalctl -u raw-byte-meter` on that host.
- **Questions about specific topic numbers:** check the `raw-byte-summary` topic directly, or ask the team responsible for the dashboards/alerts built on top of it.

## Quick facts

| | |
|---|---|
| **Type** | Background Kafka consumer service (no UI, no HTTP API) |
| **Input** | Reads messages from configured Kafka topics |
| **Output** | Writes JSON summaries to a Kafka topic (default `raw-byte-summary`) |
| **Frequency** | Every 60 seconds (configurable), plus optional hourly rollups |
| **Status** | Deployed and running continuously as a systemd service |
| **Maintenance needed** | None for normal use — it's self-running |

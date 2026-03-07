# 🚀 Zero-Redis Scheduled Messaging System Load test

This is the official loadtest code for the **[E-Book] Building a NATS Scheduled Messaging System Without Redis**.
It simulates an extreme stress environment by triggering **multiple overwrite (update) requests for each scheduled message** before dispatching.

## 📂 Directory Structure

```
├── loadTest/
│    └── cmd/
│        └── main.go # starting point of loadtest
│
├── LICENSE.md
└── README.md
```

## 🚀 how to run load test

### 1. How to Run

Execute the test via the `loadtest/main.go` file (adjust the path if necessary).
and look through loadtest/loadResult.log file for NATS cpu and mem usage.

```bash
docker run -p 4222:4222 -p 8222:8222 -ti nats:latest -js -m 8222

cd ./loadtest

# Example:
# (go run main go 2000 10 1 2000 : total 2,000 messages, updated 10 times each, dispatching 2000 messages every 1 minute)
go run main.go {TOTAL_MESSAGES} {OVERWRITE_COUNT} {INTERVAL_MINUTES} {MESSAGES_PER_INTERVAL}
```

### 2. Monitoring Logs

During the test, two log files will be generated, recording resources every 30 seconds:

loadResult.log: Resource usage of the Scheduler JetStream.

### 3. 📊 Test Results

OS: Windows 11
NATS: (docker container) single node, 3 consumer
update count per msg: 10
msg scheduleTine interval: 60s
(# of dispatched msg per minute == # of scheduled msg)

Under severe conditions (each message updated 10 times before scheduling),
NATS handled the traffic flawlessly while maintaining under 1% CPU usage.

| Scheduled Msgs (Total I/O) | Avg Latency | P99 Latency | Max Latency | CPU (Avg / Max) | Memory (Avg / Max)  | Fails |
| -------------------------- | ----------- | ----------- | ----------- | --------------- | ------------------- | ----- |
| 500 (5,000 req)            | 5.03 ms     | 93.68 ms    | 126.10 ms   | 0.00% / 0.00%   | 21.06 MB / 23.25 MB | 0     |
| 1,000 (10,000 req)         | 8.58 ms     | 155.22 ms   | 193.84 ms   | 0.00% / 0.00%   | 25.94 MB / 29.6 MB  | 0     |
| 2,000 (20,000 req)         | 36.89 ms    | 365.9 ms    | 427.25 ms   | 0.00% / 0.00%   | 30.20 MB / 35.2 MB  | 0     |

🔥 Stress Test (total 20,000 messages):

Meaning: With a total of 20,000 reservations and continuous overwrites occurring,
this is a situation where “2,000 notifications simultaneously trigger within a single minute.”
(This perfectly matches scenarios like ticket sales opening or sending notifications at the top of the hour.)

```
go run main.go 20000 10 1 2000
```

- result:
  Even under an extreme load of hundreds of dispatches per second, the system did NOT crash.
  It queued and processed all messages sequentially without data loss.

```
📊 Benchmark Data
Total Dispatched: 20,000 messages
Concurrent Spike: 2,000 messages per minute
Failures / Data Loss: 0

⏱️ Latency Results
Average Latency: 151.72 ms
P95 Latency: 585.60 ms
P99 Latency: 747.06 ms
Max Latency: 1.06 s

💻 NATS Resource Usage
Memory Usage: Avg 52.03 MiB / Max 61.62 MiB
CPU Usage: Avg 7.04% / Max 40.00%

```

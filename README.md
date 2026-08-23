# Demand Intelligence — Local Setup

Two ways to run Kafka for this project:

| Option                                   | When to use                                      |
| ---------------------------------------- | ------------------------------------------------ |
| **A. Local Docker (Apache Kafka KRaft)** | Default — free, offline, no cloud account needed |
| **B. Confluent Cloud (managed)**         | When you need a real distributed cluster         |

The generator auto-detects which one to use based on whether `KAFKA_API_KEY` and `KAFKA_API_SECRET` exist in `.env`.

---

## Option A — Local Docker (recommended for getting started)

### Prerequisites

1. **Docker Desktop** installed and running
2. **Python 3.11+** with the project venv set up
3. **One-time hosts file edit** (requires Administrator PowerShell):

   ```powershell
   # Right-click PowerShell → "Run as Administrator"
   Add-Content C:\Windows\System32\drivers\etc\hosts "127.0.0.1 kafka"
   ```

   Verify:

   ```powershell
   ping kafka
   # Should show: Reply from 127.0.0.1
   ```

   This is required because the broker advertises itself as `kafka:9092`, and
   Windows needs to know how to reach that hostname. Without this entry, the
   Python producer on the host can't connect.

### Install Python deps

```powershell
.\venv\Scripts\python.exe -m pip install kafka-python faker python-dotenv
```

### Start Kafka

```powershell
docker compose up -d
```

Wait 30–40 seconds for the healthcheck. Then verify:

```powershell
docker compose ps
# kafka should show 'Up ... (healthy)'

docker compose logs --tail=10 kafka-init
# last lines should show 'Created topic orders-topic' etc.
```

### Run the generator

```powershell
# No Kafka connection — just print sample events
.\venv\Scripts\python.exe synthetic_generator.py --mode preview

# Stream live events to Kafka (~1 every 0.5s)
.\venv\Scripts\python.exe synthetic_generator.py --mode both

# Backfill 180 days × 500 events/day = 90,000 events (~30–60 seconds)
.\venv\Scripts\python.exe synthetic_generator.py --mode backfill --days 180
```

### Verify in Kafka UI

Open **http://localhost:8080** in your browser. Select the `orders-topic` and click "Messages" to see live events flowing in.

---

## Option B — Confluent Cloud (managed)

### Prerequisites

1. Free Confluent Cloud account at https://confluent.cloud/
2. A cluster (any region — `ap-south-1` for lowest latency from India)
3. An API key (Cluster settings → API keys → Add key)
4. Two topics created: `orders-topic` (3 partitions) and `pricing-topic` (2 partitions)

### Configure credentials

```powershell
Copy-Item .env.example .env
notepad .env
```

Fill in:

```
KAFKA_BOOTSTRAP_SERVER=pkc-xxxxx.region.aws.confluent.cloud:9092
KAFKA_API_KEY=YOUR_API_KEY
KAFKA_API_SECRET=YOUR_API_SECRET
```

### Run the generator

Same commands as Option A — the generator detects the env vars and switches to Confluent Cloud automatically.

---

## Troubleshooting

| Error                                          | Cause                                    | Fix                                                                                             |
| ---------------------------------------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `kafka-python not installed`                   | pip install needed                       | Run the install command above                                                                   |
| `KafkaTimeoutError: Failed to update metadata` | Broker not reachable                     | For local: Docker stack not up, or hosts file entry missing. For cloud: wrong bootstrap server. |
| `Authentication failed` (Confluent Cloud)      | Wrong API key/secret                     | Re-generate API key in Confluent Cloud                                                          |
| `NoBrokersAvailable`                           | Kafka container not started or unhealthy | `docker compose ps` and `docker compose logs kafka`                                             |
| `ping kafka` returns wrong IP                  | Hosts file entry missing or wrong        | Re-run the `Add-Content` command as Administrator                                               |

---

## Common operations

```powershell
# Stop the stack (keeps data)
docker compose down

# Wipe all data and start fresh
docker compose down -v

# Tail Kafka logs
docker compose logs -f kafka

# Check topics from inside the container
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

---

## Architecture recap

```
┌─────────────────────────────────┐                ┌─────────────────┐
│  Python producer (synthetic_    │  ──kafka:9092─▶│  Kafka broker   │
│  generator.py) — on Windows     │                │  (in Docker)    │
└─────────────────────────────────┘                └─────────────────┘
                                                            ▲
                                                            │
                  ┌─────────────────────────────────────────┘
                  │
                  ├── kafka-init  (one-shot, creates topics)
                  └── kafka-ui    (http://localhost:8080 — web dashboard)
```

# ===========================
# Common Configuration
# ===========================

# Broker IDs
BROKER_ID = "broker1"       # change to "broker2" on follower

# Shared Redis (on leader machine)
REDIS_HOST = "192.168.191.105"   # <-- IP of the LEADER machine running Redis
REDIS_PORT = 6379

# Ports
PORT = 5000 if BROKER_ID == "broker1" else 5001

# Lease time in ms (must match across all)
LEASE_TTL_MS = 10000

# Optional file log
LOG_FILE = f"logs/{BROKER_ID}.log"

# Optional known brokers (used for display, not critical anymore)
BROKERS = {
    "broker1": "http://192.168.191.105:5000",
    "broker2": "http://192.168.191.197:5001"
}

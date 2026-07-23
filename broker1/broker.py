import time, threading, json, requests, socket, os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from common import redis_client
from common.config import BROKER_ID, BROKERS, REDIS_HOST, REDIS_PORT, LEASE_TTL_MS, LOG_FILE, PORT
from common.log_manager import LogManager

app = FastAPI(title=f"YAK Broker ({BROKER_ID})")

# ---------------------------
# State
# ---------------------------
is_leader = False
leader_info = None
local_log = []

# Ensure log directory exists
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# ---------------------------
# Helper: File-backed log manager
# ---------------------------
class LogManager:
    def __init__(self, file_path):
        self.file_path = file_path
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                self.log = [line.strip() for line in f if line.strip()]
        else:
            self.log = []

    def append(self, offset, message):
        try:
            self.log.append(f"{offset}:{message}")
            with open(self.file_path, "a") as f:
                f.write(f"{offset}:{message}\n")
            return True   # ✅ <--- add this
        except Exception as e:
            print(f"[ERROR] Log append failed: {e}")
            return False


    def read_all(self):
        return self.log

log_mgr = LogManager(LOG_FILE)

# ---------------------------
# Leader Election & Heartbeat
# ---------------------------
def heartbeat_thread():
    global is_leader, leader_info
    while True:
        leader = redis_client.get_current_leader()
        if not leader:
            print("[ELECTION] No leader found — attempting to acquire lease...")
            ok = redis_client.try_acquire_leader_lease(BROKER_ID, LEASE_TTL_MS)
            if ok:
                is_leader = True
                print(f"[INFO] {BROKER_ID} became LEADER.")
                leader_info = {"broker_id": BROKER_ID, "ip": get_my_ip(), "port": PORT}
            else:
                is_leader = False
        else:
            leader_id = leader.get("broker_id")
            if leader_id == BROKER_ID:
                is_leader = True
                redis_client.renew_lease(BROKER_ID, LEASE_TTL_MS)
            else:
                is_leader = False
                leader_info = leader

        time.sleep(LEASE_TTL_MS / 3000)  # renew ~3 times/sec

def get_my_ip():
    return socket.gethostbyname(socket.gethostname())

# ---------------------------
# API Routes
# ---------------------------

@app.post("/produce")
async def produce(req: Request):
    global is_leader
    data = await req.json()
    msg = data.get("message")

    if not msg:
        return JSONResponse({"error": "Empty message"}, status_code=400)

    if not is_leader:
        leader = redis_client.get_current_leader()
        return JSONResponse({"error": "Not the Leader", "leader": leader}, status_code=403)

    offset = redis_client.get_hwm() + 1
    log_mgr.append(offset, msg)
    print(f"[LEADER] Received '{msg}' at offset {offset}")

    ack_count = replicate_to_followers(offset, msg)
    if ack_count >= 1:
        redis_client.set_hwm(offset)
        print(f"[COMMIT] Message '{msg}' committed at offset {offset}")
        return {"status": "OK", "offset": offset}
    else:
        print("[WARN] Replication failed, message uncommitted.")
        return JSONResponse({"error": "Replication failed"}, status_code=500)

@app.post("/internal/replicate")
async def internal_replicate(req: Request):
    try:
        data = await req.json()
        offset = data.get("offset")
        msg = data.get("message")

        if offset is None or msg is None:
            return JSONResponse({"error": "Invalid replication payload"}, status_code=400)

        print(f"[FOLLOWER] Replicating message '{msg[:50]}...' at offset {offset}")

        success = log_mgr.append(offset, msg)

        if not success:
            raise Exception("Log write failed")

        return {"status": "OK"}
    except Exception as e:
        print(f"[ERROR] Replication failed internally: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/consume")
async def consume(offset: int = -1):
    try:
        hwm = redis_client.get_hwm()
        msgs = log_mgr.read_all()

        readable = []
        for line in msgs:
            # skip empty or invalid lines
            if not line.strip() or ":" not in line:
                continue

            parts = line.split(":", 1)
            if len(parts) != 2:
                continue

            idx, message = parts
            try:
                if int(idx) > offset and int(idx) <= hwm:
                    readable.append(message)
            except ValueError:
                continue

        return {"messages": readable, "hwm": hwm}
    except Exception as e:
        print(f"[ERROR] Consume failed: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/metadata/leader")
async def metadata_leader():
    leader = redis_client.get_current_leader()
    return {"leader": leader}

# ---------------------------
# Replication Helper
# ---------------------------
def replicate_to_followers(offset, msg):
    ack_count = 0
    for bid, url in BROKERS.items():
        if bid == BROKER_ID:
            continue
        try:
            resp = requests.post(f"{url}/internal/replicate",
                                 json={"offset": offset, "message": msg},
                                 timeout=3)
            if resp.status_code == 200:
                ack_count += 1
                print(f"[REPLICATION] ACK from {bid}")
        except Exception as e:
            print(f"[ERROR] Failed to replicate to {bid}: {e}")
    return ack_count

# ---------------------------
# Startup
# ---------------------------
if __name__ == "__main__":
    import uvicorn
    threading.Thread(target=heartbeat_thread, daemon=True).start()
    print(f"[BROKER] Starting {BROKER_ID} on port {PORT} (Leader={is_leader})")
    uvicorn.run(app, host="0.0.0.0", port=PORT)

import requests
import time
import json
import os

# ----------------------------
# Configuration
# ----------------------------
REDIS_LEADER_URLS = [
    "http://192.168.191.105:5000/metadata/leader",  # Broker 1
    "http://192.168.191.197:5001/metadata/leader"   # Broker 2
]

LOCAL_OFFSET_FILE = "consumer_offset.txt"
LOCAL_LOG_FILE = "consumer_messages.log"
POLL_INTERVAL = 2  # seconds between fetch attempts


# ----------------------------
# Local State Handling
# ----------------------------
def load_last_offset():
    if os.path.exists(LOCAL_OFFSET_FILE):
        with open(LOCAL_OFFSET_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except:
                return -1
    return -1


def save_last_offset(offset):
    with open(LOCAL_OFFSET_FILE, "w") as f:
        f.write(str(offset))


def append_local_log(offset, message):
    with open(LOCAL_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{offset}:{message}\n")


# ----------------------------
# Leader Discovery
# ----------------------------
def get_current_leader():
    for url in REDIS_LEADER_URLS:
        try:
            resp = requests.get(url, timeout=3)
            if resp.status_code == 200:
                data = resp.json().get("leader")
                if data and data.get("ip") and data.get("port"):
                    return data.get("ip"), data.get("port")
        except Exception:
            continue
    return None, None


# ----------------------------
# Consume Messages
# ----------------------------
def consume_messages(ip, port, offset):
    try:
        resp = requests.get(
            f"http://{ip}:{port}/consume?offset={offset}",
            timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            msgs = data.get("messages", [])
            hwm = data.get("hwm", offset)

            if msgs:
                for msg in msgs:
                    offset += 1
                    print(f"[RECEIVED] Offset {offset}: {msg}")
                    append_local_log(offset, msg)
                    save_last_offset(offset)
            else:
                print("[WAIT] No new messages yet...")

            return offset
        else:
            print(f"[ERR] Leader responded with {resp.status_code}: {resp.text}")
            return offset

    except requests.exceptions.ConnectionError:
        print(f"[CONN] Connection refused at {ip}:{port}. Retrying...")
        return offset
    except Exception as e:
        print(f"[ERR] Exception while consuming: {e}")
        return offset


# ----------------------------
# Main Loop
# ----------------------------
def main():
    print("[CONSUMER] Continuous consumer started. Press CTRL+C to stop.")
    offset = load_last_offset()
    ip, port = get_current_leader()

    if not ip or not port:
        print("[FATAL] No leader found at startup.")
        return

    print(f"[INFO] Connected to leader at {ip}:{port} starting from offset {offset}")

    retry_count = 0
    while True:
        new_ip, new_port = get_current_leader()
        if new_ip and new_port and (new_ip != ip or new_port != port):
            print(f"[LEADER CHANGE] New leader detected: {new_ip}:{new_port}")
            ip, port = new_ip, new_port

        offset = consume_messages(ip, port, offset)
        retry_count += 1
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
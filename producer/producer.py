import time
import json
import requests
from common import redis_client

# Configuration
PRODUCE_INTERVAL = 3     # seconds between messages
RETRY_INTERVAL = 5       # seconds before retrying after a failure
MAX_RETRIES = 5          # consecutive retry attempts before rechecking leader

def get_leader_info():
    """Fetch current leader info from Redis, handle tuple or dict."""
    leader = redis_client.get_current_leader()
    if not leader:
        print("[WARN] No leader found in Redis.")
        return None, None, None

    # ✅ Handle both tuple and dict
    if isinstance(leader, tuple):
        if len(leader) == 3:
            return leader  # (broker_id, ip, port)
        else:
            print("[ERR] Unexpected tuple format for leader:", leader)
            return None, None, None
    elif isinstance(leader, dict):
        return leader.get("broker_id"), leader.get("ip"), leader.get("port")
    else:
        print("[ERR] Unknown leader format:", type(leader), leader)
        return None, None, None

def send_to_leader(message):
    """Send message to the active leader."""
    leader_id, leader_ip, leader_port = get_leader_info()
    if not leader_ip or not leader_port:
        print("[ERR] Leader info invalid — waiting for new leader election...")
        return False

    url = f"http://{leader_ip}:{leader_port}/produce"
    try:
        response = requests.post(url, json={"message": message}, timeout=5)
        if response.status_code == 200:
            print(f"[OK] Sent → Leader {leader_id} ({leader_ip}:{leader_port}) → {response.json()}")
            return True
        elif response.status_code == 403:
            print(f"[WARN] {leader_id} refused (not leader anymore).")
            return False
        else:
            print(f"[ERR] Leader responded with {response.status_code}: {response.text}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"[CONN] Connection refused at {leader_ip}:{leader_port}. Retrying...")
        return False
    except requests.exceptions.Timeout:
        print(f"[TIMEOUT] Timeout when sending to {leader_ip}:{leader_port}")
        return False
    except Exception as e:
        print(f"[ERR] Unexpected error: {e}")
        return False

def main():
    print("[PRODUCER] Continuous producer started. Press CTRL+C to stop.")
    counter = 0

    while True:
        message = f"Sensor reading #{counter}: Temp={20 + counter % 5}°C"
        print(f"[SEND] Trying: {message}")

        retries = 0
        sent = False
        while not sent:
            sent = send_to_leader(message)
            if not sent:
                retries += 1
                if retries >= MAX_RETRIES:
                    print("[WARN] Max retries reached. Rechecking leader in Redis...")
                    retries = 0
                time.sleep(RETRY_INTERVAL)
            else:
                break  # message successfully sent

        counter += 1
        time.sleep(PRODUCE_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[PRODUCER] Stopped by user.")
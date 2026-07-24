import requests
from common_config import BROKERS

def get_current_leader():
    """Ask any broker who the current leader is."""
    for b in BROKERS:
        try:
            resp = requests.get(f"{b}/metadata/leader", timeout=2)
            if resp.status_code == 200:
                leader = resp.json().get("leader")
                if leader:
                    # Return full URL of leader broker
                    for candidate in BROKERS:
                        if leader in candidate:
                            return candidate
                    # If only broker ID returned (e.g., 'broker1'), infer mapping
                    return BROKERS[0] if leader == "broker1" else BROKERS[1]
        except Exception:
            continue
    return None


def consume_from_leader(leader_url, offset):
    """Read messages from leader starting after given offset."""
    try:
        resp = requests.get(f"{leader_url}/consume", params={"offset": offset}, timeout=3)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 403:
            # Not the leader anymore
            raise Exception("Not the Leader")
    except Exception as e:
        raise e

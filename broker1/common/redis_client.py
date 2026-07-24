import redis, json, time
from common.config import REDIS_HOST, REDIS_PORT

r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
LEASE_KEY = "leader_lease"

def get_current_leader():
    val = r.get(LEASE_KEY)
    if not val:
        return None
    try:
        data = json.loads(val)
        return data
    except Exception:
        return None

def try_acquire_leader_lease(broker_id, ttl_ms):
    # Assign the correct ZeroTier IP and port based on which broker this is
    if broker_id == "broker1":
        ip = "192.168.191.105"   # Leader’s ZeroTier IP
        port = 5000
    else:
        ip = "192.168.191.197"   # Follower’s ZeroTier IP
        port = 5001

    leader_info = {
        "broker_id": broker_id,
        "ip": ip,
        "port": port,
        "expires_at": time.time() + ttl_ms / 1000
    }

    ok = r.setnx("leader_lease", json.dumps(leader_info))
    if ok:
        r.expire("leader_lease", int(ttl_ms / 1000))
    return ok

def renew_lease(broker_id, ttl_ms):
    val = r.get(LEASE_KEY)
    if not val:
        return False
    data = json.loads(val)
    if data["broker_id"] != broker_id:
        return False
    data["expires_at"] = time.time() + ttl_ms / 1000
    r.set(LEASE_KEY, json.dumps(data), ex=int(ttl_ms / 1000))
    return True

def set_hwm(offset):
    r.set("hwm", offset)

def get_hwm():
    val = r.get("hwm")
    return int(val) if val else -1

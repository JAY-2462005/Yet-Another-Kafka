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
        return data["broker_id"], data["ip"], data["port"]
    except Exception:
        return None, None, None

def try_acquire_leader_lease(broker_id, ttl_ms, ip, port):
    leader_info = {
        "broker_id": broker_id,
        "ip": ip,
        "port": port,
        "expires_at": time.time() + ttl_ms / 1000
    }
    ok = r.setnx(LEASE_KEY, json.dumps(leader_info))
    if ok:
        r.expire(LEASE_KEY, int(ttl_ms / 1000))
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
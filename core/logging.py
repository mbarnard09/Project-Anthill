import json, sys, time, os

def log(level: str, msg: str, **kwargs):
    record = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "level": level, "msg": msg, **kwargs}
    sys.stdout.write(json.dumps(record) + "\n")
    sys.stdout.flush()

info = lambda m, **k: log("INFO", m, **k)
warn = lambda m, **k: log("WARN", m, **k)
err  = lambda m, **k: log("ERROR", m, **k)


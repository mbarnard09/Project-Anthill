import time, os
from core.config import Settings
from core.logging import info

def main():
    s = Settings()
    subs = (s.SUBREDDITS or "").split(",") if s.SUBREDDITS else []
    info("starting reddit1 placeholder", service=s.SERVICE_NAME, subs=subs)
    i = 0
    while True:
        i += 1
        info("reddit1 tick", tick=i, subs=subs)
        time.sleep(s.POLL_SECONDS)

if __name__ == "__main__":
    main()


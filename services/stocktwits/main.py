import time
from core.config import Settings
from core.logging import info

def main():
    s = Settings()
    info("starting stocktwits placeholder", service=s.SERVICE_NAME)
    i = 0
    while True:
        i += 1
        info("stocktwits tick", tick=i)
        time.sleep(s.POLL_SECONDS)

if __name__ == "__main__":
    main()


import time
from core.config import Settings
from core.logging import info

def main():
    s = Settings()
    boards = (s.FOURCHAN_BOARDS or "biz").split(",")
    info("starting fourchan placeholder", service=s.SERVICE_NAME, boards=boards)
    i = 0
    while True:
        i += 1
        info("fourchan tick", tick=i, boards=boards)
        time.sleep(s.POLL_SECONDS)

if __name__ == "__main__":
    main()


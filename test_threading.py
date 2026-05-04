import threading
import time
import random

def sync_worker():
    while True:
        slp = random.randint(1, 15)
        print("This is SYNC_WORKER!", " *** ", slp)
        time.sleep(slp)

def run_serial_protocol():
    while True:
        slp = random.randint(1, 100)
        print("This is SERIAL_WORKER!", " *** ", slp)
        time.sleep(0.1)

threading.Thread(target=sync_worker, daemon=True).start()
run_serial_protocol()
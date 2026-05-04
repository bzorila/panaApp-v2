import threading
import time
import random

def sync_worker():
    while True:
        slp = random.randint(1, 10)
        print("This is SYNC_WORKER!", " *** ", slp)
        time.sleep(1)

def run_serial_protocol():
    while True:
        slp = 10*random.random()
        print("This is SERIAL_WORKER!", " *** ", slp)
        time.sleep(slp)

threading.Thread(target=sync_worker, daemon=True).start()
run_serial_protocol()
import threading
import time
import random
import sqlite3
from datetime import datetime

def init_db():
    conn = sqlite3.connect('date_locale.db', timeout=10, check_same_thread=False)
    
    # Optimizări performanță
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    
    # Creare tabel
    conn.execute('''CREATE TABLE IF NOT EXISTS readings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        device_id TEXT,
                        val REAL,
                        sync_status INTEGER DEFAULT 0,
                        timestamp DATETIME
                    )''')
    
    # Index pentru căutare rapidă a datelor netrimise
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_sync ON readings (id) WHERE sync_status = 0;")
    
    return conn

def save_to_local_db(device_id, value):
    conn = sqlite3.connect('date_locale.db', timeout=10, check_same_thread=False)
    dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with conn: # Deschide automat o tranzacție
            conn.execute("INSERT INTO readings (device_id, val, timestamp) VALUES (?, ?, ?)", 
                         (device_id, value, dt))
        # Commit-ul se face automat aici la ieșirea din blocul 'with'
    except sqlite3.Error as e:
        print(f"Eroare la scrierea în baza de date: {e}")

def sync_worker():
    while True:
        slp = random.randint(1, 10)        
        print("This is SYNC_WORKER!", " *** ", slp)        
        time.sleep(slp)

def run_serial_protocol():
    init_db()
    while True:
        slp = round(10*random.random(), 2)
        dev = random.randint(0, 3)
        if dev == 0:
            dev += 1
        elif dev == 3:
            dev -= 1
        print("This is SERIAL_WORKER!", " *** ", slp, dev)
        save_to_local_db(dev, slp)
        time.sleep(5)

threading.Thread(target=sync_worker, daemon=True).start()
run_serial_protocol()
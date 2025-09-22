# arquivo: cep_worker.py
import asyncio
import aiohttp
import aiosqlite
import csv
import time
import random
from asyncio import Semaphore

VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"

# parâmetros ajustáveis
CONCURRENCY = 100           # controle de concorrência (ajuste com cautela)
MAX_RETRIES = 4
BACKOFF_BASE = 0.5          # segundos
DB_PATH = "cache_ceps.db"
INPUT_CSV = "input_ceps.csv"
OUTPUT_CSV = "results_ceps.csv"

async def init_db():
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("""
      CREATE TABLE IF NOT EXISTS cep_cache (
        cep TEXT PRIMARY KEY,
        json TEXT,
        fetched_at INTEGER
      )
    """)
    await db.commit()
    return db

async def fetch_cep(session, cep):
    url = VIACEP_URL.format(cep=cep)
    for attempt in range(1, MAX_RETRIES+1):
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.text()
                    # via cep returns {"erro": true} for invalid cep
                    return data
                elif resp.status in (429, 503, 502, 504):
                    # backoff + jitter
                    wait = BACKOFF_BASE * (2 ** (attempt - 1)) + random.random() * 0.1
                    await asyncio.sleep(wait)
                else:
                    # other 4xx/5xx: don't retry too much
                    text = await resp.text()
                    return {"error_status": resp.status, "body": text}
        except asyncio.TimeoutError:
            wait = BACKOFF_BASE * (2 ** (attempt - 1)) + random.random() * 0.1
            await asyncio.sleep(wait)
        except Exception as e:
            # log and return minimal info
            return {"exception": str(e)}
    return {"error": "max_retries_exceeded"}

async def process_cep(sem: Semaphore, session, db, cep, writer_lock, csv_writer):
    # normalize cep: remove hífens/espacos
    cep_norm = "".join(ch for ch in cep if ch.isdigit())
    if len(cep_norm) != 8:
        result = {"cep": cep, "error": "invalid_format"}
        async with writer_lock:
            csv_writer.writerow([cep, str(result)])
        return

    # check cache
    row = await db.execute_fetchone("SELECT json FROM cep_cache WHERE cep = ?", (cep_norm,))
    if row:
        saved = row[0]
        async with writer_lock:
            csv_writer.writerow([cep_norm, saved, "from_cache"])
        return

    async with sem:
        data = await fetch_cep(session, cep_norm)

    # persist
    ts = int(time.time())
    await db.execute(
        "INSERT OR REPLACE INTO cep_cache (cep, json, fetched_at) VALUES (?, ?, ?)",
        (cep_norm, str(data), ts)
    )
    await db.commit()

    async with writer_lock:
        csv_writer.writerow([cep_norm, str(data), "fetched"])

async def main():
    db = await init_db()
    sem = Semaphore(CONCURRENCY)
    writer_lock = asyncio.Lock()

    # load input CEPS (one per line or from csv)
    input_ceps = []
    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if row:
                input_ceps.append(row[0].strip())

    # prepare output csv
    out_f = open(OUTPUT_CSV, "w", newline='', encoding='utf-8')
    csv_writer = csv.writer(out_f)
    csv_writer.writerow(["cep", "response_json", "source"])

    timeout = aiohttp.ClientTimeout(total=30)
    conn = aiohttp.TCPConnector(limit=0)  # we control concurrency via sem
    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
        tasks = [
            asyncio.create_task(process_cep(sem, session, db, cep, writer_lock, csv_writer))
            for cep in input_ceps
        ]
        # gather with limited concurrency via sem
        await asyncio.gather(*tasks)

    await db.close()
    out_f.close()

if __name__ == "__main__":
    asyncio.run(main())

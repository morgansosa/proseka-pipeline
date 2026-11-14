from airflow.sdk import dag, task
from datetime import datetime, timezone, timedelta
import json, sqlite3, urllib.request


BASE = "https://raw.githubusercontent.com/Sekai-World/sekai-master-db-diff/main/"
DB_PATH = "/home/momop/airflow-proseka/proseka.sqlite"
SERVER = "jp"

def ms_to_date(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat() if ms else None

def on_fail(context):
    ti = context["task_instance"]
    with open("/home/momop/airflow-proseka/failures.log", "a") as f:
        f.write(f"{datetime.now(timezone.utc).isoformat()}  {ti.dag_id}.{ti.task_id} "
                f"try {ti.try_number}: {context.get('exception')}\n") 

@dag(
        schedule="@daily", 
        start_date=datetime(2026, 11, 1), 
        catchup=False, 
        tags=["proseka"], 
        default_args={
            "retries": 2, 
            "retry_delay": timedelta(minutes=10), 
            "on_retry_callback": on_fail, 
            "on_failure_callback": on_fail}
)
def proseka_songs():

    @task
    def fetch() -> dict:
        def get(name):
            with urllib.request.urlopen(BASE + name) as r:
                return json.load(r)
        musics = [{
            "music_id": m["id"], "server": SERVER, "title": m["title"],
            "pronunciation": m.get("pronunciation"), "composer": m.get("composer"),
            "lyricist": m.get("lyricist"), "arranger": m.get("arranger"),
            "published_at": ms_to_date(m.get("publishedAt")),
        } for m in get("musics.json")]
        charts = [{
            "music_id": d["musicId"], "server": SERVER, "difficulty": d["musicDifficulty"],
            "level": d["playLevel"], "notes": d["totalNoteCount"],
        } for d in get("musicDifficulties.json")]
        return {"songs": musics, "charts": charts}

    @task
    def diff(data: dict) -> dict:
        con = sqlite3.connect(DB_PATH)
        known = {r[0] for r in con.execute("SELECT music_id FROM songs WHERE server=?", (SERVER,))}
        con.close()
        n_fetched, n_known = len(data["songs"]), len(known)
        if n_fetched == 0:
            raise ValueError("fetch returned zero songs; refusing to continue")
        if n_fetched < n_known:
            raise ValueError(f"fetched {n_fetched} songs but DB already has {n_known}; source looks wrong")
        new_ids = sorted(s["music_id"] for s in data["songs"] if s["music_id"] not in known)
        return {"new_ids": new_ids, **data}

    @task
    def load(data: dict) -> int:
        con = sqlite3.connect(DB_PATH)
        con.executemany("""
            INSERT INTO songs (music_id, server, title, pronunciation, composer, lyricist, arranger, published_at)
            VALUES (:music_id, :server, :title, :pronunciation, :composer, :lyricist, :arranger, :published_at)
            ON CONFLICT(music_id, server) DO UPDATE SET
              title=excluded.title, pronunciation=excluded.pronunciation, composer=excluded.composer,
              lyricist=excluded.lyricist, arranger=excluded.arranger, published_at=excluded.published_at
        """, data["songs"])
        con.executemany("""
            INSERT INTO charts (music_id, server, difficulty, level, notes)
            VALUES (:music_id, :server, :difficulty, :level, :notes)
            ON CONFLICT(music_id, server, difficulty) DO UPDATE SET
              level=excluded.level, notes=excluded.notes
        """, data["charts"])
        con.execute("INSERT INTO refresh_log (at, server, n_songs, n_charts) VALUES (?, ?, ?, ?)",
                    (datetime.now(timezone.utc).isoformat(), SERVER, len(data["songs"]), len(data["charts"])))
        con.commit(); con.close()
        return len(data["new_ids"])

    load(diff(fetch()))

proseka_songs()

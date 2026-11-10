# proseka-pipeline

An Apache Airflow pipeline that keeps a SQLite database of Project Sekai songs, charts, and my full-combo history up to date.

I've been logging which songs I FC in [Project Sekai](https://pjsekai.sega.jp/) since March 2026. This replaces the Google Sheet I was updating by hand with a daily DAG that pulls the song list from the game's master data and upserts it into SQLite, so the only thing I ever enter manually is the clear itself.

The long version, including everything that went wrong, is in [NOTES.md](NOTES.md).

## What it does

Once a day, `proseka_songs` runs three tasks:

1. **fetch** — downloads `musics.json` and `musicDifficulties.json` from [Sekai-World/sekai-master-db-diff](https://github.com/Sekai-World/sekai-master-db-diff) and reshapes them to match the schema.
2. **diff** — compares the fetched song IDs against what's already in the database and reports which ones are new.
3. **load** — upserts every song and chart (`INSERT ... ON CONFLICT DO UPDATE`), so rebalanced levels get picked up, and writes a row to `refresh_log`.

My clears live in a separate `clears` table that the DAG never touches.

## Stack

- Apache Airflow 3.3 (TaskFlow API, `airflow standalone`)
- SQLite
- systemd, running Airflow as a system service on WSL2/Ubuntu
- Python standard library only; no extra dependencies beyond Airflow

## Layout

```
airflow-proseka/
├── README.md
├── NOTES.md                      write-up: why, how, and what broke
├── schema.sql                    the SQLite schema
├── .gitignore
├── airflow/
│   └── dags/
│       └── proseka_songs.py      the DAG
├── systemd/
│   └── airflow-proseka.service   system unit that runs Airflow at boot
└── docs/
    └── img/                      screenshots
```

## Running it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install "apache-airflow==3.3.2" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-$(python -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")').txt"

sqlite3 proseka.sqlite < schema.sql
export AIRFLOW_HOME=$PWD/airflow
airflow standalone
```

The UI is at `http://localhost:8080`; the admin password is in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`. `DB_PATH` at the top of the DAG is an absolute path; change it to wherever your `proseka.sqlite` lives.

To run it as a service instead:

```bash
sudo cp systemd/airflow-proseka.service /etc/systemd/system/
# edit User=, Group=, and the paths to match your setup
sudo systemctl daemon-reload
sudo systemctl enable --now airflow-proseka
```

## Schema

Five tables. `songs` and `charts` come from the master data; `clears` is mine; `refresh_log` is one row per DAG run. See `schema.sql`.

| table | grain |
|---|---|
| `songs` | one row per song per server (`jp` / `en`) |
| `charts` | one row per song per server per difficulty, with level and note count |
| `clears` | one row per song/difficulty/clear type (CLEAR, FC, AP), with the date I got it |
| `refresh_log` | one row per load, with counts |

## Next

- Add the EN server (`sekai-master-db-en-diff`); the schema already has a `server` column
- Sanity checks in `diff`: fail the run if the fetch comes back empty or smaller than the database
- Push the current state back to the Google Sheet so the sheet becomes a view
- dbt models over the same database (staging model, `first_fc_by_level` mart, tests)

## Credits

Song and chart data is from [Sekai-World/sekai-master-db-diff](https://github.com/Sekai-World/sekai-master-db-diff). Project Sekai is © SEGA / Colorful Palette / Crypton Future Media.

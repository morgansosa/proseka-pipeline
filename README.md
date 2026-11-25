# proseka-pipeline

An Apache Airflow pipeline that keeps a SQLite database of Project Sekai songs, charts, and my full-combo history up to date, plus a small Streamlit app for logging clears and a dbt project that models the result.

I've been logging which songs I FC in [Project Sekai](https://pjsekai.sega.jp/) since March 2026. This replaces the Google Sheet I was updating by hand with a daily DAG that pulls the song list from the game's master data and upserts it into SQLite, so the only thing I ever enter manually is the clear itself.

The long version, including everything that went wrong, is in [NOTES.md](NOTES.md).

## What it does

Once a day, `proseka_songs` runs three tasks:

1. **fetch** — downloads `musics.json` and `musicDifficulties.json` from [Sekai-World/sekai-master-db-diff](https://github.com/Sekai-World/sekai-master-db-diff) and reshapes them to match the schema.
2. **diff** — compares the fetched song IDs against what's already in the database and reports which ones are new. Fails the run if the fetch comes back empty or smaller than the database, so a bad download never reaches the load.
3. **load** — upserts every song and chart (`INSERT ... ON CONFLICT DO UPDATE`), so rebalanced levels get picked up, and writes a row to `refresh_log`.

Every task retries twice, ten minutes apart. Each retry and each final failure appends a line to `failures.log`.

My clears live in a separate `clears` table that the DAG never touches. They go in through the app:

- **`app/clears_app.py`** — search charts by title, kana, level, or difficulty; tick CLEAR / FC / AP; ticks accumulate in a cart across searches; one date and optional per-row notes per batch. Add-only: nothing is ever deleted, and existing dates are never changed. AP implies FC implies CLEAR, and implied rows are inserted with `date_is_upper_bound = 1`.

On top of the database, **`proseka_dbt/`** is a dbt project that reads the SQLite file through DuckDB and builds:

- two staging views, `stg_charts` and `stg_clears`, that tidy names and types
- one mart, `first_fc_by_level`: for each chart level, the date of my first logged FC and how many I have
- three tests: `clear_type` is always CLEAR, FC, or AP, and the mart has exactly one non-null row per level

## Stack

- Apache Airflow 3.3 (TaskFlow API, `airflow standalone`)
- SQLite
- Streamlit + pandas for the entry app
- dbt-core with the dbt-duckdb adapter (DuckDB attaches the SQLite file as a source)
- systemd, running Airflow as a system service on WSL2/Ubuntu

## Layout

```
airflow-proseka/
├── README.md
├── NOTES.md                      write-up: why, how, and what broke
├── schema.sql                    the SQLite schema
├── requirements.txt
├── .gitignore
├── airflow/
│   └── dags/
│       └── proseka_songs.py      the DAG
├── app/
│   └── clears_app.py             Streamlit app for logging clears
├── proseka_dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── schema.yml            tests
│       ├── staging/
│       │   ├── sources.yml       the three source tables
│       │   ├── stg_charts.sql
│       │   └── stg_clears.sql
│       └── marts/
│           └── first_fc_by_level.sql
├── systemd/
│   └── airflow-proseka.service   system unit that runs Airflow at boot
└── docs/
    └── img/                      screenshots
```

## Running it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-$(python -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")').txt"

sqlite3 proseka.sqlite < schema.sql
export AIRFLOW_HOME=$PWD/airflow
airflow standalone
```

The UI is at `http://localhost:8080`; the admin password is in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`. `DB_PATH` at the top of the DAG and the app is an absolute path; change it to wherever your `proseka.sqlite` lives.

To run Airflow as a service instead:

```bash
sudo cp systemd/airflow-proseka.service /etc/systemd/system/
# edit User=, Group=, and the paths to match your setup
sudo systemctl daemon-reload
sudo systemctl enable --now airflow-proseka
```

### Logging clears

```bash
streamlit run app/clears_app.py
```

Opens at `http://localhost:8501`. Narrow by level and difficulty, search by title or kana, tick boxes, set the date, submit. The app refuses to render more than 300 charts at once, so filter first.

### dbt models

dbt lives in its own virtual environment so its dependencies don't collide with Airflow's.

```bash
python3 -m venv .venv-dbt && source .venv-dbt/bin/activate
pip install dbt-core dbt-duckdb
```

Add a profile to `~/.dbt/profiles.yml`, with both paths changed to match your setup:

```yaml
proseka_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /home/you/airflow-proseka/proseka_dbt/proseka.duckdb
      attach:
        - path: /home/you/airflow-proseka/proseka.sqlite
          type: sqlite
          alias: src
```

Then, from inside the project folder:

```bash
cd proseka_dbt
dbt run     # builds the two staging views and the mart
dbt test    # runs the three tests
dbt show --select first_fc_by_level --limit 30
```

Check the `attach` path carefully. If it points at a file that doesn't exist, DuckDB creates an empty one without complaint and the models fail with "table does not exist."

## Schema

Four tables. `songs` and `charts` come from the master data; `clears` is mine; `refresh_log` is one row per DAG run. See `schema.sql`.

| table | grain |
|---|---|
| `songs` | one row per song per server (`jp` / `en`) |
| `charts` | one row per song per server per difficulty, with level and note count |
| `clears` | one row per song/difficulty/clear type (CLEAR, FC, AP), with the date I got it and whether that date is an upper bound |
| `refresh_log` | one row per load, with counts |

And the dbt models built on top:

| model | grain |
|---|---|
| `stg_charts` | one row per chart, joined to its song |
| `stg_clears` | one row per song/difficulty/clear type, with a real date column |
| `first_fc_by_level` | one row per chart level |

## Next

- Add the EN server (`sekai-master-db-en-diff`); the schema already has a `server` column
- Rename `first_fc_date` to say what it is, the first *logged* FC, and mark FCs from before I started tracking as upper bounds
- Audit the level-27 rows logged as FC that are really clears
- Add a Snowflake target so the same dbt models run on DuckDB and Snowflake
- Push the current state back to the Google Sheet so the sheet becomes a view
- Romaji search in the app, only if it ever becomes multi-user

## Credits

Song and chart data is from [Sekai-World/sekai-master-db-diff](https://github.com/Sekai-World/sekai-master-db-diff). Project Sekai is © SEGA / Colorful Palette / Crypton Future Media.

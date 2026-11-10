CREATE TABLE songs (
    music_id      INTEGER NOT NULL,
    server        TEXT    NOT NULL,          -- 'jp' | 'en'
    title         TEXT    NOT NULL,
    pronunciation TEXT,
    composer      TEXT, lyricist TEXT, arranger TEXT,
    published_at  TEXT,                      -- ISO date, server release
    PRIMARY KEY (music_id, server)
);
CREATE TABLE charts (
    music_id   INTEGER NOT NULL,
    server     TEXT    NOT NULL,
    difficulty TEXT    NOT NULL,
    level      INTEGER,
    notes      INTEGER,
    PRIMARY KEY (music_id, server, difficulty)
);
CREATE TABLE refresh_log (
    at TEXT, server TEXT, n_songs INTEGER, n_charts INTEGER
);
CREATE TABLE clears (
    music_id   INTEGER NOT NULL,
    server     TEXT    NOT NULL DEFAULT 'jp',
    difficulty TEXT    NOT NULL,
    clear_type TEXT    NOT NULL,             -- CLEAR | FC | AP
    date       TEXT,                         -- ISO date, may be NULL
    date_is_upper_bound INTEGER DEFAULT 0,   -- 1 = "pre YYYY-MM-DD"
    note       TEXT,
    PRIMARY KEY (music_id, server, difficulty, clear_type)
);
CREATE TABLE aliases (
    music_id INTEGER PRIMARY KEY,
    romaji   TEXT                            -- e.g. Sekaipedia romanization
);

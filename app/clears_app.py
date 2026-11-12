"""
Log clears into proseka.sqlite.

Add-only, with one exception: you can fix the note on rows from the batch you
just submitted. Nothing is ever deleted; existing dates are never changed.

Ticks accumulate in a cart across searches; nothing hits the database until
"Submit batch". Cascade rule: AP implies FC implies CLEAR. Implied rows get
the same date and date_is_upper_bound = 1 ("no later than this").

Run:  streamlit run clears_app.py
"""
import sqlite3
from datetime import date

import pandas as pd
import streamlit as st

DB_PATH = "/home/momop/airflow-proseka/proseka.sqlite"
TIERS = ["CLEAR", "FC", "AP"]  # ordered low → high
DIFF_ORDER = ["easy", "normal", "hard", "expert", "master", "append"]
ROW_CAP = 300

st.set_page_config(page_title="proseka clears", layout="wide")
st.title("Log clears")

# cart: {(music_id, difficulty): {"title", "level", "ticked": set, "logged": set}}
cart = st.session_state.setdefault("cart", {})
# last_batch: [(music_id, difficulty, clear_type, title)] from the most recent submit
last_batch = st.session_state.setdefault("last_batch", [])


def query(sql, params=()):
    con = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(sql, con, params=params)
    finally:
        con.close()


def execute(sql, rows):
    con = sqlite3.connect(DB_PATH)
    try:
        before = con.total_changes
        con.executemany(sql, rows)
        con.commit()
        return con.total_changes - before
    finally:
        con.close()


if msg := st.session_state.pop("last_result", None):
    st.success(msg)

# ---------------------------------------------------------------- filters
c1, c2, c3, c4 = st.columns([1, 3, 2, 2])
server = c1.selectbox("server", ["jp", "en"])
search = c2.text_input("search title / kana", placeholder="てる, Tell Your, ...")

lo, hi = query("SELECT MIN(level) lo, MAX(level) hi FROM charts WHERE server=?", (server,)).iloc[0]
lo, hi = int(lo), int(hi)
levels = c3.slider("level", lo, hi, (min(26, hi), min(28, hi)))
diffs = c4.multiselect("difficulty", DIFF_ORDER, default=["expert", "master"])

# ------------------------------------------------------------------ rows
SQL = """
SELECT c.music_id, s.title, s.pronunciation, c.difficulty, c.level,
       MAX(CASE WHEN k.clear_type = 'CLEAR' THEN 1 ELSE 0 END) AS "CLEAR",
       MAX(CASE WHEN k.clear_type = 'FC'    THEN 1 ELSE 0 END) AS "FC",
       MAX(CASE WHEN k.clear_type = 'AP'    THEN 1 ELSE 0 END) AS "AP"
FROM charts c
JOIN songs s
  ON s.music_id = c.music_id AND s.server = c.server
LEFT JOIN clears k
  ON k.music_id = c.music_id AND k.server = c.server AND k.difficulty = c.difficulty
WHERE c.server = ?
  AND c.level BETWEEN ? AND ?
  AND (s.title LIKE ? OR s.pronunciation LIKE ?)
GROUP BY c.music_id, c.difficulty
ORDER BY c.level, s.pronunciation
"""
like = f"%{search}%"
db_rows = query(SQL, (server, levels[0], levels[1], like, like))
if diffs:
    db_rows = db_rows[db_rows["difficulty"].isin(diffs)].reset_index(drop=True)
for t in TIERS:
    db_rows[t] = db_rows[t].astype(bool)

if len(db_rows) > ROW_CAP:
    st.warning(f"{len(db_rows)} charts match; narrow the search or level range.")
elif db_rows.empty:
    st.info("No charts match.")
else:
    # Overlay pending ticks from the cart so they show as ticked in this view.
    view = db_rows.copy()
    for i, r in view.iterrows():
        pending = cart.get((int(r.music_id), r.difficulty))
        if pending:
            for t in pending["ticked"]:
                view.at[i, t] = True

    st.caption(
        f"{len(view)} charts. Already-logged boxes are ticked and can't be un-logged; "
        "new ticks go in the cart below until you submit."
    )
    edited = st.data_editor(
        view,
        hide_index=True,
        disabled=["music_id", "title", "pronunciation", "difficulty", "level"],
        column_config={
            "music_id": st.column_config.NumberColumn("id", width="small"),
            "title": st.column_config.TextColumn(width="large"),
            "pronunciation": st.column_config.TextColumn("kana", width="medium"),
            "difficulty": st.column_config.TextColumn(width="small"),
            "level": st.column_config.NumberColumn("lv", width="small"),
            **{t: st.column_config.CheckboxColumn(t, width="small") for t in TIERS},
        },
        key=f"editor|{server}|{search}|{levels}|{tuple(diffs)}",
    )

    # Sync this view's ticks into the cart (compare against DB state, not the overlay).
    for orig, row in zip(db_rows.itertuples(index=False), edited.itertuples(index=False)):
        key = (int(orig.music_id), orig.difficulty)
        ticked = {t for t in TIERS if getattr(row, t) and not getattr(orig, t)}
        if ticked:
            entry = cart.get(key, {"note": ""})
            entry.update(
                title=orig.title,
                level=int(orig.level),
                ticked=ticked,
                logged={t for t in TIERS if getattr(orig, t)},
            )
            cart[key] = entry
        else:
            cart.pop(key, None)

# ------------------------------------------------------------------ cart
st.divider()
st.subheader(f"Pending: {len(cart)} chart{'s' if len(cart) != 1 else ''}")

if cart:
    pending_df = pd.DataFrame(
        [
            {
                "music_id": k[0],
                "title": v["title"],
                "difficulty": k[1],
                "lv": v["level"],
                "ticked": " + ".join(t for t in TIERS if t in v["ticked"]),
                "note": v.get("note", ""),
            }
            for k, v in sorted(cart.items(), key=lambda kv: (kv[1]["level"], kv[1]["title"]))
        ]
    )
    pending_edited = st.data_editor(
        pending_df,
        hide_index=True,
        use_container_width=True,
        disabled=["music_id", "title", "difficulty", "lv", "ticked"],
        column_config={
            "music_id": st.column_config.NumberColumn("id", width="small"),
            "title": st.column_config.TextColumn(width="large"),
            "difficulty": st.column_config.TextColumn(width="small"),
            "lv": st.column_config.NumberColumn(width="small"),
            "ticked": st.column_config.TextColumn(width="small"),
            "note": st.column_config.TextColumn("note (per row)", width="large"),
        },
        key=f"pending|{tuple(sorted(cart))}",
    )
    for r in pending_edited.itertuples(index=False):
        cart[(int(r.music_id), r.difficulty)]["note"] = (r.note or "").strip()

    d1, d2 = st.columns([1, 3])
    the_date = d1.date_input("date for this batch", date.today())
    note = d2.text_input("default note (optional; used for rows with no note of their own)")

    b1, b2 = st.columns([1, 1])
    if b1.button("Submit batch", type="primary"):
        new = []  # (music_id, server, difficulty, clear_type, date, upper_bound, note)
        batch = []
        for (music_id, difficulty), v in cart.items():
            row_note = v.get("note") or note or None
            highest = max(TIERS.index(t) for t in v["ticked"])
            for t in TIERS[: highest + 1]:
                if t in v["logged"]:
                    continue  # already in the DB; its date stays
                explicit = t in v["ticked"]
                new.append((music_id, server, difficulty, t,
                            the_date.isoformat(), 0 if explicit else 1,
                            row_note if explicit else None))
                batch.append((music_id, difficulty, t, v["title"]))
        inserted = execute(
            """
            INSERT INTO clears
                (music_id, server, difficulty, clear_type, date, date_is_upper_bound, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(music_id, server, difficulty, clear_type) DO NOTHING
            """,
            new,
        )
        explicit_n = sum(1 for r in new if r[5] == 0)
        st.session_state["last_result"] = (
            f"{inserted} rows inserted on {the_date}: "
            f"{explicit_n} ticked, {inserted - explicit_n} implied by the cascade."
        )
        st.session_state["last_batch"] = batch
        st.session_state["cart"] = {}
        st.rerun()

    if b2.button("Clear pending"):
        st.session_state["cart"] = {}
        st.rerun()
else:
    st.caption("Tick boxes above. Ticks survive searching; they're only written on submit.")

# ------------------------------------------------------- fix a note (last batch only)
if last_batch:
    with st.expander(f"Fix a note on the last batch ({len(last_batch)} rows)"):
        labels = {f"{title} · {diff} · {tier}": (mid, diff, tier) for mid, diff, tier, title in last_batch}
        choice = st.selectbox("row", list(labels))
        mid, diff, tier = labels[choice]
        current = query(
            "SELECT note FROM clears WHERE music_id=? AND server=? AND difficulty=? AND clear_type=?",
            (mid, server, diff, tier),
        )
        current_note = current.iloc[0, 0] if not current.empty else None
        new_note = st.text_input("note", value=current_note or "")
        if st.button("Save note"):
            execute(
                "UPDATE clears SET note=? WHERE music_id=? AND server=? AND difficulty=? AND clear_type=?",
                [(new_note or None, mid, server, diff, tier)],
            )
            st.session_state["last_result"] = f"Note saved on {choice}."
            st.rerun()

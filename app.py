# app.py
# Referee Allocator (MVP) — Admin + Referee Portal + Offers + Blackouts + Printable PDFs

# ============================================================
# Imports
# ============================================================
import os
import sqlite3
import secrets
import smtplib
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from io import BytesIO

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from dateutil import parser as dtparser
from streamlit_autorefresh import st_autorefresh

# PDF (ReportLab)
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm

# ============================================================
# Scroll persistence helper (Admin tab refresh / autorefresh)
# ============================================================
def preserve_scroll(scroll_key: str = "refalloc_admin_scroll"):
    """
    Persists the page scroll position (window.scrollY) in localStorage and
    restores it after every Streamlit rerun (including st_autorefresh).

    IMPORTANT:
      - You must CALL preserve_scroll() inside the tab you want it to apply to.
    """
    components.html(
        f"""
        <script>
        (function() {{
          const KEY = "{scroll_key}";

          // Install scroll listener only once per browser page load
          if (!window.__refallocScrollInstalled) {{
            window.__refallocScrollInstalled = true;

            let ticking = false;
            window.addEventListener("scroll", function() {{
              if (!ticking) {{
                window.requestAnimationFrame(function() {{
                  try {{
                    localStorage.setItem(KEY, String(window.scrollY || 0));
                  }} catch (e) {{}}
                  ticking = false;
                }});
                ticking = true;
              }}
            }}, {{ passive: true }});
          }}

          function restore() {{
            let y = 0;
            try {{
              y = parseInt(localStorage.getItem(KEY) || "0", 10) || 0;
            }} catch (e) {{}}

            const maxY = Math.max(0, document.body.scrollHeight - window.innerHeight);
            if (y > maxY) y = maxY;
            window.scrollTo(0, y);
          }}

          // Multiple delayed restores handles layout changes
          window.setTimeout(restore, 0);
          window.setTimeout(restore, 80);
          window.setTimeout(restore, 200);
        }})();
        </script>
        """,
        height=0,
        width=0,
    )


# ============================================================
# CONFIG
# ============================================================
BASE_DIR = Path(__file__).resolve().parent

DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "league.db"))
Path(DB_PATH).expanduser().parent.mkdir(parents=True, exist_ok=True)

REF_PORTAL_ENABLED = os.getenv("REF_PORTAL_ENABLED", "false").lower() == "true"
DEBUG_BANNER = os.getenv("DEBUG_BANNER", "false").lower() == "true"

st.set_page_config(page_title="Referee Allocator (MVP)", layout="wide")

if DEBUG_BANNER:
    st.warning("DEBUG: App reached top of script ✅")
    st.write("DEBUG: query_params =", dict(st.query_params))
    st.write("DEBUG: session_state keys =", list(st.session_state.keys()))
    st.markdown("---")


# ============================================================
# ⚠️ CORE HELPERS — DO NOT MOVE BELOW THIS LINE
# ============================================================

# ============================================================
# Small utilities
# ============================================================
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _time_12h(dt: datetime) -> str:
    return dt.strftime("%I:%M %p").lstrip("0")


def game_local_date(game_row) -> date:
    """
    Returns the local calendar date for a game row.
    Expects game_row to have 'start_dt' (ISO string).
    """
    dt = dtparser.parse(game_row["start_dt"])
    return dt.date()


def status_badge(text: str, bg: str, fg: str = "white"):
    st.markdown(
        f"""
        <div style="
            display:inline-block;
            padding:6px 10px;
            border-radius:8px;
            background:{bg};
            color:{fg};
            font-weight:700;
            font-size:14px;
        ">
        {text}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# DB
# ============================================================
def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def ensure_referees_phone_column():
    """
    Safe migration: adds referees.phone if it doesn't exist.
    Works on existing DBs without data loss.
    """
    conn = db()
    try:
        cols = conn.execute("PRAGMA table_info(referees);").fetchall()
        col_names = {c["name"] for c in cols}
        if "phone" not in col_names:
            conn.execute("ALTER TABLE referees ADD COLUMN phone TEXT;")
            conn.commit()
    finally:
        conn.close()


def ensure_ladder_tables():
    """
    Safe migrations for ladder system:
    - teams: team name + division + opening_balance
    - game_results: one row per game with admin-entered scoring inputs + default flags
    """
    conn = db()
    try:
        cur = conn.cursor()

        # ---- teams ----
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                division TEXT NOT NULL,
                opening_balance INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        cols = conn.execute("PRAGMA table_info(teams);").fetchall()
        col_names = {c["name"] for c in cols}
        if "opening_balance" not in col_names:
            conn.execute(
                "ALTER TABLE teams ADD COLUMN opening_balance INTEGER NOT NULL DEFAULT 0;"
            )

        # ---- game_results ----
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS game_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER NOT NULL UNIQUE,

                home_score INTEGER NOT NULL DEFAULT 0,
                away_score INTEGER NOT NULL DEFAULT 0,

                home_female_tries INTEGER NOT NULL DEFAULT 0,
                away_female_tries INTEGER NOT NULL DEFAULT 0,

                home_conduct INTEGER NOT NULL DEFAULT 0,
                away_conduct INTEGER NOT NULL DEFAULT 0,

                home_unstripped INTEGER NOT NULL DEFAULT 0,
                away_unstripped INTEGER NOT NULL DEFAULT 0,

                home_defaulted INTEGER NOT NULL DEFAULT 0,
                away_defaulted INTEGER NOT NULL DEFAULT 0,

                updated_at TEXT NOT NULL,

                FOREIGN KEY(game_id) REFERENCES games(id) ON DELETE CASCADE
            );
            """
        )

        cols_gr = conn.execute("PRAGMA table_info(game_results);").fetchall()
        gr_names = {c["name"] for c in cols_gr}
        if "home_defaulted" not in gr_names:
            conn.execute(
                "ALTER TABLE game_results ADD COLUMN home_defaulted INTEGER NOT NULL DEFAULT 0;"
            )
        if "away_defaulted" not in gr_names:
            conn.execute(
                "ALTER TABLE game_results ADD COLUMN away_defaulted INTEGER NOT NULL DEFAULT 0;"
            )

        conn.commit()
    finally:
        conn.close()


# ============================================================
# Ladder / scoring helpers
# (KEEP THIS SINGLE BLOCK — remove all other duplicates)
# ============================================================

LADDER_WIN_PTS = 3
LADDER_DRAW_PTS = 2
LADDER_LOSS_PTS = 0

DIVISIONS = [
    "Division 1",
    "Division 2",
    "Division 3",
    "Golden Oldies",
    "Other",
]


def ensure_ladder_tables():
    """
    Safe migrations for ladder system:
    - teams: team name + division + opening_balance
    - game_results: one row per game with admin-entered scoring inputs (+ default flags)
    """
    conn = db()
    try:
        cur = conn.cursor()

        # ---- teams table ----
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                division TEXT NOT NULL,
                opening_balance INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        cols = conn.execute("PRAGMA table_info(teams);").fetchall()
        team_col_names = {c["name"] for c in cols}
        if "opening_balance" not in team_col_names:
            conn.execute(
                "ALTER TABLE teams ADD COLUMN opening_balance INTEGER NOT NULL DEFAULT 0;"
            )

        # ---- game_results table ----
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS game_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER NOT NULL UNIQUE,

                home_score INTEGER NOT NULL DEFAULT 0,
                away_score INTEGER NOT NULL DEFAULT 0,

                home_female_tries INTEGER NOT NULL DEFAULT 0,
                away_female_tries INTEGER NOT NULL DEFAULT 0,

                home_conduct INTEGER NOT NULL DEFAULT 0,   -- 0..10
                away_conduct INTEGER NOT NULL DEFAULT 0,   -- 0..10

                home_unstripped INTEGER NOT NULL DEFAULT 0,
                away_unstripped INTEGER NOT NULL DEFAULT 0,

                home_defaulted INTEGER NOT NULL DEFAULT 0,
                away_defaulted INTEGER NOT NULL DEFAULT 0,

                updated_at TEXT NOT NULL,

                FOREIGN KEY(game_id) REFERENCES games(id) ON DELETE CASCADE
            );
            """
        )

        # If game_results existed previously, add default columns safely
        cols = conn.execute("PRAGMA table_info(game_results);").fetchall()
        gr_col_names = {c["name"] for c in cols}
        if "home_defaulted" not in gr_col_names:
            conn.execute(
                "ALTER TABLE game_results ADD COLUMN home_defaulted INTEGER NOT NULL DEFAULT 0;"
            )
        if "away_defaulted" not in gr_col_names:
            conn.execute(
                "ALTER TABLE game_results ADD COLUMN away_defaulted INTEGER NOT NULL DEFAULT 0;"
            )

        conn.commit()
    finally:
        conn.close()


def upsert_team(name: str, division: str, opening_balance: int = 0):
    name = (name or "").strip()
    division = (division or "").strip()
    if not name or not division:
        return

    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO teams(name, division, opening_balance)
            VALUES (?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                division=excluded.division,
                opening_balance=excluded.opening_balance
            """,
            (name, division, int(opening_balance)),
        )
        conn.commit()
    finally:
        conn.close()


def list_teams() -> list[sqlite3.Row]:
    conn = db()
    rows = conn.execute(
        "SELECT id, name, division, opening_balance FROM teams ORDER BY division ASC, name ASC"
    ).fetchall()
    conn.close()
    return rows


def get_team_division(name: str) -> str:
    conn = db()
    row = conn.execute(
        "SELECT division FROM teams WHERE name=? LIMIT 1",
        ((name or "").strip(),),
    ).fetchone()
    conn.close()
    return (row["division"] if row else "").strip()


def get_team_opening_balance(name: str) -> int:
    conn = db()
    row = conn.execute(
        "SELECT opening_balance FROM teams WHERE name=? LIMIT 1",
        ((name or "").strip(),),
    ).fetchone()
    conn.close()
    return int(row["opening_balance"] or 0) if row else 0


def get_game_result(game_id: int) -> sqlite3.Row | None:
    conn = db()
    row = conn.execute(
        """
        SELECT
            game_id,
            home_score, away_score,
            home_female_tries, away_female_tries,
            home_conduct, away_conduct,
            home_unstripped, away_unstripped,
            COALESCE(home_defaulted,0) AS home_defaulted,
            COALESCE(away_defaulted,0) AS away_defaulted,
            updated_at
        FROM game_results
        WHERE game_id=?
        LIMIT 1
        """,
        (int(game_id),),
    ).fetchone()
    conn.close()
    return row


def upsert_game_result(
    *,
    game_id: int,
    home_score: int,
    away_score: int,
    home_female_tries: int,
    away_female_tries: int,
    home_conduct: int,
    away_conduct: int,
    home_unstripped: int,
    away_unstripped: int,
    home_defaulted: int = 0,
    away_defaulted: int = 0,
):
    home_defaulted = 1 if int(home_defaulted) else 0
    away_defaulted = 1 if int(away_defaulted) else 0
    if home_defaulted and away_defaulted:
        raise ValueError("Invalid default: both teams cannot be marked as DEFAULTED.")

    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO game_results(
                game_id,
                home_score, away_score,
                home_female_tries, away_female_tries,
                home_conduct, away_conduct,
                home_unstripped, away_unstripped,
                home_defaulted, away_defaulted,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(game_id) DO UPDATE SET
                home_score=excluded.home_score,
                away_score=excluded.away_score,
                home_female_tries=excluded.home_female_tries,
                away_female_tries=excluded.away_female_tries,
                home_conduct=excluded.home_conduct,
                away_conduct=excluded.away_conduct,
                home_unstripped=excluded.home_unstripped,
                away_unstripped=excluded.away_unstripped,
                home_defaulted=excluded.home_defaulted,
                away_defaulted=excluded.away_defaulted,
                updated_at=excluded.updated_at
            """,
            (
                int(game_id),
                int(home_score), int(away_score),
                int(home_female_tries), int(away_female_tries),
                int(home_conduct), int(away_conduct),
                int(home_unstripped), int(away_unstripped),
                int(home_defaulted), int(away_defaulted),
                now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def ladder_audit_df_for_date(selected_date: date) -> pd.DataFrame:
    """
    Per-team per-game audit rows for the selected date.
    DEFAULT rule:
      - defaulting team total = 10 (conduct only)
      - opponent total = 13
    """
    start_min = datetime.combine(selected_date, datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(selected_date + timedelta(days=1), datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    rows = conn.execute(
        """
        WITH base AS (
            SELECT
                g.id AS game_id,
                g.start_dt,
                g.field_name,
                g.home_team AS home_team,
                g.away_team AS away_team,
                COALESCE(t1.division,'') AS home_division,
                COALESCE(t2.division,'') AS away_division,
                COALESCE(gr.home_score,0) AS home_score,
                COALESCE(gr.away_score,0) AS away_score,
                COALESCE(gr.home_female_tries,0) AS home_female_tries,
                COALESCE(gr.away_female_tries,0) AS away_female_tries,
                COALESCE(gr.home_conduct,0) AS home_conduct,
                COALESCE(gr.away_conduct,0) AS away_conduct,
                COALESCE(gr.home_unstripped,0) AS home_unstripped,
                COALESCE(gr.away_unstripped,0) AS away_unstripped,
                COALESCE(gr.home_defaulted,0) AS home_defaulted,
                COALESCE(gr.away_defaulted,0) AS away_defaulted,
                gr.updated_at
            FROM games g
            LEFT JOIN teams t1 ON t1.name = g.home_team
            LEFT JOIN teams t2 ON t2.name = g.away_team
            LEFT JOIN game_results gr ON gr.game_id = g.id
            WHERE g.start_dt >= ? AND g.start_dt < ?
        ),
        teamsplit AS (
            SELECT
                game_id, start_dt, field_name,
                home_team AS team,
                away_team AS opponent,
                home_division AS division,
                home_score AS pf,
                away_score AS pa,
                home_female_tries AS female_tries,
                home_conduct AS conduct,
                home_unstripped AS unstripped,
                home_defaulted AS defaulted,
                away_defaulted AS opponent_defaulted,
                updated_at
            FROM base
            UNION ALL
            SELECT
                game_id, start_dt, field_name,
                away_team AS team,
                home_team AS opponent,
                away_division AS division,
                away_score AS pf,
                home_score AS pa,
                away_female_tries AS female_tries,
                away_conduct AS conduct,
                away_unstripped AS unstripped,
                away_defaulted AS defaulted,
                home_defaulted AS opponent_defaulted,
                updated_at
            FROM base
        )
        SELECT
            game_id,
            start_dt,
            field_name,
            division,
            team,
            opponent,
            pf,
            pa,
            (pf - pa) AS margin,

            CASE
              WHEN pf > pa THEN 'W'
              WHEN pf = pa THEN 'D'
              ELSE 'L'
            END AS result,

            CASE
              WHEN pf > pa THEN ?
              WHEN pf = pa THEN ?
              ELSE ?
            END AS match_pts,

            CASE
              WHEN pf < pa AND (pa - pf) IN (1,2) THEN 1 ELSE 0
            END AS close_loss_bp,

            female_tries,
            CASE WHEN female_tries >= 4 THEN 1 ELSE 0 END AS female_bp,

            conduct,

            unstripped,
            CASE WHEN unstripped >= 3 THEN -2 ELSE 0 END AS unstripped_pen,

            defaulted,
            opponent_defaulted,

            updated_at
        FROM teamsplit
        ORDER BY start_dt ASC, field_name ASC, team ASC
        """,
        (start_min, start_max, LADDER_WIN_PTS, LADDER_DRAW_PTS, LADDER_LOSS_PTS),
    ).fetchall()
    conn.close()

    out = []
    for r in rows:
        match_pts = int(r["match_pts"] or 0)
        close_bp = int(r["close_loss_bp"] or 0)
        female_bp = int(r["female_bp"] or 0)
        conduct = int(r["conduct"] or 0)
        pen = int(r["unstripped_pen"] or 0)

        defaulted = int(r["defaulted"] or 0)
        opponent_defaulted = int(r["opponent_defaulted"] or 0)

        if defaulted == 1:
            # Defaulting team: ONLY 10 conduct points
            match_pts, close_bp, female_bp, pen = 0, 0, 0, 0
            conduct = 10
            result = "L"
            total = 10
        elif opponent_defaulted == 1:
            # Opponent of defaulting team: 13 total automatically
            match_pts, close_bp, female_bp, pen = 3, 0, 0, 0
            conduct = 10
            result = "W"
            total = 13
        else:
            result = r["result"] or "—"
            total = match_pts + close_bp + female_bp + conduct + pen

        out.append(
            {
                "Start": _time_12h(dtparser.parse(r["start_dt"])) if r["start_dt"] else "—",
                "Field": r["field_name"] or "—",
                "Division": (r["division"] or "").strip() or "—",
                "Team": r["team"] or "—",
                "Opponent": r["opponent"] or "—",
                "PF": int(r["pf"] or 0),
                "PA": int(r["pa"] or 0),
                "Res": result,
                "Match": match_pts,
                "CloseBP": close_bp,
                "FemTries": int(r["female_tries"] or 0),
                "FemBP": female_bp,
                "Conduct": conduct,
                "Unstrip": int(r["unstripped"] or 0),
                "Pen": pen,
                "Total": total,
                "Defaulted": "YES" if defaulted == 1 else ("OPP DEFAULTED" if opponent_defaulted == 1 else ""),
                "Updated": (r["updated_at"] or "")[:19],
            }
        )

    return pd.DataFrame(out)


def ladder_table_df_for_date(selected_date: date, division: str) -> pd.DataFrame:
    """
    Aggregated ladder for the selected date only.
    Includes opening_balance.
    """
    df = ladder_audit_df_for_date(selected_date)
    if df.empty:
        return pd.DataFrame()

    div = (division or "").strip()
    if div:
        df = df[df["Division"].fillna("—") == div]
    if df.empty:
        return pd.DataFrame()

    grouped = df.groupby("Team", dropna=False).agg(
        P=("Team", "count"),
        W=("Res", lambda s: int((s == "W").sum())),
        D=("Res", lambda s: int((s == "D").sum())),
        L=("Res", lambda s: int((s == "L").sum())),
        PF=("PF", "sum"),
        PA=("PA", "sum"),
        Match=("Match", "sum"),
        CloseBP=("CloseBP", "sum"),
        FemBP=("FemBP", "sum"),
        Conduct=("Conduct", "sum"),
        Pen=("Pen", "sum"),
        Total=("Total", "sum"),
    ).reset_index()

    grouped["PD"] = grouped["PF"] - grouped["PA"]

    # Opening balance lookup
    grouped["Opening"] = grouped["Team"].apply(get_team_opening_balance)
    grouped["Total+Opening"] = grouped["Total"] + grouped["Opening"]

    grouped = grouped.sort_values(
        by=["Total+Opening", "PD", "PF", "Team"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    grouped = grouped[
        ["Team", "P", "W", "D", "L", "PF", "PA", "PD", "Opening", "Total", "Total+Opening"]
    ]
    return grouped


def ladder_validation_warnings_for_date(selected_date: date) -> list[str]:
    warnings: list[str] = []

    games = get_games()
    todays = [g for g in games if game_local_date(g) == selected_date]
    if not todays:
        return warnings

    missing_div = set()
    mismatch_div_games = []

    for g in todays:
        h = (g["home_team"] or "").strip()
        a = (g["away_team"] or "").strip()
        dh = get_team_division(h)
        da = get_team_division(a)

        if not dh:
            missing_div.add(h)
        if not da:
            missing_div.add(a)
        if dh and da and dh != da:
            mismatch_div_games.append(f"{h} vs {a} ({dh} / {da})")

    if missing_div:
        warnings.append("Missing team division for: " + ", ".join(sorted(missing_div)))

    if mismatch_div_games:
        warnings.append("Cross-division games detected: " + "; ".join(mismatch_div_games))

    for g in todays:
        gr = get_game_result(int(g["id"]))
        if not gr:
            warnings.append(f"Missing result entry: {g['home_team']} vs {g['away_team']}")
            continue

        hd = int(gr["home_defaulted"] or 0)
        ad = int(gr["away_defaulted"] or 0)
        if hd and ad:
            warnings.append(f"Invalid default flags (both sides): {g['home_team']} vs {g['away_team']}")

        if not (hd or ad):
            for side in ("home", "away"):
                c = int(gr[f"{side}_conduct"] or 0)
                if c < 0 or c > 10:
                    warnings.append(
                        f"Conduct out of range (0-10): {g['home_team']} vs {g['away_team']} ({side}={c})"
                    )

        for k in [
            "home_score", "away_score",
            "home_female_tries", "away_female_tries",
            "home_unstripped", "away_unstripped",
        ]:
            v = int(gr[k] or 0)
            if v < 0:
                warnings.append(f"Negative value {k} for game: {g['home_team']} vs {g['away_team']}")

    return warnings


def ensure_referees_phone_column():
    """
    Safe migration: adds referees.phone if it doesn't exist.
    """
    conn = db()
    try:
        cols = conn.execute("PRAGMA table_info(referees);").fetchall()
        col_names = {c["name"] for c in cols}
        if "phone" not in col_names:
            conn.execute("ALTER TABLE referees ADD COLUMN phone TEXT;")
            conn.commit()
    finally:
        conn.close()


def ensure_ladder_tables():
    """
    Safe migrations for ladder system:
    - teams: team name + division + opening_balance
    - game_results: one row per game (scores + referee inputs + default flags)
    """
    conn = db()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                division TEXT NOT NULL,
                opening_balance INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        cols = conn.execute("PRAGMA table_info(teams);").fetchall()
        col_names = {c["name"] for c in cols}
        if "opening_balance" not in col_names:
            conn.execute(
                "ALTER TABLE teams ADD COLUMN opening_balance INTEGER NOT NULL DEFAULT 0;"
            )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS game_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER NOT NULL UNIQUE,

                home_score INTEGER NOT NULL DEFAULT 0,
                away_score INTEGER NOT NULL DEFAULT 0,

                home_female_tries INTEGER NOT NULL DEFAULT 0,
                away_female_tries INTEGER NOT NULL DEFAULT 0,

                home_conduct INTEGER NOT NULL DEFAULT 0,
                away_conduct INTEGER NOT NULL DEFAULT 0,

                home_unstripped INTEGER NOT NULL DEFAULT 0,
                away_unstripped INTEGER NOT NULL DEFAULT 0,

                home_defaulted INTEGER NOT NULL DEFAULT 0,
                away_defaulted INTEGER NOT NULL DEFAULT 0,

                updated_at TEXT NOT NULL,

                FOREIGN KEY(game_id) REFERENCES games(id) ON DELETE CASCADE
            );
            """
        )

        cols_gr = conn.execute("PRAGMA table_info(game_results);").fetchall()
        gr_names = {c["name"] for c in cols_gr}
        if "home_defaulted" not in gr_names:
            conn.execute(
                "ALTER TABLE game_results ADD COLUMN home_defaulted INTEGER NOT NULL DEFAULT 0;"
            )
        if "away_defaulted" not in gr_names:
            conn.execute(
                "ALTER TABLE game_results ADD COLUMN away_defaulted INTEGER NOT NULL DEFAULT 0;"
            )

        conn.commit()
    finally:
        conn.close()


# ============================================================
# init_db (KEEP THIS SINGLE VERSION — remove duplicates)
# ============================================================
def init_db():
    """
    Creates core tables (referees/games/assignments/offers/blackouts/admin auth)
    then runs safe migrations (phone column + ladder tables).

    This order matters on a fresh DB.
    """
    conn = db()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS referees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                active INTEGER NOT NULL DEFAULT 1,
                phone TEXT
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_key TEXT NOT NULL UNIQUE,
                home_team TEXT NOT NULL,
                away_team TEXT NOT NULL,
                field_name TEXT NOT NULL,
                start_dt TEXT NOT NULL
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER NOT NULL,
                slot_no INTEGER NOT NULL,
                referee_id INTEGER,
                status TEXT NOT NULL DEFAULT 'EMPTY',
                updated_at TEXT NOT NULL,
                UNIQUE(game_id, slot_no),
                FOREIGN KEY(game_id) REFERENCES games(id),
                FOREIGN KEY(referee_id) REFERENCES referees(id)
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS offers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                assignment_id INTEGER NOT NULL,
                token TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                responded_at TEXT,
                response TEXT,
                FOREIGN KEY(assignment_id) REFERENCES assignments(id)
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS blackouts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referee_id INTEGER NOT NULL,
                blackout_date TEXT NOT NULL,
                UNIQUE(referee_id, blackout_date),
                FOREIGN KEY(referee_id) REFERENCES referees(id)
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                token TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used_at TEXT
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                token TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked_at TEXT
            );
            """
        )

        conn.commit()
    finally:
        conn.close()

    # Safe migrations (depend on the tables existing)
    ensure_referees_phone_column()
    ensure_ladder_tables()


# ============================================================
# Email (SMTP)
# ============================================================
def smtp_settings():
    """
    Required:
      SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD,
      SMTP_FROM_EMAIL, SMTP_FROM_NAME, APP_BASE_URL
    """
    secrets_dict = {}

    secrets_paths = [
        "/opt/render/.streamlit/secrets.toml",
        "/opt/render/project/src/.streamlit/secrets.toml",
        str(BASE_DIR / ".streamlit" / "secrets.toml"),
    ]
    if any(os.path.exists(p) for p in secrets_paths):
        try:
            secrets_dict = dict(st.secrets)
        except Exception:
            secrets_dict = {}

    def get(key: str, default: str = "") -> str:
        return os.environ.get(key, str(secrets_dict.get(key, default)))

    return {
        "host": get("SMTP_HOST", ""),
        "port": int(get("SMTP_PORT", "587") or 587),
        "user": get("SMTP_USER", ""),
        "password": get("SMTP_PASSWORD", ""),
        "from_email": get("SMTP_FROM_EMAIL", ""),
        "from_name": get("SMTP_FROM_NAME", "Referee Allocator"),
        "app_base_url": get("APP_BASE_URL", "").rstrip("/"),
    }


def send_html_email(
    to_email: str,
    to_name: str,
    subject: str,
    html_body: str,
    text_body: str | None = None,
):
    cfg = smtp_settings()
    if not (
        cfg["host"]
        and cfg["user"]
        and cfg["password"]
        and cfg["from_email"]
        and cfg["app_base_url"]
    ):
        raise RuntimeError(
            "Email not configured. Set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, "
            "SMTP_FROM_EMAIL, SMTP_FROM_NAME, APP_BASE_URL."
        )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f'{cfg["from_name"]} <{cfg["from_email"]}>'
    msg["To"] = f"{to_name} <{to_email}>"

    if not text_body:
        text_body = "You have a notification from Referee Allocator."
    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(cfg["host"], cfg["port"]) as server:
        server.starttls()
        server.login(cfg["user"], cfg["password"])
        server.sendmail(cfg["from_email"], [to_email], msg.as_string())


# ============================================================
# Admin auth
# ============================================================
def admin_count() -> int:
    conn = db()
    row = conn.execute("SELECT COUNT(*) AS c FROM admins").fetchone()
    conn.close()
    return int(row["c"])


def add_admin(email: str):
    email = email.strip().lower()
    conn = db()
    conn.execute(
        "INSERT OR IGNORE INTO admins(email, active, created_at) VALUES (?, 1, ?)",
        (email, now_iso()),
    )
    conn.commit()
    conn.close()


def is_admin_email_allowed(email: str) -> bool:
    conn = db()
    row = conn.execute(
        "SELECT 1 FROM admins WHERE email=? AND active=1 LIMIT 1",
        (email.strip().lower(),),
    ).fetchone()
    conn.close()
    return bool(row)


def set_admin_active(email: str, active: bool):
    conn = db()
    conn.execute(
        "UPDATE admins SET active=? WHERE email=?",
        (1 if active else 0, email.strip().lower()),
    )
    conn.commit()
    conn.close()


def list_admins():
    conn = db()
    rows = conn.execute(
        "SELECT email, active, created_at FROM admins ORDER BY email ASC"
    ).fetchall()
    conn.close()
    return rows


def create_admin_login_token(email: str, minutes_valid: int = 15) -> str:
    token = secrets.token_urlsafe(32)
    created = datetime.now(timezone.utc)
    expires = created + timedelta(minutes=minutes_valid)

    conn = db()
    conn.execute(
        """
        INSERT INTO admin_tokens(email, token, created_at, expires_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            email.strip().lower(),
            token,
            created.isoformat(timespec="seconds"),
            expires.isoformat(timespec="seconds"),
        ),
    )
    conn.commit()
    conn.close()
    return token


def consume_admin_token(token: str) -> tuple[bool, str]:
    conn = db()
    row = conn.execute(
        """
        SELECT id, email, expires_at, used_at
        FROM admin_tokens
        WHERE token=?
        """,
        (token,),
    ).fetchone()

    if not row:
        conn.close()
        return False, "Invalid or unknown login link."

    if row["used_at"] is not None:
        conn.close()
        return False, "This login link has already been used."

    expires_at = dtparser.parse(row["expires_at"])
    if datetime.now(timezone.utc) > expires_at:
        conn.close()
        return False, "This login link has expired. Please request a new one."

    email = row["email"].strip().lower()
    if not is_admin_email_allowed(email):
        conn.close()
        return False, "This email is not an authorised administrator."

    conn.execute("UPDATE admin_tokens SET used_at=? WHERE id=?", (now_iso(), row["id"]))
    conn.commit()
    conn.close()
    return True, email


def create_admin_session(email: str, days_valid: int = 14) -> str:
    token = secrets.token_urlsafe(32)
    created = datetime.now(timezone.utc)
    expires = created + timedelta(days=days_valid)

    conn = db()
    conn.execute(
        """
        INSERT INTO admin_sessions(email, token, created_at, expires_at, revoked_at)
        VALUES (?, ?, ?, ?, NULL)
        """,
        (
            email.strip().lower(),
            token,
            created.isoformat(timespec="seconds"),
            expires.isoformat(timespec="seconds"),
        ),
    )
    conn.commit()
    conn.close()
    return token


def consume_admin_session(token: str) -> tuple[bool, str]:
    conn = db()
    row = conn.execute(
        """
        SELECT email, expires_at, revoked_at
        FROM admin_sessions
        WHERE token=?
        LIMIT 1
        """,
        (token,),
    ).fetchone()

    if not row:
        conn.close()
        return False, "Invalid session."

    if row["revoked_at"] is not None:
        conn.close()
        return False, "Session revoked."

    if datetime.now(timezone.utc) > dtparser.parse(row["expires_at"]):
        conn.close()
        return False, "Session expired."

    email = row["email"].strip().lower()
    if not is_admin_email_allowed(email):
        conn.close()
        return False, "Not authorised."

    conn.close()
    return True, email


def maybe_restore_admin_from_session_param():
    qp = st.query_params
    token = qp.get("session")
    if token and not st.session_state.get("admin_email"):
        ok, value = consume_admin_session(token)
        if ok:
            st.session_state["admin_email"] = value
        else:
            st.query_params.pop("session", None)
            st.rerun()


def send_admin_login_email(email: str) -> str:
    email = email.strip().lower()
    cfg = smtp_settings()
    base = cfg.get("app_base_url", "").rstrip("/")
    token = create_admin_login_token(email)
    login_url = f"{base}/?admin_login=1&token={token}"

    subject = "Admin login link"
    text = (
        "Use this link to sign in as an administrator (expires in 15 minutes):\n"
        f"{login_url}\n"
    )
    html = f"""
    <div style="font-family: Arial, sans-serif; line-height: 1.4;">
      <p>Hi,</p>
      <p>Use the button below to sign in as an administrator.
         This link expires in <b>15 minutes</b>.</p>
      <p>
        <a href="{login_url}" style="display:inline-block;padding:10px 14px;background:#1565c0;color:#fff;text-decoration:none;border-radius:6px;">
          Sign in
        </a>
      </p>
      <p>If you didn’t request this, you can ignore this email.</p>
    </div>
    """
    send_html_email(email, email, subject, html, text_body=text)
    return login_url


def handle_admin_login_via_query_params():
    qp = st.query_params
    if qp.get("admin_login") == "1" and qp.get("token"):
        token = qp.get("token")
        ok, value = consume_admin_token(token)
        if ok:
            st.session_state["admin_email"] = value
            session_token = create_admin_session(value)

            st.query_params.pop("admin_login", None)
            st.query_params.pop("token", None)
            st.query_params["session"] = session_token
            st.rerun()
        else:
            st.title("Admin Login")
            st.error(value)
            st.info("Please go back and request a new login link.")
            st.stop()


def admin_logout_button():
    if st.session_state.get("admin_email"):
        c1, c2 = st.columns([3, 1])
        with c1:
            st.caption(f"Logged in as: {st.session_state['admin_email']}")
        with c2:
            if st.button("Log out"):
                st.session_state.pop("admin_email", None)
                st.query_params.pop("session", None)
                st.rerun()

# ============================================================
# Imports & data helpers
# ============================================================
def import_referees_csv(df: pd.DataFrame):
    cols = {c.lower().strip(): c for c in df.columns}
    if "name" not in cols or "email" not in cols:
        raise ValueError("Referees CSV must contain columns: name, email")

    phone_col = cols.get("phone")  # optional

    conn = db()
    cur = conn.cursor()
    added = 0
    updated = 0

    for _, row in df.iterrows():
        name = str(row[cols["name"]]).strip()
        email = str(row[cols["email"]]).strip().lower()
        phone = ""
        if phone_col:
            phone = str(row[phone_col]).strip()
            if phone.lower() == "nan":
                phone = ""

        if not name or not email or email == "nan":
            continue

        cur.execute("SELECT id, name, COALESCE(phone,'') AS phone FROM referees WHERE email=?", (email,))
        existing = cur.fetchone()

        if existing:
            needs_update = False
            if existing["name"] != name:
                needs_update = True
            if phone_col and (existing["phone"] or "") != phone:
                needs_update = True

            if needs_update:
                cur.execute(
                    "UPDATE referees SET name=?, phone=? WHERE email=?",
                    (name, phone, email),
                )
                updated += 1
        else:
            cur.execute(
                "INSERT INTO referees(name, email, phone, active) VALUES (?, ?, ?, 1)",
                (name, email, phone),
            )
            added += 1

    conn.commit()
    conn.close()
    return added, updated


def replace_referees_csv(df: pd.DataFrame):
    cols = {c.lower().strip(): c for c in df.columns}
    if "name" not in cols or "email" not in cols:
        raise ValueError("Referees CSV must contain columns: name, email")

    phone_col = cols.get("phone")  # optional

    new_refs = []
    for _, row in df.iterrows():
        name = str(row[cols["name"]]).strip()
        email = str(row[cols["email"]]).strip().lower()

        phone = ""
        if phone_col:
            phone = str(row[phone_col]).strip()
            if phone.lower() == "nan":
                phone = ""

        if not name or not email or email == "nan":
            continue

        new_refs.append((name, email, phone))

    if len(new_refs) == 0:
        raise ValueError("Referees CSV has no valid rows. Aborting replace import (nothing deleted).")

    conn = db()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute("DELETE FROM offers")
        cur.execute(
            """
            UPDATE assignments
            SET referee_id=NULL, status='EMPTY', updated_at=?
            """,
            (now_iso(),),
        )
        cur.execute("DELETE FROM blackouts")
        cur.execute("DELETE FROM referees")

        try:
            cur.execute("DELETE FROM sqlite_sequence WHERE name IN ('referees','blackouts','offers')")
        except Exception:
            pass

        cur.executemany(
            "INSERT INTO referees(name, email, phone, active) VALUES (?, ?, ?, 1)",
            new_refs,
        )
        conn.commit()
        return len(new_refs)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def import_games_csv(df: pd.DataFrame):
    cols = {c.lower().strip(): c for c in df.columns}

    key_col = None
    for k in ["game_id", "game_key", "id"]:
        if k in cols:
            key_col = cols[k]
            break

    has_date_time = "date" in cols and "start_time" in cols
    has_start_dt = "start_datetime" in cols

    required = ["home_team", "away_team", "field"]
    missing = [r for r in required if r not in cols]
    if not key_col:
        missing.append("game_id")
    if not (has_date_time or has_start_dt):
        missing.append("date + start_time (or start_datetime)")

    if missing:
        raise ValueError(f"Games CSV missing columns: {', '.join(missing)}")

    conn = db()
    cur = conn.cursor()

    inserted, updated = 0, 0
    for _, row in df.iterrows():
        game_key = str(row[key_col]).strip()
        home = str(row[cols["home_team"]]).strip()
        away = str(row[cols["away_team"]]).strip()
        field = str(row[cols["field"]]).strip()

        if not (game_key and home and away and field) or game_key == "nan":
            continue

        try:
            if has_date_time:
                d_raw = str(row[cols["date"]]).strip()
                t_raw = str(row[cols["start_time"]]).strip()
                start_dt = dtparser.parse(f"{d_raw} {t_raw}")
            else:
                start_raw = str(row[cols["start_datetime"]]).strip()
                start_dt = dtparser.parse(start_raw)
        except Exception as e:
            raise ValueError(f"Could not parse date/time for game_id={game_key}: {e}")

        start_iso = start_dt.isoformat(timespec="minutes")

        cur.execute("SELECT id FROM games WHERE game_key=?", (game_key,))
        g = cur.fetchone()

        if g:
            cur.execute(
                """
                UPDATE games
                SET home_team=?, away_team=?, field_name=?, start_dt=?
                WHERE game_key=?
                """,
                (home, away, field, start_iso, game_key),
            )
            updated += 1
            game_id = g["id"]
        else:
            cur.execute(
                """
                INSERT INTO games(game_key, home_team, away_team, field_name, start_dt)
                VALUES (?, ?, ?, ?, ?)
                """,
                (game_key, home, away, field, start_iso),
            )
            inserted += 1
            game_id = cur.lastrowid

        for slot in (1, 2):
            cur.execute(
                "SELECT 1 FROM assignments WHERE game_id=? AND slot_no=?",
                (game_id, slot),
            )
            if not cur.fetchone():
                cur.execute(
                    """
                    INSERT INTO assignments(game_id, slot_no, referee_id, status, updated_at)
                    VALUES (?, ?, NULL, 'EMPTY', ?)
                    """,
                    (game_id, slot, now_iso()),
                )

    conn.commit()
    conn.close()
    return inserted, updated


def import_blackouts_csv(df: pd.DataFrame):
    cols = {c.lower().strip(): c for c in df.columns}
    if "email" not in cols or "blackout_date" not in cols:
        raise ValueError("Blackouts CSV must contain columns: email, blackout_date")

    conn = db()
    cur = conn.cursor()

    added, skipped = 0, 0
    for _, row in df.iterrows():
        email = str(row[cols["email"]]).strip().lower()
        d_raw = str(row[cols["blackout_date"]]).strip()
        if not email or email == "nan" or not d_raw or d_raw == "nan":
            continue

        cur.execute("SELECT id FROM referees WHERE email=?", (email,))
        r = cur.fetchone()
        if not r:
            skipped += 1
            continue

        try:
            d = dtparser.parse(d_raw).date()
        except Exception:
            raise ValueError(f"Could not parse blackout_date '{d_raw}' for {email}")

        try:
            cur.execute(
                "INSERT INTO blackouts(referee_id, blackout_date) VALUES (?, ?)",
                (r["id"], d.isoformat()),
            )
            added += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    return added, skipped


def get_games():
    conn = db()
    rows = conn.execute(
        """
        SELECT id, game_key, home_team, away_team, field_name, start_dt
        FROM games
        ORDER BY start_dt ASC
        """
    ).fetchall()
    conn.close()
    return rows


def get_referees():
    conn = db()
    rows = conn.execute(
        """
        SELECT id, name, email, COALESCE(phone,'') AS phone
        FROM referees
        WHERE active=1
        ORDER BY name ASC
        """
    ).fetchall()
    conn.close()
    return rows



def get_assignments_for_game(game_id: int):
    conn = db()
    rows = conn.execute(
        """
        SELECT
            a.id,
            a.slot_no,
            a.referee_id,
            a.status,
            a.updated_at,
            r.name AS ref_name,
            r.email AS ref_email
        FROM assignments a
        LEFT JOIN referees r ON r.id = a.referee_id
        WHERE a.game_id=?
        ORDER BY a.slot_no ASC
        """,
        (game_id,),
    ).fetchall()
    conn.close()
    return rows

def get_assignment_live(assignment_id: int):
    """
    Re-fetch the latest assignment row (and linked referee details) from the DB.

    Used inside Action handlers so we don't rely on stale 'a' from the UI loop.
    Returns sqlite3.Row or None.
    """
    conn = db()
    row = conn.execute(
        """
        SELECT
            a.id,
            a.game_id,
            a.slot_no,
            a.referee_id,
            a.status,
            a.updated_at,
            r.name  AS ref_name,
            r.email AS ref_email,
            COALESCE(r.phone,'') AS ref_phone
        FROM assignments a
        LEFT JOIN referees r ON r.id = a.referee_id
        WHERE a.id = ?
        LIMIT 1
        """,
        (int(assignment_id),),
    ).fetchone()
    conn.close()
    return row

# ============================================================
# Assignment helpers
# ============================================================

def set_assignment_status(assignment_id: int, status: str):
    status = (status or "EMPTY").strip().upper()
    conn = db()
    conn.execute(
        """
        UPDATE assignments
        SET status=?, updated_at=?
        WHERE id=?
        """,
        (status, now_iso(), int(assignment_id)),
    )
    conn.commit()
    conn.close()


def set_assignment_ref(assignment_id: int, referee_id: int):
    """
    Set the referee for a slot.
    If a referee is set and the slot was EMPTY, we move it to NOT_OFFERED
    (your UI treats any non-empty referee as 'NOT OFFERED YET' unless OFFERED/DECLINED/ACCEPTED/ASSIGNED).
    """
    conn = db()

    # Keep status consistent with your UI badges
    cur = conn.execute(
        "SELECT referee_id, status FROM assignments WHERE id=? LIMIT 1",
        (int(assignment_id),),
    ).fetchone()

    if not cur:
        conn.close()
        return

    cur_status = (cur["status"] or "EMPTY").strip().upper()
    new_status = cur_status

    if cur_status == "EMPTY":
        new_status = "NOT_OFFERED"

    conn.execute(
        """
        UPDATE assignments
        SET referee_id=?, status=?, updated_at=?
        WHERE id=?
        """,
        (int(referee_id), new_status, now_iso(), int(assignment_id)),
    )
    conn.commit()
    conn.close()


def _delete_offers_for_assignment(conn: sqlite3.Connection, assignment_id: int):
    """
    Internal helper: ensure no stale offer links remain for this assignment.
    """
    conn.execute("DELETE FROM offers WHERE assignment_id=?", (int(assignment_id),))


def clear_assignment(assignment_id: int):
    """
    Clears the slot back to EMPTY and removes any offers for that assignment.
    This is what your RESET/DELETE action needs.
    """
    conn = db()
    try:
        _delete_offers_for_assignment(conn, assignment_id)
        conn.execute(
            """
            UPDATE assignments
            SET referee_id=NULL, status='EMPTY', updated_at=?
            WHERE id=?
            """,
            (now_iso(), int(assignment_id)),
        )
        conn.commit()
    finally:
        conn.close()

        
# ============================================================
# Ladder / scoring helpers
# ============================================================

LADDER_WIN_PTS = 3
LADDER_DRAW_PTS = 2
LADDER_LOSS_PTS = 0

DIVISIONS = [
    "Division 1",
    "Division 2",
    "Division 3",
    "Golden Oldies",
    "Other",
]


def ensure_game_results_default_columns():
    """
    Safe migration: add default columns to game_results if missing.
    """
    conn = db()
    try:
        cols = conn.execute("PRAGMA table_info(game_results);").fetchall()
        col_names = {c["name"] for c in cols}

        if "home_defaulted" not in col_names:
            conn.execute(
                "ALTER TABLE game_results ADD COLUMN home_defaulted INTEGER NOT NULL DEFAULT 0;"
            )
        if "away_defaulted" not in col_names:
            conn.execute(
                "ALTER TABLE game_results ADD COLUMN away_defaulted INTEGER NOT NULL DEFAULT 0;"
            )

        conn.commit()
    finally:
        conn.close()


def upsert_team(name: str, division: str, opening_balance: int = 0):
    """
    Insert/update team division + opening_balance (points carried in before this system started).
    Requires teams.opening_balance column.
    """
    name = (name or "").strip()
    division = (division or "").strip()
    if not name or not division:
        return

    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO teams(name, division, opening_balance)
            VALUES (?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                division=excluded.division,
                opening_balance=excluded.opening_balance
            """,
            (name, division, int(opening_balance or 0)),
        )
        conn.commit()
    finally:
        conn.close()


def list_teams() -> list[sqlite3.Row]:
    conn = db()
    rows = conn.execute(
        """
        SELECT id, name, division, COALESCE(opening_balance,0) AS opening_balance
        FROM teams
        ORDER BY division ASC, name ASC
        """
    ).fetchall()
    conn.close()
    return rows


def get_team_division(name: str) -> str:
    conn = db()
    row = conn.execute(
        "SELECT division FROM teams WHERE name=? LIMIT 1",
        ((name or "").strip(),),
    ).fetchone()
    conn.close()
    return (row["division"] if row else "").strip()


def get_team_opening_balance(name: str) -> int:
    conn = db()
    row = conn.execute(
        "SELECT COALESCE(opening_balance,0) AS opening_balance FROM teams WHERE name=? LIMIT 1",
        ((name or "").strip(),),
    ).fetchone()
    conn.close()
    return int(row["opening_balance"] or 0) if row else 0


def get_game_result(game_id: int) -> sqlite3.Row | None:
    conn = db()
    row = conn.execute(
        """
        SELECT
            game_id,
            home_score, away_score,
            home_female_tries, away_female_tries,
            home_conduct, away_conduct,
            home_unstripped, away_unstripped,
            COALESCE(home_defaulted,0) AS home_defaulted,
            COALESCE(away_defaulted,0) AS away_defaulted,
            updated_at
        FROM game_results
        WHERE game_id=?
        LIMIT 1
        """,
        (game_id,),
    ).fetchone()
    conn.close()
    return row


def upsert_game_result(
    *,
    game_id: int,
    home_score: int,
    away_score: int,
    home_female_tries: int,
    away_female_tries: int,
    home_conduct: int,
    away_conduct: int,
    home_unstripped: int,
    away_unstripped: int,
    home_defaulted: int = 0,
    away_defaulted: int = 0,
):
    home_defaulted = 1 if int(home_defaulted) else 0
    away_defaulted = 1 if int(away_defaulted) else 0
    if home_defaulted and away_defaulted:
        raise ValueError("Invalid default: both teams cannot be marked as DEFAULTED.")

    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO game_results(
                game_id,
                home_score, away_score,
                home_female_tries, away_female_tries,
                home_conduct, away_conduct,
                home_unstripped, away_unstripped,
                home_defaulted, away_defaulted,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(game_id) DO UPDATE SET
                home_score=excluded.home_score,
                away_score=excluded.away_score,
                home_female_tries=excluded.home_female_tries,
                away_female_tries=excluded.away_female_tries,
                home_conduct=excluded.home_conduct,
                away_conduct=excluded.away_conduct,
                home_unstripped=excluded.home_unstripped,
                away_unstripped=excluded.away_unstripped,
                home_defaulted=excluded.home_defaulted,
                away_defaulted=excluded.away_defaulted,
                updated_at=excluded.updated_at
            """,
            (
                int(game_id),
                int(home_score), int(away_score),
                int(home_female_tries), int(away_female_tries),
                int(home_conduct), int(away_conduct),
                int(home_unstripped), int(away_unstripped),
                int(home_defaulted), int(away_defaulted),
                now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def ladder_audit_df_for_date(selected_date: date) -> pd.DataFrame:
    """
    Per-team per-game scoring for fault finding (selected date only).

    DEFAULT rules:
      - Defaulting team total = 10 (conduct only)
      - Opponent total = 13 (3 match pts + 10 conduct)
    """
    start_min = datetime.combine(selected_date, datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(selected_date + timedelta(days=1), datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    rows = conn.execute(
        """
        WITH base AS (
            SELECT
                g.id AS game_id,
                g.start_dt,
                g.field_name,
                g.home_team AS home_team,
                g.away_team AS away_team,
                COALESCE(t1.division,'') AS home_division,
                COALESCE(t2.division,'') AS away_division,
                gr.home_score, gr.away_score,
                gr.home_female_tries, gr.away_female_tries,
                gr.home_conduct, gr.away_conduct,
                gr.home_unstripped, gr.away_unstripped,
                COALESCE(gr.home_defaulted,0) AS home_defaulted,
                COALESCE(gr.away_defaulted,0) AS away_defaulted,
                gr.updated_at
            FROM games g
            LEFT JOIN teams t1 ON t1.name = g.home_team
            LEFT JOIN teams t2 ON t2.name = g.away_team
            LEFT JOIN game_results gr ON gr.game_id = g.id
            WHERE g.start_dt >= ? AND g.start_dt < ?
        ),
        teamsplit AS (
            SELECT
                game_id, start_dt, field_name,
                home_team AS team,
                away_team AS opponent,
                home_division AS division,
                home_score AS pf,
                away_score AS pa,
                home_female_tries AS female_tries,
                home_conduct AS conduct,
                home_unstripped AS unstripped,
                home_defaulted AS defaulted,
                away_defaulted AS opponent_defaulted,
                updated_at
            FROM base

            UNION ALL

            SELECT
                game_id, start_dt, field_name,
                away_team AS team,
                home_team AS opponent,
                away_division AS division,
                away_score AS pf,
                home_score AS pa,
                away_female_tries AS female_tries,
                away_conduct AS conduct,
                away_unstripped AS unstripped,
                away_defaulted AS defaulted,
                home_defaulted AS opponent_defaulted,
                updated_at
            FROM base
        )
        SELECT
            game_id,
            start_dt,
            field_name,
            division,
            team,
            opponent,
            pf,
            pa,
            (pf - pa) AS margin,

            CASE
              WHEN pf > pa THEN 'W'
              WHEN pf = pa THEN 'D'
              ELSE 'L'
            END AS result,

            CASE
              WHEN pf > pa THEN ?
              WHEN pf = pa THEN ?
              ELSE ?
            END AS match_pts,

            CASE
              WHEN pf < pa AND (pa - pf) IN (1,2) THEN 1 ELSE 0
            END AS close_loss_bp,

            female_tries,
            CASE WHEN female_tries >= 4 THEN 1 ELSE 0 END AS female_bp,

            conduct,

            unstripped,
            CASE WHEN unstripped >= 3 THEN -2 ELSE 0 END AS unstripped_pen,

            defaulted,
            opponent_defaulted,

            updated_at
        FROM teamsplit
        ORDER BY start_dt ASC, field_name ASC, team ASC
        """,
        (start_min, start_max, LADDER_WIN_PTS, LADDER_DRAW_PTS, LADDER_LOSS_PTS),
    ).fetchall()
    conn.close()

    out = []
    for r in rows:
        match_pts = int(r["match_pts"] or 0)
        close_bp = int(r["close_loss_bp"] or 0)
        female_bp = int(r["female_bp"] or 0)
        conduct = int(r["conduct"] or 0)
        pen = int(r["unstripped_pen"] or 0)

        defaulted = int(r["defaulted"] or 0)
        opponent_defaulted = int(r["opponent_defaulted"] or 0)

        if defaulted == 1:
            match_pts = 0
            close_bp = 0
            female_bp = 0
            pen = 0
            conduct = 10
            result = "L"
            total = 10
        elif opponent_defaulted == 1:
            match_pts = 3
            close_bp = 0
            female_bp = 0
            pen = 0
            conduct = 10
            result = "W"
            total = 13
        else:
            result = r["result"] or "—"
            total = match_pts + close_bp + female_bp + conduct + pen

        out.append(
            {
                "Start": _time_12h(dtparser.parse(r["start_dt"])) if r["start_dt"] else "—",
                "Field": r["field_name"] or "—",
                "Division": (r["division"] or "").strip() or "—",
                "Team": r["team"] or "—",
                "Opponent": r["opponent"] or "—",
                "PF": int(r["pf"] or 0),
                "PA": int(r["pa"] or 0),
                "Res": result,
                "Match": match_pts,
                "CloseBP": close_bp,
                "FemTries": int(r["female_tries"] or 0),
                "FemBP": female_bp,
                "Conduct": conduct,
                "Unstrip": int(r["unstripped"] or 0),
                "Pen": pen,
                "Total": total,
                "Defaulted": "YES" if defaulted == 1 else ("OPP DEFAULTED" if opponent_defaulted == 1 else ""),
                "Updated": (r["updated_at"] or "")[:19],
            }
        )

    return pd.DataFrame(out)


def ladder_table_df_for_date(selected_date: date, division: str) -> pd.DataFrame:
    """
    Aggregated ladder for selected date, includes Opening + Final.
    """
    df = ladder_audit_df_for_date(selected_date)
    if df.empty:
        return pd.DataFrame()

    df = df[df["Division"].fillna("—") == (division or "—")]
    if df.empty:
        return pd.DataFrame()

    opening_map = {t["name"]: int(t["opening_balance"] or 0) for t in list_teams()}

    grouped = df.groupby("Team", dropna=False).agg(
        P=("Team", "count"),
        W=("Res", lambda s: int((s == "W").sum())),
        D=("Res", lambda s: int((s == "D").sum())),
        L=("Res", lambda s: int((s == "L").sum())),
        PF=("PF", "sum"),
        PA=("PA", "sum"),
        Match=("Match", "sum"),
        CloseBP=("CloseBP", "sum"),
        FemBP=("FemBP", "sum"),
        Conduct=("Conduct", "sum"),
        Pen=("Pen", "sum"),
        Total=("Total", "sum"),
    ).reset_index()

    grouped["PD"] = grouped["PF"] - grouped["PA"]
    grouped["Opening"] = grouped["Team"].apply(lambda t: int(opening_map.get((t or "").strip(), 0)))
    grouped["Final"] = grouped["Opening"] + grouped["Total"]

    grouped = grouped.sort_values(
        by=["Final", "PD", "PF", "Team"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    return grouped[
        [
            "Team", "P", "W", "D", "L",
            "PF", "PA", "PD",
            "Opening",
            "Match", "CloseBP", "FemBP", "Conduct", "Pen",
            "Total", "Final",
        ]
    ]


def ladder_validation_warnings_for_date(selected_date: date) -> list[str]:
    warnings: list[str] = []

    games = get_games()
    todays = [g for g in games if game_local_date(g) == selected_date]
    if not todays:
        return warnings

    missing_div = set()
    mismatch_div_games = []

    for g in todays:
        h = (g["home_team"] or "").strip()
        a = (g["away_team"] or "").strip()
        dh = get_team_division(h)
        da = get_team_division(a)

        if not dh:
            missing_div.add(h)
        if not da:
            missing_div.add(a)
        if dh and da and dh != da:
            mismatch_div_games.append(f"{h} vs {a} ({dh} / {da})")

    if missing_div:
        warnings.append("Missing team division for: " + ", ".join(sorted(missing_div)))

    if mismatch_div_games:
        warnings.append("Cross-division games detected: " + "; ".join(mismatch_div_games))

    for g in todays:
        gr = get_game_result(int(g["id"]))
        if not gr:
            warnings.append(f"Missing result entry: {g['home_team']} vs {g['away_team']}")
            continue

        hd = int(gr["home_defaulted"] or 0)
        ad = int(gr["away_defaulted"] or 0)
        if hd and ad:
            warnings.append(f"Invalid default flags (both sides): {g['home_team']} vs {g['away_team']}")

        if not (hd or ad):
            for side in ("home", "away"):
                c = int(gr[f"{side}_conduct"] or 0)
                if c < 0 or c > 10:
                    warnings.append(
                        f"Conduct out of range (0-10): {g['home_team']} vs {g['away_team']} ({side}={c})"
                    )

        for k in [
            "home_score", "away_score",
            "home_female_tries", "away_female_tries",
            "home_unstripped", "away_unstripped",
        ]:
            v = int(gr[k] or 0)
            if v < 0:
                warnings.append(f"Negative value {k} for game: {g['home_team']} vs {g['away_team']}")

    return warnings


# ============================================================
# Acceptance progress helpers
# ============================================================
def iso_week_window(d: date) -> tuple[date, date]:
    start = d - timedelta(days=d.weekday())  # Monday
    end_excl = start + timedelta(days=7)
    return start, end_excl


def get_acceptance_progress_for_window(start_date: date, end_date_exclusive: date) -> tuple[int, int]:
    start_min = datetime.combine(start_date, datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(end_date_exclusive, datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN UPPER(COALESCE(a.status,'')) IN ('ACCEPTED','ASSIGNED') THEN 1 ELSE 0 END) AS accepted_slots,
            COUNT(a.id) AS total_slots
        FROM games g
        JOIN assignments a ON a.game_id = g.id
        WHERE g.start_dt >= ? AND g.start_dt < ?
        """,
        (start_min, start_max),
    ).fetchone()
    conn.close()

    accepted = int(row["accepted_slots"] or 0)
    total = int(row["total_slots"] or 0)
    return accepted, total


def list_referees_not_accepted_for_window(start_date: date, end_date_exclusive: date) -> list[str]:
    start_min = datetime.combine(start_date, datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(end_date_exclusive, datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    rows = conn.execute(
        """
        SELECT DISTINCT TRIM(r.name) AS name
        FROM games g
        JOIN assignments a ON a.game_id = g.id
        JOIN referees r ON r.id = a.referee_id
        WHERE g.start_dt >= ? AND g.start_dt < ?
          AND a.referee_id IS NOT NULL
          AND UPPER(COALESCE(a.status,'')) NOT IN ('ACCEPTED','ASSIGNED')
          AND TRIM(COALESCE(r.name,'')) <> ''
        ORDER BY name ASC
        """,
        (start_min, start_max),
    ).fetchall()
    conn.close()

    return [row["name"] for row in rows]


def has_any_offers_for_window(start_date: date, end_date_exclusive: date) -> bool:
    """
    True if at least one offer exists for any assignment whose game start_dt falls within the window.
    This is the cleanest signal that an OFFER has been sent (or at least created).
    """
    start_min = datetime.combine(start_date, datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(end_date_exclusive, datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    row = conn.execute(
        """
        SELECT 1
        FROM games g
        JOIN assignments a ON a.game_id = g.id
        JOIN offers o ON o.assignment_id = a.id
        WHERE g.start_dt >= ? AND g.start_dt < ?
        LIMIT 1
        """,
        (start_min, start_max),
    ).fetchone()
    conn.close()
    return bool(row)

def get_referee_workload_all_time() -> pd.DataFrame:
    """
    Returns a dataframe of all active referees and how many slots they have that are
    ACCEPTED/ASSIGNED across ALL games in the database.
    Sorted least -> most.
    """
    conn = db()
    rows = conn.execute(
        """
        SELECT
            r.id AS referee_id,
            TRIM(r.name) AS name,
            TRIM(r.email) AS email,
            TRIM(COALESCE(r.phone,'')) AS phone,
            SUM(
                CASE
                    WHEN UPPER(COALESCE(a.status,'')) IN ('ACCEPTED','ASSIGNED')
                    THEN 1 ELSE 0
                END
            ) AS accepted_slots
        FROM referees r
        LEFT JOIN assignments a
            ON a.referee_id = r.id
        WHERE r.active = 1
        GROUP BY r.id, r.name, r.email, r.phone
        ORDER BY accepted_slots ASC, name ASC
        """
    ).fetchall()
    conn.close()

    df = pd.DataFrame(
        [
            {
                "Referee": (row["name"] or "").strip() or "—",
                "Phone": (row["phone"] or "").strip() or "—",
                "Email": (row["email"] or "").strip() or "—",
                "Accepted": int(row["accepted_slots"] or 0),
            }
            for row in rows
        ]
    )

    if df.empty:
        df = pd.DataFrame(columns=["Referee", "Phone", "Email", "Accepted"])

    return df


# ============================================================
# Printable PDF helpers
# ============================================================
def get_admin_print_rows_for_date(selected_date: date):
    start_min = datetime.combine(selected_date, datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(selected_date + timedelta(days=1), datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    rows = conn.execute(
        """
        SELECT
            g.id AS game_id,
            g.home_team,
            g.away_team,
            g.field_name,
            g.start_dt,
            a.slot_no,
            a.status,
            r.name AS ref_name
        FROM games g
        LEFT JOIN assignments a ON a.game_id = g.id
        LEFT JOIN referees r ON r.id = a.referee_id
        WHERE g.start_dt >= ? AND g.start_dt < ?
        ORDER BY g.start_dt ASC, g.field_name ASC, g.home_team ASC, a.slot_no ASC
        """,
        (start_min, start_max),
    ).fetchall()
    conn.close()

    games_map = {}
    for row in rows:
        gid = int(row["game_id"])
        if gid not in games_map:
            games_map[gid] = {
                "home_team": row["home_team"],
                "away_team": row["away_team"],
                "field_name": row["field_name"],
                "start_dt": row["start_dt"],
                "slots": {
                    1: {"name": "", "status": "EMPTY"},
                    2: {"name": "", "status": "EMPTY"},
                },
            }

        slot_no = row["slot_no"]
        if slot_no in (1, 2):
            nm = (row["ref_name"] or "").strip()
            stt = (row["status"] or "EMPTY").strip().upper()
            games_map[gid]["slots"][int(slot_no)] = {"name": nm, "status": stt}

    out = list(games_map.values())
    out.sort(key=lambda x: x["start_dt"])
    return out


def _format_ref_name(name: str, status: str) -> str:
    name = (name or "").strip()
    status = (status or "EMPTY").strip().upper()
    if not name:
        return "—"
    if status in ("ACCEPTED", "ASSIGNED"):
        return name
    if status in ("OFFERED", "DECLINED"):
        return f"{name} ({status})"
    return name


def build_admin_summary_pdf_bytes(selected_date: date) -> bytes:
    games = get_admin_print_rows_for_date(selected_date)

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=16,
        rightMargin=16,
        topMargin=16,
        bottomMargin=16,
        title=f"Game Summary {selected_date.isoformat()}",
    )

    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"<b>Game Summary</b> — {selected_date.isoformat()}", styles["Title"]))
    story.append(Spacer(1, 4))

    if not games:
        story.append(Paragraph("No games found for this date.", styles["Normal"]))
        doc.build(story)
        return buffer.getvalue()

    grouped = {}
    for g in games:
        dt = dtparser.parse(g["start_dt"])
        key = _time_12h(dt)
        grouped.setdefault(key, []).append((dt, g))

    group_keys = sorted(grouped.keys(), key=lambda k: min(dt for dt, _ in grouped[k]))

    for time_key in group_keys:
        story.append(Paragraph(f"<b>Start time: {time_key}</b>", styles["Heading3"]))
        story.append(Spacer(1, 2))

        data = [["Teams", "Field", "Start", "Referees"]]
        for dt, g in grouped[time_key]:
            teams = f"{g['home_team']} vs {g['away_team']}"
            field = g["field_name"]
            start_str = _time_12h(dt)

            r1 = _format_ref_name(g["slots"][1]["name"], g["slots"][1]["status"])
            r2 = _format_ref_name(g["slots"][2]["name"], g["slots"][2]["status"])
            refs = f"{r1} / {r2}"

            data.append([teams, field, start_str, refs])

        table = Table(
            data,
            colWidths=[360, 110, 75, 210],
            repeatRows=1,
        )
        table.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 9),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 8),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 3),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                    ("TOPPADDING", (0, 0), (-1, -1), 1),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
                ]
            )
        )

        story.append(table)
        story.append(Spacer(1, 4))

    doc.build(story)
    return buffer.getvalue()


def build_admin_summary_xlsx_bytes(selected_date: date) -> bytes:
    """
    Builds an .xlsx version of the SAME summary as the PDF.
    One row per game, with refs in Slot 1 / Slot 2.
    """
    games = get_admin_print_rows_for_date(selected_date)

    rows = []
    for g in games:
        dt = dtparser.parse(g["start_dt"])
        start_12h = dt.strftime("%I:%M %p").lstrip("0")

        r1 = _format_ref_name(g["slots"][1]["name"], g["slots"][1]["status"])
        r2 = _format_ref_name(g["slots"][2]["name"], g["slots"][2]["status"])

        rows.append(
            {
                "Start": start_12h,
                "Field": g["field_name"],
                "Teams": f"{g['home_team']} vs {g['away_team']}",
                "Ref 1": r1 if r1 != "—" else "",
                "Ref 2": r2 if r2 != "—" else "",
            }
        )

    df = pd.DataFrame(rows)

    # Sort by start time then field then teams
    if not df.empty:
        df = df.sort_values(by=["Start", "Field", "Teams"], ascending=[True, True, True]).reset_index(drop=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        sheet = "Summary"
        df.to_excel(writer, index=False, sheet_name=sheet)

        # Basic formatting (safe, minimal)
        ws = writer.sheets[sheet]
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        # Column widths (approx)
        widths = {"A": 12, "B": 12, "C": 40, "D": 22, "E": 22}
        for col, w in widths.items():
            ws.column_dimensions[col].width = w

    return output.getvalue()


def _refs_names_only_for_game(g: dict) -> str:
    r1 = (g["slots"][1]["name"] or "").strip() or "—"
    r2 = (g["slots"][2]["name"] or "").strip() or "—"
    return f"{r1} / {r2}"


def build_referee_scorecards_pdf_bytes(selected_date: date) -> bytes:
    games = get_admin_print_rows_for_date(selected_date)

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    page_w, page_h = A4

    outer_margin = 10 * mm
    gutter_x = 6 * mm
    gutter_y = 6 * mm

    card_w = (page_w - (2 * outer_margin) - gutter_x) / 2.0
    card_h = (page_h - (2 * outer_margin) - (2 * gutter_y)) / 3.0

    pad = 6 * mm

    TITLE_SIZE = 16
    TEAMS_MAX_SIZE = 13
    TEAMS_MIN_SIZE = 9
    REFS_SIZE = 11
    FIELD_TIME_SIZE = 12

    TEAM_ABOVE_NUM_MAX = 11
    TEAM_ABOVE_NUM_MIN = 8

    TRIES_NUM_SIZE = 18
    FOOT_LABEL_SIZE = 10

    WRITE_LINE_W = int(22 * 1.25)
    WRITE_LINE_THICK = 1.2

    def fit_bold_font_size(text: str, max_width: float, start_size: int, min_size: int) -> int:
        size = start_size
        while size > min_size:
            if c.stringWidth(text, "Helvetica-Bold", size) <= max_width:
                return size
            size -= 1
        return min_size

    def fit_left_text_size(text: str, max_width: float, start_size: int, min_size: int) -> int:
        size = start_size
        while size > min_size:
            if c.stringWidth(text, "Helvetica-Bold", size) <= max_width:
                return size
            size -= 1
        return min_size

    def draw_card(x0: float, y0: float, g: dict):
        c.setLineWidth(1)
        c.rect(x0, y0, card_w, card_h)

        left = x0 + pad
        right = x0 + card_w - pad
        y_top = y0 + card_h - pad
        max_text_w = right - left
        cx = x0 + card_w / 2.0

        c.setFont("Helvetica-Bold", TITLE_SIZE)
        c.drawCentredString(cx, y_top, "REFEREE SCORECARD")

        c.setLineWidth(0.8)
        c.line(left, y_top - 8, right, y_top - 8)

        teams_line = f"{g['home_team']} vs {g['away_team']}"
        teams_size = fit_bold_font_size(teams_line, max_text_w, TEAMS_MAX_SIZE, TEAMS_MIN_SIZE)
        c.setFont("Helvetica-Bold", teams_size)
        c.drawCentredString(cx, y_top - 26, teams_line)

        refs_line = _refs_names_only_for_game(g)
        c.setFont("Helvetica", REFS_SIZE)
        c.drawCentredString(cx, y_top - 44, refs_line)

        dt = dtparser.parse(g["start_dt"])
        field_time = f"{g['field_name']} @ {_time_12h(dt)}"
        c.setFont("Helvetica", FIELD_TIME_SIZE)
        c.drawCentredString(cx, y_top - 62, field_time)

        field_div_y = (y_top - 62) - 10
        c.setLineWidth(0.8)
        c.line(left, field_div_y, right, field_div_y)

        nums_left = left
        nums_right = right
        nums_span = nums_right - nums_left
        step = nums_span / 10.0

        wld_text = "W  /  L  /  D"
        wld_w = c.stringWidth(wld_text, "Helvetica-Bold", FOOT_LABEL_SIZE)

        team1_name_y = field_div_y - 16
        team1_nums_y = team1_name_y - 20

        INTER_TEAM_GAP = int(18 * 2.0)

        team2_name_y = team1_nums_y - INTER_TEAM_GAP
        team2_nums_y = team2_name_y - 20

        def draw_team_name_with_wld(team_name: str, y: float):
            nm = str(team_name)
            max_name_w = max_text_w - (wld_w + 6)
            size = fit_left_text_size(nm, max_name_w, TEAM_ABOVE_NUM_MAX, TEAM_ABOVE_NUM_MIN)

            c.setFont("Helvetica-Bold", size)
            c.drawString(left, y, nm)

            c.setFont("Helvetica-Bold", FOOT_LABEL_SIZE)
            c.drawRightString(right, y, wld_text)

        def draw_nums_row(y: float):
            c.setFont("Helvetica-Bold", TRIES_NUM_SIZE)
            for i in range(10):
                n = str(i + 1)
                x = nums_left + (step * (i + 0.5))
                c.drawCentredString(x, y, n)

        draw_team_name_with_wld(g["home_team"], team1_name_y)
        draw_nums_row(team1_nums_y)

        draw_team_name_with_wld(g["away_team"], team2_name_y)
        draw_nums_row(team2_nums_y)

        line_y = team2_nums_y - 14
        c.setLineWidth(0.8)
        c.line(left, line_y, right, line_y)

        line_x2 = right
        line_x1 = right - WRITE_LINE_W

        conduct_y = line_y - 18
        c.setFont("Helvetica-Bold", FOOT_LABEL_SIZE)
        c.drawString(left, conduct_y, "Conduct (/10)")
        c.setLineWidth(WRITE_LINE_THICK)
        c.line(line_x1, conduct_y - 3, line_x2, conduct_y - 3)

        unstrip_y = conduct_y - 22
        c.setFont("Helvetica-Bold", FOOT_LABEL_SIZE)
        c.drawString(left, unstrip_y, "Unstripped Players")
        c.setLineWidth(WRITE_LINE_THICK)
        c.line(line_x1, unstrip_y - 3, line_x2, unstrip_y - 3)

    for idx, g in enumerate(games):
        if idx > 0 and idx % 6 == 0:
            c.showPage()

        pos = idx % 6
        r = pos // 2
        col = pos % 2

        x0 = outer_margin + col * (card_w + gutter_x)
        y0 = page_h - outer_margin - (r + 1) * card_h - r * gutter_y

        draw_card(x0, y0, g)

    c.save()
    return buf.getvalue()


# ============================================================
# Offers
# ============================================================
def create_offer(assignment_id: int) -> str:
    token = secrets.token_urlsafe(24)
    conn = db()
    conn.execute(
        """
        INSERT INTO offers(assignment_id, token, created_at)
        VALUES (?, ?, ?)
        """,
        (assignment_id, token, now_iso()),
    )
    conn.commit()
    conn.close()
    return token


def delete_offer_by_token(token: str):
    conn = db()
    conn.execute("DELETE FROM offers WHERE token=?", (token,))
    conn.commit()
    conn.close()


def resolve_offer(token: str, response: str) -> tuple[bool, str]:
    response = (response or "").strip().upper()
    if response not in ("ACCEPTED", "DECLINED"):
        return False, "Invalid response."

    conn = db()
    offer = conn.execute(
        """
        SELECT id, assignment_id
        FROM offers
        WHERE token=?
        """,
        (token,),
    ).fetchone()

    if not offer:
        conn.close()
        return False, "Invalid or unknown offer link."

    conn.execute(
        """
        UPDATE offers
        SET responded_at=?, response=?
        WHERE id=?
        """,
        (now_iso(), response, offer["id"]),
    )

    new_status = "ACCEPTED" if response == "ACCEPTED" else "DECLINED"
    conn.execute(
        """
        UPDATE assignments
        SET status=?, updated_at=?
        WHERE id=?
        """,
        (new_status, now_iso(), offer["assignment_id"]),
    )

    conn.commit()
    conn.close()
    return True, f"Thanks — you have {response.lower()} the offer."


def send_offer_email_and_mark_offered(
    *,
    assignment_id: int,
    referee_name: str,
    referee_email: str,
    game,
    start_dt,
    msg_key: str,
):
    token = create_offer(assignment_id)

    try:
        cfg = smtp_settings()
        base = cfg.get("app_base_url", "").rstrip("/")
        if not base:
            raise RuntimeError("APP_BASE_URL is missing. Add it in Render environment variables.")

        game_line = f"{game['home_team']} vs {game['away_team']}"
        when_line = start_dt.strftime("%Y-%m-%d %I:%M %p").lstrip("0")
        subject = f"{referee_name} — Match assignment: {game_line}"

        portal_url = f"{base}/?offer_token={token}"

        text = (
            f"Hi {referee_name},\n\n"
            f"You have a match assignment offer:\n"
            f"- Game: {game_line}\n"
            f"- Field: {game['field_name']}\n"
            f"- Start: {when_line}\n\n"
            f"View and respond here:\n{portal_url}\n"
        )

        html = f"""
        <div style="font-family: Arial, sans-serif; line-height:1.4;">
          <p>Hi {referee_name},</p>
          <p>You have a match assignment offer:</p>
          <ul>
            <li><b>Game:</b> {game_line}</li>
            <li><b>Field:</b> {game['field_name']}</li>
            <li><b>Start:</b> {when_line}</li>
          </ul>
          <p>
            <a href="{portal_url}" style="display:inline-block;padding:10px 14px;background:#1565c0;color:#fff;text-decoration:none;border-radius:6px;">
              View offer
            </a>
          </p>
          <p style="color:#666;font-size:12px;">
            If the button doesn’t work, copy and paste this link:<br>{portal_url}
          </p>
        </div>
        """

        send_html_email(referee_email, referee_name, subject, html, text_body=text)

        set_assignment_status(assignment_id, "OFFERED")
        st.session_state[msg_key] = "Offer emailed successfully and marked as OFFERED."
    except Exception as e:
        delete_offer_by_token(token)
        st.session_state[msg_key] = f"Email failed — offer not created: {e}"


# ============================================================
# Referee portal (My Offers)
# ============================================================
def get_offer_details_by_token(token: str):
    conn = db()
    row = conn.execute(
        """
        SELECT
            o.id AS offer_id,
            o.token,
            o.created_at,
            o.responded_at,
            o.response,
            a.id AS assignment_id,
            a.slot_no,
            a.referee_id,
            a.status,
            r.name AS ref_name,
            r.email AS ref_email,
            g.home_team,
            g.away_team,
            g.field_name,
            g.start_dt
        FROM offers o
        JOIN assignments a ON a.id = o.assignment_id
        JOIN games g ON g.id = a.game_id
        LEFT JOIN referees r ON r.id = a.referee_id
        WHERE o.token=?
        LIMIT 1
        """,
        (token,),
    ).fetchone()
    conn.close()
    return row


def list_offers_for_referee(referee_id: int):
    conn = db()
    rows = conn.execute(
        """
        SELECT
            o.id AS offer_id,
            o.token,
            o.created_at,
            o.responded_at,
            o.response,
            a.id AS assignment_id,
            a.slot_no,
            a.status,
            g.home_team,
            g.away_team,
            g.field_name,
            g.start_dt
        FROM offers o
        JOIN assignments a ON a.id = o.assignment_id
        JOIN games g ON g.id = a.game_id
        WHERE a.referee_id=?
        ORDER BY o.created_at DESC
        """,
        (referee_id,),
    ).fetchall()
    conn.close()
    return rows


def maybe_handle_referee_portal_login():
    if not REF_PORTAL_ENABLED:
        return

    qp = st.query_params
    offer_token = qp.get("offer_token")
    if not offer_token:
        return

    offer = get_offer_details_by_token(offer_token)
    if not offer:
        st.title("My Offers")
        st.error("That offer link is invalid.")
        st.stop()

    if offer["referee_id"] is None:
        st.title("My Offers")
        st.error("This offer is not linked to a referee. Please contact the administrator.")
        st.stop()

    st.session_state["referee_id"] = int(offer["referee_id"])
    st.session_state["referee_name"] = offer["ref_name"] or "Referee"
    st.session_state["referee_email"] = offer["ref_email"] or ""

    st.query_params.pop("offer_token", None)
    st.rerun()


def referee_logout_button():
    if st.session_state.get("referee_id"):
        c1, c2 = st.columns([3, 1])
        with c1:
            st.caption(
                f"Logged in as: {st.session_state.get('referee_name')} "
                f"({st.session_state.get('referee_email')})"
            )
        with c2:
            if st.button("Log out", key="ref_logout_btn"):
                st.session_state.pop("referee_id", None)
                st.session_state.pop("referee_name", None)
                st.session_state.pop("referee_email", None)
                st.rerun()


def render_my_offers_page() -> bool:
    if not REF_PORTAL_ENABLED:
        return False

    ref_id = st.session_state.get("referee_id")
    if not ref_id:
        return False

    st.title("My Offers")
    referee_logout_button()
    st.markdown("---")

    offers = list_offers_for_referee(int(ref_id))
    if not offers:
        st.info("You have no offers at the moment.")
        return True

    for o in offers:
        start_dt = dtparser.parse(o["start_dt"])
        title = f"{o['home_team']} vs {o['away_team']}"
        subtitle = f"Field: {o['field_name']} • Start: {_time_12h(start_dt)} • Slot {o['slot_no']}"

        with st.container(border=True):
            st.subheader(title)
            st.caption(subtitle)

            current_resp = (o["response"] or "").strip().upper()
            if o["responded_at"] and current_resp:
                if current_resp == "DECLINED":
                    st.markdown(
                        "<div style='font-weight:700;color:#c62828;'>Response recorded: DECLINED</div>",
                        unsafe_allow_html=True,
                    )
                elif current_resp == "ACCEPTED":
                    st.markdown(
                        "<div style='font-weight:700;color:#2e7d32;'>Response recorded: ACCEPTED</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.info(f"Response recorded: {current_resp}")

                st.caption("You can change your response below if needed.")

            c1, c2 = st.columns(2)

            if c1.button("Accept", key=f"portal_acc_{o['token']}"):
                ok, msg = resolve_offer(o["token"], "ACCEPTED")
                if ok:
                    st.success("Accepted. Thank you.")
                else:
                    st.error(msg)
                st.rerun()

            if c2.button("Decline", key=f"portal_dec_{o['token']}"):
                ok, msg = resolve_offer(o["token"], "DECLINED")
                if ok:
                    st.success("Declined. Thank you.")
                else:
                    st.error(msg)
                st.rerun()

    return True


def maybe_handle_offer_response():
    qp = st.query_params
    token = qp.get("token")
    action = qp.get("action")
    if token and action in ("accept", "decline"):
        response = "ACCEPTED" if action == "accept" else "DECLINED"
        ok, msg = resolve_offer(token, response)
        st.title("Referee Response")
        if ok:
            st.success(msg)
        else:
            st.error(msg)
        st.info("You can close this page now.")
        st.stop()

# ============================================================
# APP START
# ============================================================
init_db()

maybe_handle_referee_portal_login()
maybe_handle_offer_response()

if render_my_offers_page():
    st.stop()

handle_admin_login_via_query_params()
maybe_restore_admin_from_session_param()

st.title("Referee Allocator — MVP")
st.caption(f"Database file: {DB_PATH}")

# Bootstrap: create first admin if none exist
if admin_count() == 0:
    st.warning("Initial setup: No administrators exist yet.")
    st.write("Enter your email to create the first admin account (one-time setup).")
    first_email = st.text_input("Your admin email", key="first_admin_email")
    if st.button("Create first admin", key="create_first_admin_btn"):
        if not first_email.strip():
            st.error("Please enter an email.")
        else:
            add_admin(first_email)
            st.success("First admin created. Now request a login link below.")
    st.markdown("---")

# Login screen
if not st.session_state.get("admin_email"):
    st.subheader("Admin Login")
    st.write("Enter your email to receive a one-time login link (15 minutes).")
    email = st.text_input("Admin email", key="login_email")

    if st.button("Send login link", key="send_login_link_btn"):
        if not email.strip():
            st.error("Please enter an email.")
        elif not is_admin_email_allowed(email):
            st.error("That email is not an authorised administrator.")
        else:
            try:
                send_admin_login_email(email)
                st.success("Login link sent. Check your email.")
            except Exception as e:
                st.error(str(e))

    st.stop()

# Logged in view
admin_logout_button()

tabs = st.tabs(["Admin", "Ladder", "Import", "Blackouts", "Administrators"])


# ============================================================
# Admin tab
# ============================================================
with tabs[0]:
    # Keep scroll position stable across reruns/autorefresh
    preserve_scroll("refalloc_admin_scroll")

    st.subheader("Games & Assignments")

    # --- Auto refresh ---
    auto = st.toggle("Auto-refresh every 5 seconds", value=True, key="auto_refresh_toggle")
    if auto:
        st_autorefresh(interval=5000, key="auto_refresh_tick")

    if st.button("Refresh status", key="refresh_status_btn"):
        st.rerun()

    # --- Load data ---
    games = get_games()
    refs = get_referees()

    if not games:
        st.info("Import a Games CSV first (Import tab).")
        st.stop()

    # --- Date selector ---
    all_dates = sorted({game_local_date(g) for g in games})
    today = date.today()
    default_idx = 0
    for i, d in enumerate(all_dates):
        if d >= today:
            default_idx = i
            break

    selected_date = st.selectbox(
        "Show games for date",
        all_dates,
        index=default_idx,
        key="games_date_select",
    )

    todays_games = [g for g in games if game_local_date(g) == selected_date]
    st.caption(f"{len(todays_games)} game(s) on {selected_date.isoformat()}")

    # ========================================================
    # Printable Summary (PDF + XLSX + Scorecards PDF)
    # ========================================================
    st.markdown("---")
    st.subheader("Printable Summary")

    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])

    with c1:
        if st.button("Build PDF", key="build_pdf_btn"):
            try:
                st.session_state["admin_summary_pdf_bytes"] = build_admin_summary_pdf_bytes(selected_date)
                st.success("PDF built.")
            except Exception as e:
                st.error(f"Failed to build PDF: {e}")

    with c2:
        if st.button("Build XLSX", key="build_xlsx_btn"):
            try:
                st.session_state["admin_summary_xlsx_bytes"] = build_admin_summary_xlsx_bytes(selected_date)
                st.success("XLSX built.")
            except Exception as e:
                st.error(f"Failed to build XLSX: {e}")

    with c3:
        if st.button("Build Scorecards", key="build_scorecards_btn"):
            try:
                st.session_state["ref_scorecards_pdf_bytes"] = build_referee_scorecards_pdf_bytes(selected_date)
                st.success("Scorecards PDF built.")
            except Exception as e:
                st.error(f"Failed to build scorecards: {e}")

    with c4:
        pdf_bytes = st.session_state.get("admin_summary_pdf_bytes")
        xlsx_bytes = st.session_state.get("admin_summary_xlsx_bytes")
        score_bytes = st.session_state.get("ref_scorecards_pdf_bytes")

        d1, d2, d3 = st.columns(3)

        with d1:
            if pdf_bytes:
                st.download_button(
                    label="Download PDF",
                    data=pdf_bytes,
                    file_name=f"game_summary_{selected_date.isoformat()}.pdf",
                    mime="application/pdf",
                    key="download_pdf_btn",
                )
            else:
                st.caption("Build PDF first.")

        with d2:
            if xlsx_bytes:
                st.download_button(
                    label="Download XLSX",
                    data=xlsx_bytes,
                    file_name=f"game_summary_{selected_date.isoformat()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_xlsx_btn",
                )
            else:
                st.caption("Build XLSX first.")

        with d3:
            if score_bytes:
                st.download_button(
                    label="Download Scorecards",
                    data=score_bytes,
                    file_name=f"referee_scorecards_{selected_date.isoformat()}.pdf",
                    mime="application/pdf",
                    key="download_scorecards_btn",
                )
            else:
                st.caption("Build scorecards first.")

 # ============================================================
# Admin tab
# ============================================================
with tabs[0]:
    # Keep scroll position stable across reruns/autorefresh
    preserve_scroll("refalloc_admin_scroll")

    st.subheader("Games & Assignments")

    # --- Auto refresh ---
    auto = st.toggle("Auto-refresh every 5 seconds", value=True, key="auto_refresh_toggle")
    if auto:
        st_autorefresh(interval=5000, key="auto_refresh_tick")

    if st.button("Refresh status", key="refresh_status_btn"):
        st.rerun()

    # --- Load data ---
    games = get_games()
    refs = get_referees()

    if not games:
        st.info("Import a Games CSV first (Import tab).")
        st.stop()

    # --- Date selector ---
    all_dates = sorted({game_local_date(g) for g in games})
    today = date.today()
    default_idx = 0
    for i, d in enumerate(all_dates):
        if d >= today:
            default_idx = i
            break

    selected_date = st.selectbox(
        "Show games for date",
        all_dates,
        index=default_idx,
        key="games_date_select",
    )

    todays_games = [g for g in games if game_local_date(g) == selected_date]
    st.caption(f"{len(todays_games)} game(s) on {selected_date.isoformat()}")

    # ========================================================
    # Printable Summary (PDF + XLSX + Scorecards PDF)
    # ========================================================
    st.markdown("---")
    st.subheader("Printable Summary")

    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])

    with c1:
        if st.button("Build PDF", key="build_pdf_btn"):
            try:
                st.session_state["admin_summary_pdf_bytes"] = build_admin_summary_pdf_bytes(selected_date)
                st.success("PDF built.")
            except Exception as e:
                st.error(f"Failed to build PDF: {e}")

    with c2:
        if st.button("Build XLSX", key="build_xlsx_btn"):
            try:
                st.session_state["admin_summary_xlsx_bytes"] = build_admin_summary_xlsx_bytes(selected_date)
                st.success("XLSX built.")
            except Exception as e:
                st.error(f"Failed to build XLSX: {e}")

    with c3:
        if st.button("Build Scorecards", key="build_scorecards_btn"):
            try:
                st.session_state["ref_scorecards_pdf_bytes"] = build_referee_scorecards_pdf_bytes(selected_date)
                st.success("Scorecards PDF built.")
            except Exception as e:
                st.error(f"Failed to build scorecards: {e}")

    with c4:
        pdf_bytes = st.session_state.get("admin_summary_pdf_bytes")
        xlsx_bytes = st.session_state.get("admin_summary_xlsx_bytes")
        score_bytes = st.session_state.get("ref_scorecards_pdf_bytes")

        d1, d2, d3 = st.columns(3)

        with d1:
            if pdf_bytes:
                st.download_button(
                    label="Download PDF",
                    data=pdf_bytes,
                    file_name=f"game_summary_{selected_date.isoformat()}.pdf",
                    mime="application/pdf",
                    key="download_pdf_btn",
                )
            else:
                st.caption("Build PDF first.")

        with d2:
            if xlsx_bytes:
                st.download_button(
                    label="Download XLSX",
                    data=xlsx_bytes,
                    file_name=f"game_summary_{selected_date.isoformat()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_xlsx_btn",
                )
            else:
                st.caption("Build XLSX first.")

        with d3:
            if score_bytes:
                st.download_button(
                    label="Download Scorecards",
                    data=score_bytes,
                    file_name=f"referee_scorecards_{selected_date.isoformat()}.pdf",
                    mime="application/pdf",
                    key="download_scorecards_btn",
                )
            else:
                st.caption("Build scorecards first.")

    # ========================================================
    # Games & Assignments (RESTORED)
    # ========================================================
    st.markdown("---")

    if not todays_games:
        st.info("No games found for this date.")
        st.stop()

    # Referee options
    ref_options = ["— (unassigned)"] + [f"{r['name']} ({r['email']})" for r in refs]
    ref_lookup = {f"{r['name']} ({r['email']})": int(r["id"]) for r in refs}

    for g in todays_games:
        start_dt = dtparser.parse(g["start_dt"])
        header = f"{g['home_team']} vs {g['away_team']} — {g['field_name']} @ {_time_12h(start_dt)}"

        with st.container(border=True):
            st.markdown(f"**{header}**")

            assigns = get_assignments_for_game(int(g["id"]))
            if not assigns:
                st.caption("No assignment slots found for this game.")
                continue

            slot_cols = st.columns(len(assigns))

            for idx, a in enumerate(assigns):
                with slot_cols[idx]:
                    st.markdown(f"**Slot {a['slot_no']}**")

                    # Status badge
                    stt = (a["status"] or "EMPTY").strip().upper()
                    if stt in ("ACCEPTED", "ASSIGNED"):
                        status_badge(stt, "#2e7d32")
                    elif stt == "DECLINED":
                        status_badge(stt, "#c62828")
                    elif stt == "OFFERED":
                        status_badge(stt, "#1565c0")
                    elif stt == "NOT_OFFERED":
                        status_badge("NOT OFFERED", "#6d4c41")
                    else:
                        status_badge("EMPTY", "#616161")

                    # Current selection
                    current_label = "— (unassigned)"
                    if a["referee_id"]:
                        nm = (a["ref_name"] or "").strip()
                        em = (a["ref_email"] or "").strip()
                        if nm and em:
                            current_label = f"{nm} ({em})"

                    pick = st.selectbox(
                        "Referee",
                        options=ref_options,
                        index=ref_options.index(current_label) if current_label in ref_options else 0,
                        key=f"refpick_{g['id']}_{a['id']}",
                        label_visibility="collapsed",
                    )

                    # Apply assignment change
                    if pick == "— (unassigned)":
                        if a["referee_id"]:
                            if st.button("Clear", key=f"clear_{a['id']}"):
                                clear_assignment(int(a["id"]))
                                st.rerun()
                    else:
                        new_ref_id = ref_lookup.get(pick)
                        if new_ref_id and int(a["referee_id"] or 0) != int(new_ref_id):
                            if st.button("Assign", key=f"assign_{a['id']}"):
                                set_assignment_ref(int(a["id"]), int(new_ref_id))
                                st.rerun()

                        # Offer button (only if referee selected)
                        if st.button("Offer", key=f"offer_{a['id']}"):
                            live = get_assignment_live(int(a["id"]))
                            msg_key = f"offer_msg_{a['id']}"
                            if not live or not live["referee_id"]:
                                st.error("Select a referee first.")
                            else:
                                send_offer_email_and_mark_offered(
                                    assignment_id=int(live["id"]),
                                    referee_name=live["ref_name"] or "Referee",
                                    referee_email=live["ref_email"] or "",
                                    game=g,
                                    start_dt=dtparser.parse(g["start_dt"]),
                                    msg_key=msg_key,
                                )
                            if st.session_state.get(msg_key):
                                st.caption(st.session_state[msg_key])

                    # Small spacing
                    st.caption(" ")

# ========================================================
# Games & Assignments (Actions dropdown restored)
# ========================================================
st.markdown("---")

if not todays_games:
    st.info("No games found for this date.")
    st.stop()

# Referee options
ref_options = ["— (unassigned)"] + [f"{r['name']} ({r['email']})" for r in refs]
ref_lookup = {f"{r['name']} ({r['email']})": int(r["id"]) for r in refs}

ACTION_OPTIONS = [
    "—",
    "ASSIGN (set status ASSIGNED)",
    "OFFER (email offer + set status OFFERED)",
    "CLEAR (remove referee + delete offers)",
]

for g in todays_games:
    start_dt = dtparser.parse(g["start_dt"])
    header = f"{g['home_team']} vs {g['away_team']} — {g['field_name']} @ {_time_12h(start_dt)}"

    with st.container(border=True):
        st.markdown(f"**{header}**")

        assigns = get_assignments_for_game(int(g["id"]))
        if not assigns:
            st.caption("No assignment slots found for this game.")
            continue

        slot_cols = st.columns(len(assigns), gap="large")

        for idx, a in enumerate(assigns):
            with slot_cols[idx]:
                st.markdown(f"**Slot {a['slot_no']}**")

                # Status badge
                stt = (a["status"] or "EMPTY").strip().upper()
                if stt == "ASSIGNED":
                    status_badge("ASSIGNED", "#2e7d32")
                elif stt == "ACCEPTED":
                    status_badge("ACCEPTED", "#1565c0")
                elif stt == "DECLINED":
                    status_badge("DECLINED", "#c62828")
                elif stt == "OFFERED":
                    status_badge("OFFERED", "#6d4c41")
                elif stt == "NOT_OFFERED":
                    status_badge("NOT OFFERED", "#616161")
                else:
                    status_badge("EMPTY", "#9e9e9e")

                # Current referee label
                current_label = "— (unassigned)"
                if a["referee_id"]:
                    nm = (a["ref_name"] or "").strip()
                    em = (a["ref_email"] or "").strip()
                    if nm and em:
                        current_label = f"{nm} ({em})"

                pick_key = f"refpick_{g['id']}_{a['id']}"
                pick = st.selectbox(
                    "Referee",
                    options=ref_options,
                    index=ref_options.index(current_label) if current_label in ref_options else 0,
                    key=pick_key,
                    label_visibility="collapsed",
                )

                # Action dropdown (this is what you said you lost)
                act_key = f"action_{g['id']}_{a['id']}"
                action = st.selectbox(
                    "Action",
                    options=ACTION_OPTIONS,
                    index=0,
                    key=act_key,
                    label_visibility="collapsed",
                )

                run_key = f"run_{g['id']}_{a['id']}"
                if st.button("Run", key=run_key):
                    # Re-read latest row at click time (avoids stale UI data)
                    live = get_assignment_live(int(a["id"]))
                    if not live:
                        st.error("Assignment not found.")
                        st.rerun()

                    # Apply referee selection change first
                    if pick == "— (unassigned)":
                        # If user unassigned the ref, treat as CLEAR
                        clear_assignment(int(live["id"]))
                        st.rerun()
                    else:
                        new_ref_id = ref_lookup.get(pick)
                        if new_ref_id and int(live["referee_id"] or 0) != int(new_ref_id):
                            set_assignment_ref(int(live["id"]), int(new_ref_id))
                            live = get_assignment_live(int(live["id"]))  # refresh

                    # Now perform selected action
                    if action == "—":
                        st.info("No action selected.")
                        st.rerun()

                    if action.startswith("CLEAR"):
                        clear_assignment(int(live["id"]))
                        st.rerun()

                    if not live["referee_id"]:
                        st.error("Select a referee first.")
                        st.rerun()

                    if action.startswith("ASSIGN"):
                        set_assignment_status(int(live["id"]), "ASSIGNED")
                        st.rerun()

                    if action.startswith("OFFER"):
                        msg_key = f"offer_msg_{live['id']}"
                        send_offer_email_and_mark_offered(
                            assignment_id=int(live["id"]),
                            referee_name=live["ref_name"] or "Referee",
                            referee_email=live["ref_email"] or "",
                            game=g,
                            start_dt=dtparser.parse(g["start_dt"]),
                            msg_key=msg_key,
                        )
                        if st.session_state.get(msg_key):
                            st.caption(st.session_state[msg_key])
                        st.rerun()

                st.caption(" ")   


# ============================================================
# Ladder tab
# ============================================================
with tabs[1]:
    st.subheader("Competition Ladder (Admin)")
    st.caption("Set divisions + opening balances, enter results (incl DEFAULT), then view ladder + audit.")

    games = get_games()
    if not games:
        st.info("Import a Games CSV first.")
        st.stop()

    all_dates = sorted({game_local_date(g) for g in games})
    if not all_dates:
        st.info("No game dates found.")
        st.stop()

    today = date.today()
    default_idx = 0
    for i, d in enumerate(all_dates):
        if d >= today:
            default_idx = i
            break

    selected_date = st.selectbox(
        "Ladder date",
        all_dates,
        index=default_idx,
        key="ladder_date_select",
    )

    todays_games = [g for g in games if game_local_date(g) == selected_date]
    st.caption(f"{len(todays_games)} game(s) on {selected_date.isoformat()}")

    st.markdown("---")
    st.markdown("### 1) Team divisions + opening balance")

    teams_today = sorted(
        {(g["home_team"] or "").strip() for g in todays_games}
        | {(g["away_team"] or "").strip() for g in todays_games}
    )
    teams_today = [t for t in teams_today if t]

    teams_rows = list_teams()
    existing = {
        (t["name"] or "").strip(): {
            "division": (t["division"] or "").strip(),
            "opening_balance": int(t["opening_balance"] or 0),
        }
        for t in teams_rows
    }

    div_col1, div_col2 = st.columns([2, 1], gap="large")

    with div_col1:
        st.write("Set division + opening balance per team (saved immediately).")

        for t in teams_today:
            current_div = (existing.get(t, {}).get("division") or "").strip()
            current_open = int(existing.get(t, {}).get("opening_balance") or 0)

            d_idx = DIVISIONS.index(current_div) if current_div in DIVISIONS else 0

            cA, cB = st.columns([2, 1], vertical_alignment="center")
            with cA:
                new_div = st.selectbox(
                    label=t,
                    options=DIVISIONS,
                    index=d_idx,
                    key=f"div_select_{selected_date.isoformat()}_{t}",
                )
            with cB:
                new_open = st.number_input(
                    label="Opening",
                    min_value=0,
                    step=1,
                    value=current_open,
                    key=f"open_{selected_date.isoformat()}_{t}",
                )

            if new_div != current_div or int(new_open) != current_open:
                upsert_team(t, new_div, opening_balance=int(new_open))
                existing[t] = {"division": new_div, "opening_balance": int(new_open)}

    with div_col2:
        st.write("Teams (today)")

        df_teams_today = pd.DataFrame(
            [
                {
                    "Team": t,
                    "Division": (existing.get(t, {}).get("division") or "").strip() or "—",
                    "Opening": int(existing.get(t, {}).get("opening_balance") or 0),
                }
                for t in teams_today
            ]
        )

        if df_teams_today.empty:
            st.caption("No teams found.")
        else:
            df_teams_today = df_teams_today.sort_values(
                by=["Division", "Opening", "Team"],
                ascending=[True, False, True],
            ).reset_index(drop=True)

            row_h = 35
            height = min(900, (len(df_teams_today) + 1) * row_h + 10)

            st.dataframe(
                df_teams_today,
                use_container_width=True,
                hide_index=True,
                height=height,
            )

    st.markdown("---")
    st.markdown("### 2) Enter game results (scores + referee inputs + DEFAULT)")

    if not todays_games:
        st.info("No games found for this date.")
        st.stop()

    for g in todays_games:
        start_dt = dtparser.parse(g["start_dt"])
        title = f"{g['home_team']} vs {g['away_team']} — {g['field_name']} @ {_time_12h(start_dt)}"

        gr = get_game_result(int(g["id"]))

        d_home_score = int(gr["home_score"]) if gr else 0
        d_away_score = int(gr["away_score"]) if gr else 0
        d_hft = int(gr["home_female_tries"]) if gr else 0
        d_aft = int(gr["away_female_tries"]) if gr else 0
        d_hc = int(gr["home_conduct"]) if gr else 0
        d_ac = int(gr["away_conduct"]) if gr else 0
        d_hu = int(gr["home_unstripped"]) if gr else 0
        d_au = int(gr["away_unstripped"]) if gr else 0
        d_hd = int(gr["home_defaulted"]) if gr else 0
        d_ad = int(gr["away_defaulted"]) if gr else 0

        with st.container(border=True):
            st.markdown(f"**{title}**")

            c0, c1, c2, c3 = st.columns([1, 1, 1, 1], gap="large")

            with c0:
                st.markdown("**DEFAULTED**")
                home_def = st.checkbox(f"{g['home_team']} defaulted", value=bool(d_hd), key=f"hd_{g['id']}")
                away_def = st.checkbox(f"{g['away_team']} defaulted", value=bool(d_ad), key=f"ad_{g['id']}")

                if home_def and away_def:
                    st.error("Only ONE team can be marked defaulted.")
                elif home_def:
                    st.info("Default rules apply: this team gets 10; opponent gets 13.")
                elif away_def:
                    st.info("Default rules apply: this team gets 10; opponent gets 13.")

            with c1:
                st.markdown("**Score**")
                home_score = st.number_input(
                    f"{g['home_team']} score",
                    min_value=0,
                    step=1,
                    value=d_home_score,
                    key=f"hs_{g['id']}",
                )
                away_score = st.number_input(
                    f"{g['away_team']} score",
                    min_value=0,
                    step=1,
                    value=d_away_score,
                    key=f"as_{g['id']}",
                )

            with c2:
                st.markdown("**Female tries**")
                home_ft = st.number_input(
                    f"{g['home_team']} female tries",
                    min_value=0,
                    step=1,
                    value=d_hft,
                    key=f"hft_{g['id']}",
                )
                away_ft = st.number_input(
                    f"{g['away_team']} female tries",
                    min_value=0,
                    step=1,
                    value=d_aft,
                    key=f"aft_{g['id']}",
                )

            with c3:
                st.markdown("**Conduct / Unstripped**")
                home_conduct = st.number_input(
                    f"{g['home_team']} conduct (/10)",
                    min_value=0,
                    max_value=10,
                    step=1,
                    value=d_hc,
                    key=f"hc_{g['id']}",
                )
                away_conduct = st.number_input(
                    f"{g['away_team']} conduct (/10)",
                    min_value=0,
                    max_value=10,
                    step=1,
                    value=d_ac,
                    key=f"ac_{g['id']}",
                )

                home_un = st.number_input(
                    f"{g['home_team']} unstripped",
                    min_value=0,
                    step=1,
                    value=d_hu,
                    key=f"hu_{g['id']}",
                )
                away_un = st.number_input(
                    f"{g['away_team']} unstripped",
                    min_value=0,
                    step=1,
                    value=d_au,
                    key=f"au_{g['id']}",
                )

            if st.button("Save result", key=f"save_res_{g['id']}"):
                try:
                    upsert_game_result(
                        game_id=int(g["id"]),
                        home_score=int(home_score),
                        away_score=int(away_score),
                        home_female_tries=int(home_ft),
                        away_female_tries=int(away_ft),
                        home_conduct=int(home_conduct),
                        away_conduct=int(away_conduct),
                        home_unstripped=int(home_un),
                        away_unstripped=int(away_un),
                        home_defaulted=1 if home_def else 0,
                        away_defaulted=1 if away_def else 0,
                    )
                    st.success("Saved.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

    st.markdown("---")
    st.markdown("### 3) Ladder + audit (fault finding)")

    warnings = ladder_validation_warnings_for_date(selected_date)
    if warnings:
        st.warning("Warnings:\n- " + "\n- ".join(warnings))

    df_audit = ladder_audit_df_for_date(selected_date)
    if df_audit.empty:
        st.info("No audit rows yet. Enter at least one result above.")
        st.stop()

    divisions = sorted([d for d in df_audit["Division"].unique().tolist() if d and d != "—"])
    if not divisions:
        divisions = ["—"]

    div_choice = st.selectbox("Division", divisions, key="ladder_div_select")

    df_ladder = ladder_table_df_for_date(selected_date, div_choice)
    if df_ladder.empty:
        st.info("No ladder rows for this division/date.")
    else:
        st.markdown("#### Ladder")
        st.dataframe(df_ladder, use_container_width=True, hide_index=True)

        st.download_button(
            "Download ladder CSV",
            data=df_ladder.to_csv(index=False).encode("utf-8"),
            file_name=f"ladder_{div_choice}_{selected_date.isoformat()}.csv".replace(" ", "_"),
            mime="text/csv",
            key="ladder_csv_btn",
        )

    st.markdown("#### Audit table (per team per game)")
    df_audit_show = df_audit[df_audit["Division"].fillna("—") == (div_choice or "—")]
    st.dataframe(df_audit_show, use_container_width=True, hide_index=True)

    st.download_button(
        "Download audit CSV",
        data=df_audit_show.to_csv(index=False).encode("utf-8"),
        file_name=f"audit_{div_choice}_{selected_date.isoformat()}.csv".replace(" ", "_"),
        mime="text/csv",
        key="audit_csv_btn",
    )


# ============================================================
# Import tab
# ============================================================
with tabs[2]:
    st.subheader("Import CSVs")

    st.markdown("### Games CSV")
    st.caption("Required columns: game_id, date, start_time, home_team, away_team, field")

    games_file = st.file_uploader("Upload Games CSV", type=["csv"], key="games_csv")
    if games_file:
        df_games = pd.read_csv(games_file)
        st.dataframe(df_games.head(20), use_container_width=True)
        if st.button("Import Games", key="import_games_btn"):
            ins, upd = import_games_csv(df_games)
            st.success(f"Imported games. Inserted: {ins}, Updated: {upd}")
            st.rerun()

    st.markdown("---")

    st.markdown("### Referees CSV")
    st.caption("Required columns: name, email  (optional: phone)")

    replace_mode = st.checkbox(
        "Replace ALL referees with this CSV (overwrite existing list)",
        value=False,
        key="replace_refs_mode",
    )

    refs_file = st.file_uploader("Upload Referees CSV", type=["csv"], key="refs_csv")
    if refs_file:
        df_refs = pd.read_csv(refs_file)
        st.dataframe(df_refs.head(20), use_container_width=True)

        if st.button("Import Referees", key="import_refs_btn"):
            try:
                if replace_mode:
                    count = replace_referees_csv(df_refs)
                    st.success(
                        f"Replaced referee list successfully. Imported {count} referee(s). "
                        "All assignments were reset to EMPTY."
                    )
                    for k in [k for k in st.session_state.keys() if str(k).startswith("refpick_")]:
                        st.session_state.pop(k, None)
                else:
                    added, updated = import_referees_csv(df_refs)
                    st.success(f"Imported referees. Added: {added}, Updated: {updated}")

                st.rerun()
            except Exception as e:
                st.error(str(e))

    st.markdown("---")

    st.markdown("### Blackouts CSV (optional)")
    st.caption("Required columns: email, blackout_date")

    bl_file = st.file_uploader("Upload Blackouts CSV", type=["csv"], key="bl_csv")
    if bl_file:
        df_bl = pd.read_csv(bl_file)
        st.dataframe(df_bl.head(20), use_container_width=True)
        if st.button("Import Blackouts", key="import_bl_btn"):
            added, skipped = import_blackouts_csv(df_bl)
            st.success(f"Imported blackouts. Added: {added}. Skipped: {skipped}")
            st.rerun()

# ============================================================
# Blackouts tab
# ============================================================
with tabs[3]:
    st.subheader("Manage Blackout Dates (date-only)")

    refs = get_referees()
    if not refs:
        st.info("Import referees first.")
    else:
        ref_map = {f"{r['name']} ({r['email']})": r["id"] for r in refs}
        choice = st.selectbox("Select referee", list(ref_map.keys()), key="blackout_ref_select")
        ref_id = ref_map[choice]

        add_date = st.date_input("Add blackout date", value=date.today(), key="blackout_add_date")
        if st.button("Add date", key="blackout_add_btn"):
            conn = db()
            try:
                conn.execute(
                    "INSERT INTO blackouts(referee_id, blackout_date) VALUES (?, ?)",
                    (ref_id, add_date.isoformat()),
                )
                conn.commit()
                st.success("Added blackout date.")
            except sqlite3.IntegrityError:
                st.warning("That date is already in the blackout list.")
            finally:
                conn.close()

        st.markdown("### Current blackout dates")
        conn = db()
        rows = conn.execute(
            """
            SELECT blackout_date FROM blackouts
            WHERE referee_id=?
            ORDER BY blackout_date ASC
            """,
            (ref_id,),
        ).fetchall()
        conn.close()

        if rows:
            dates = [r["blackout_date"] for r in rows]
            del_date = st.selectbox("Remove blackout date", dates, key="blackout_del_select")
            if st.button("Remove selected date", key="blackout_del_btn"):
                conn = db()
                conn.execute(
                    "DELETE FROM blackouts WHERE referee_id=? AND blackout_date=?",
                    (ref_id, del_date),
                )
                conn.commit()
                conn.close()
                st.success("Removed.")
                st.rerun()
        else:
            st.caption("No blackout dates set.")

# ============================================================
# Administrators tab
# ============================================================
with tabs[4]:
    st.subheader("Administrators (allowlist)")
    st.caption("Add/remove admins by email. Removed admins lose access immediately.")

    admins = list_admins()
    if admins:
        df_admins = pd.DataFrame(
            [
                {
                    "email": a["email"],
                    "active": "YES" if a["active"] == 1 else "NO",
                    "created_at": a["created_at"],
                }
                for a in admins
            ]
        )
        st.dataframe(df_admins, use_container_width=True)

    st.markdown("### Add admin")
    new_admin = st.text_input("Email to add", key="add_admin_email")
    if st.button("Add admin", key="add_admin_btn"):
        if not new_admin.strip():
            st.error("Enter an email.")
        else:
            add_admin(new_admin)
            st.success("Admin added (or already existed).")
            st.rerun()

    st.markdown("### Remove/disable admin")
    active_emails = [a["email"] for a in admins if a["active"] == 1]
    if active_emails:
        disable_email = st.selectbox("Select admin to disable", active_emails, key="disable_admin_select")
        if st.button("Disable selected admin", key="disable_admin_btn"):
            if disable_email == st.session_state.get("admin_email"):
                st.error("You can't disable yourself while logged in.")
            else:
                set_admin_active(disable_email, False)
                st.success("Admin disabled.")
                st.rerun()
    else:
        st.info("No active admins found (you should add at least one).")
# app.py
# Referee Allocator (MVP) — Admin + Referee Portal + Offers + Blackouts + Printable PDFs

import os
import sqlite3
import secrets
import smtplib
import streamlit.components.v1 as components
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font' Alignment' PatternFill
from openpyxl.utils import get_column_letter


def preserve_scroll(scroll_key: str = "refalloc_admin_scroll"):
    """
    Persists the page scroll position (window.scrollY) in localStorage and
    restores it after every Streamlit rerun (including st_autorefresh).
    """
    components.html(
        f"""
        <script>
        (function() {{
          const KEY = "{scroll_key}";

          // ⛔ Install scroll listener ONLY ONCE per page load
          if (!window.__refallocScrollInstalled) {{
            window.__refallocScrollInstalled = true;

            let ticking = false;
            window.addEventListener("scroll"' function() {{
              if (!ticking) {{
                window.requestAnimationFrame(function() {{
                  try {{
                    localStorage.setItem(KEY' String(window.scrollY || 0));
                  }} catch (e) {{}}
                  ticking = false;
                }});
                ticking = true;
              }}
            }}' {{ passive: true }});
          }}

          // Restore after Streamlit finishes laying out the page
          function restore() {{
            let y = 0;
            try {{
              y = parseInt(localStorage.getItem(KEY) || "0"' 10) || 0;
            }} catch (e) {{}}

            const maxY = Math.max(0' document.body.scrollHeight - window.innerHeight);
            if (y > maxY) y = maxY;

            window.scrollTo(0' y);
          }}

          // Multiple delayed restores handles layout changes
          window.setTimeout(restore' 0);
          window.setTimeout(restore' 80);
          window.setTimeout(restore' 200);
        }})();
        </script>
        """'
        height=0'
        width=0'
    )

from pathlib import Path
from datetime import datetime' date' timedelta' timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from io import BytesIO

import pandas as pd
import streamlit as st
from dateutil import parser as dtparser
from streamlit_autorefresh import st_autorefresh

# PDF (ReportLab)
from reportlab.lib.pagesizes import A4' landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate' Paragraph' Spacer' Table' TableStyle
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm


# ============================================================
# CONFIG
# ============================================================
BASE_DIR = Path(__file__).resolve().parent

DB_PATH = os.getenv("DB_PATH"' str(BASE_DIR / "league.db"))
Path(DB_PATH).expanduser().parent.mkdir(parents=True' exist_ok=True)

REF_PORTAL_ENABLED = os.getenv("REF_PORTAL_ENABLED"' "false").lower() == "true"
DEBUG_BANNER = os.getenv("DEBUG_BANNER"' "false").lower() == "true"

st.set_page_config(page_title="Referee Allocator (MVP)"' layout="wide")

if DEBUG_BANNER:
    st.warning("DEBUG: App reached top of script ✅")
    st.write("DEBUG: query_params ="' dict(st.query_params))
    st.write("DEBUG: session_state keys ="' list(st.session_state.keys()))
    st.markdown("---")


# ============================================================
# ⚠️ CORE HELPERS — DO NOT MOVE BELOW THIS LINE
# ============================================================

# ============================================================
# Small utilities
# ============================================================
def game_local_date(game_row) -> date:
    """
    Returns the local calendar date for a game row.
    Expects game_row to have 'start_dt' (ISO string).
    """
    dt = dtparser.parse(game_row["start_dt"])
    return dt.date()


def referee_has_blackout(ref_id: int' d: date) -> bool:
    """
    Returns True if the referee has a blackout on the given date.
    """
    conn = db()
    row = conn.execute(
        """
        SELECT 1
        FROM blackouts
        WHERE referee_id=? AND blackout_date=?
        LIMIT 1
        """'
        (ref_id' d.isoformat())'
    ).fetchone()
    conn.close()
    return bool(row)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def status_badge(text: str' bg: str' fg: str = "white"):
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
        """'
        unsafe_allow_html=True'
    )


def _time_12h(dt: datetime) -> str:
    return dt.strftime("%I:%M %p").lstrip("0")


# ============================================================
# DB
# ============================================================
def db():
    conn = sqlite3.connect(DB_PATH' check_same_thread=False)
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
    - game_results: one row per game with admin-entered scoring inputs
      + default flags
    """
    conn = db()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT'
                name TEXT NOT NULL UNIQUE'
                division TEXT NOT NULL'
                opening_balance INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        cols = conn.execute("PRAGMA table_info(teams);").fetchall()
        col_names = {c["name"] for c in cols}
        if "opening_balance" not in col_names:
            conn.execute("ALTER TABLE teams ADD COLUMN opening_balance INTEGER NOT NULL DEFAULT 0;")

        cur.execute(
            """
                    cur.execute(
            """
            CREATE TABLE IF NOT EXISTS game_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT'
                game_id INTEGER NOT NULL UNIQUE'

                home_score INTEGER NOT NULL DEFAULT 0'
                away_score INTEGER NOT NULL DEFAULT 0'

                home_female_tries INTEGER NOT NULL DEFAULT 0'
                away_female_tries INTEGER NOT NULL DEFAULT 0'

                home_conduct INTEGER NOT NULL DEFAULT 0'   -- 0..10
                away_conduct INTEGER NOT NULL DEFAULT 0'   -- 0..10

                home_unstripped INTEGER NOT NULL DEFAULT 0'
                away_unstripped INTEGER NOT NULL DEFAULT 0'

                home_defaulted INTEGER NOT NULL DEFAULT 0'
                away_defaulted INTEGER NOT NULL DEFAULT 0'

                updated_at TEXT NOT NULL'

                FOREIGN KEY(game_id) REFERENCES games(id) ON DELETE CASCADE
            );
            """
        )

        # Safe add columns for existing DBs
        cols = conn.execute("PRAGMA table_info(game_results);").fetchall()
        gr_cols = {c["name"] for c in cols}

        if "home_defaulted" not in gr_cols:
            conn.execute("ALTER TABLE game_results ADD COLUMN home_defaulted INTEGER NOT NULL DEFAULT 0;")
        if "away_defaulted" not in gr_cols:
            conn.execute("ALTER TABLE game_results ADD COLUMN away_defaulted INTEGER NOT NULL DEFAULT 0;")

        conn.commit()
    finally:
        conn.close()


def init_db():
    ensure_referees_phone_column()
    ensure_ladder_tables()
    conn = db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS referees (
            id INTEGER PRIMARY KEY AUTOINCREMENT'
            name TEXT NOT NULL'
            email TEXT NOT NULL UNIQUE'
            active INTEGER NOT NULL DEFAULT 1
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT'
            game_key TEXT NOT NULL UNIQUE'
            home_team TEXT NOT NULL'
            away_team TEXT NOT NULL'
            field_name TEXT NOT NULL'
            start_dt TEXT NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT'
            game_id INTEGER NOT NULL'
            slot_no INTEGER NOT NULL'
            referee_id INTEGER'
            status TEXT NOT NULL DEFAULT 'EMPTY''
            updated_at TEXT NOT NULL'
            UNIQUE(game_id' slot_no)'
            FOREIGN KEY(game_id) REFERENCES games(id)'
            FOREIGN KEY(referee_id) REFERENCES referees(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT'
            assignment_id INTEGER NOT NULL'
            token TEXT NOT NULL UNIQUE'
            created_at TEXT NOT NULL'
            responded_at TEXT'
            response TEXT'
            FOREIGN KEY(assignment_id) REFERENCES assignments(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS blackouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT'
            referee_id INTEGER NOT NULL'
            blackout_date TEXT NOT NULL'
            UNIQUE(referee_id' blackout_date)'
            FOREIGN KEY(referee_id) REFERENCES referees(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT'
            email TEXT NOT NULL UNIQUE'
            active INTEGER NOT NULL DEFAULT 1'
            created_at TEXT NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT'
            email TEXT NOT NULL'
            token TEXT NOT NULL UNIQUE'
            created_at TEXT NOT NULL'
            expires_at TEXT NOT NULL'
            used_at TEXT
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT'
            email TEXT NOT NULL'
            token TEXT NOT NULL UNIQUE'
            created_at TEXT NOT NULL'
            expires_at TEXT NOT NULL'
            revoked_at TEXT
        );
        """
    )

    conn.commit()
    conn.close()


# ============================================================
# Email (SMTP)
# ============================================================
def smtp_settings():
    """
    Required:
      SMTP_HOST' SMTP_PORT' SMTP_USER' SMTP_PASSWORD'
      SMTP_FROM_EMAIL' SMTP_FROM_NAME' APP_BASE_URL
    """
    secrets_dict = {}

    secrets_paths = [
        "/opt/render/.streamlit/secrets.toml"'
        "/opt/render/project/src/.streamlit/secrets.toml"'
        str(BASE_DIR / ".streamlit" / "secrets.toml")'
    ]
    if any(os.path.exists(p) for p in secrets_paths):
        try:
            secrets_dict = dict(st.secrets)
        except Exception:
            secrets_dict = {}

    def get(key: str' default: str = "") -> str:
        return os.environ.get(key' str(secrets_dict.get(key' default)))

    return {
        "host": get("SMTP_HOST"' "")'
        "port": int(get("SMTP_PORT"' "587") or 587)'
        "user": get("SMTP_USER"' "")'
        "password": get("SMTP_PASSWORD"' "")'
        "from_email": get("SMTP_FROM_EMAIL"' "")'
        "from_name": get("SMTP_FROM_NAME"' "Referee Allocator")'
        "app_base_url": get("APP_BASE_URL"' "").rstrip("/")'
    }


def send_html_email(
    to_email: str'
    to_name: str'
    subject: str'
    html_body: str'
    text_body: str | None = None'
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
            "Email not configured. Set SMTP_HOST' SMTP_PORT' SMTP_USER' SMTP_PASSWORD' "
            "SMTP_FROM_EMAIL' SMTP_FROM_NAME' APP_BASE_URL."
        )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f'{cfg["from_name"]} <{cfg["from_email"]}>'
    msg["To"] = f"{to_name} <{to_email}>"

    if not text_body:
        text_body = "You have a notification from Referee Allocator."
    msg.attach(MIMEText(text_body' "plain"))
    msg.attach(MIMEText(html_body' "html"))

    with smtplib.SMTP(cfg["host"]' cfg["port"]) as server:
        server.starttls()
        server.login(cfg["user"]' cfg["password"])
        server.sendmail(cfg["from_email"]' [to_email]' msg.as_string())


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
        "INSERT OR IGNORE INTO admins(email' active' created_at) VALUES (?' 1' ?)"'
        (email' now_iso())'
    )
    conn.commit()
    conn.close()


def is_admin_email_allowed(email: str) -> bool:
    conn = db()
    row = conn.execute(
        "SELECT 1 FROM admins WHERE email=? AND active=1 LIMIT 1"'
        (email.strip().lower()')'
    ).fetchone()
    conn.close()
    return bool(row)


def set_admin_active(email: str' active: bool):
    conn = db()
    conn.execute(
        "UPDATE admins SET active=? WHERE email=?"'
        (1 if active else 0' email.strip().lower())'
    )
    conn.commit()
    conn.close()


def list_admins():
    conn = db()
    rows = conn.execute(
        "SELECT email' active' created_at FROM admins ORDER BY email ASC"
    ).fetchall()
    conn.close()
    return rows


def create_admin_login_token(email: str' minutes_valid: int = 15) -> str:
    token = secrets.token_urlsafe(32)
    created = datetime.now(timezone.utc)
    expires = created + timedelta(minutes=minutes_valid)

    conn = db()
    conn.execute(
        """
        INSERT INTO admin_tokens(email' token' created_at' expires_at)
        VALUES (?' ?' ?' ?)
        """'
        (
            email.strip().lower()'
            token'
            created.isoformat(timespec="seconds")'
            expires.isoformat(timespec="seconds")'
        )'
    )
    conn.commit()
    conn.close()
    return token


def consume_admin_token(token: str) -> tuple[bool' str]:
    conn = db()
    row = conn.execute(
        """
        SELECT id' email' expires_at' used_at
        FROM admin_tokens
        WHERE token=?
        """'
        (token')'
    ).fetchone()

    if not row:
        conn.close()
        return False' "Invalid or unknown login link."

    if row["used_at"] is not None:
        conn.close()
        return False' "This login link has already been used."

    expires_at = dtparser.parse(row["expires_at"])
    if datetime.now(timezone.utc) > expires_at:
        conn.close()
        return False' "This login link has expired. Please request a new one."

    email = row["email"].strip().lower()
    if not is_admin_email_allowed(email):
        conn.close()
        return False' "This email is not an authorised administrator."

    conn.execute("UPDATE admin_tokens SET used_at=? WHERE id=?"' (now_iso()' row["id"]))
    conn.commit()
    conn.close()
    return True' email


def create_admin_session(email: str' days_valid: int = 14) -> str:
    token = secrets.token_urlsafe(32)
    created = datetime.now(timezone.utc)
    expires = created + timedelta(days=days_valid)

    conn = db()
    conn.execute(
        """
        INSERT INTO admin_sessions(email' token' created_at' expires_at' revoked_at)
        VALUES (?' ?' ?' ?' NULL)
        """'
        (
            email.strip().lower()'
            token'
            created.isoformat(timespec="seconds")'
            expires.isoformat(timespec="seconds")'
        )'
    )
    conn.commit()
    conn.close()
    return token


def consume_admin_session(token: str) -> tuple[bool' str]:
    conn = db()
    row = conn.execute(
        """
        SELECT email' expires_at' revoked_at
        FROM admin_sessions
        WHERE token=?
        LIMIT 1
        """'
        (token')'
    ).fetchone()

    if not row:
        conn.close()
        return False' "Invalid session."

    if row["revoked_at"] is not None:
        conn.close()
        return False' "Session revoked."

    if datetime.now(timezone.utc) > dtparser.parse(row["expires_at"]):
        conn.close()
        return False' "Session expired."

    email = row["email"].strip().lower()
    if not is_admin_email_allowed(email):
        conn.close()
        return False' "Not authorised."

    conn.close()
    return True' email


def maybe_restore_admin_from_session_param():
    qp = st.query_params
    token = qp.get("session")
    if token and not st.session_state.get("admin_email"):
        ok' value = consume_admin_session(token)
        if ok:
            st.session_state["admin_email"] = value
        else:
            st.query_params.pop("session"' None)
            st.rerun()


def send_admin_login_email(email: str) -> str:
    email = email.strip().lower()
    cfg = smtp_settings()
    base = cfg.get("app_base_url"' "").rstrip("/")
    token = create_admin_login_token(email)
    login_url = f"{base}/?admin_login=1&token={token}"

    subject = "Admin login link"
    text = (
        "Use this link to sign in as an administrator (expires in 15 minutes):\n"
        f"{login_url}\n"
    )
    html = f"""
    <div style="font-family: Arial' sans-serif; line-height: 1.4;">
      <p>Hi'</p>
      <p>Use the button below to sign in as an administrator.
         This link expires in <b>15 minutes</b>.</p>
      <p>
        <a href="{login_url}" style="display:inline-block;padding:10px 14px;background:#1565c0;color:#fff;text-decoration:none;border-radius:6px;">
          Sign in
        </a>
      </p>
      <p>If you didn't request this' you can ignore this email.</p>
    </div>
    """
    send_html_email(email' email' subject' html' text_body=text)
    return login_url


def handle_admin_login_via_query_params():
    qp = st.query_params
    if qp.get("admin_login") == "1" and qp.get("token"):
        token = qp.get("token")
        ok' value = consume_admin_token(token)
        if ok:
            st.session_state["admin_email"] = value
            session_token = create_admin_session(value)

            st.query_params.pop("admin_login"' None)
            st.query_params.pop("token"' None)
            st.query_params["session"] = session_token
            st.rerun()
        else:
            st.title("Admin Login")
            st.error(value)
            st.info("Please go back and request a new login link.")
            st.stop()


def admin_logout_button():
    if st.session_state.get("admin_email"):
        c1' c2 = st.columns([3' 1])
        with c1:
            st.caption(f"Logged in as: {st.session_state['admin_email']}")
        with c2:
            if st.button("Log out"):
                st.session_state.pop("admin_email"' None)
                st.query_params.pop("session"' None)
                st.rerun()


def create_admin_session_with_expires_at(email: str' expires_at_iso: str) -> str:
    """
    DEV helper: creates an admin session with a fixed expiry.
    Used for permanent DEV admin URLs (no email).
    """
    token = secrets.token_urlsafe(32)

    conn = db()
    conn.execute(
        """
        INSERT INTO admin_sessions(email' token' created_at' expires_at' revoked_at)
        VALUES (?' ?' ?' ?' NULL)
        """'
        (
            email.strip().lower()'
            token'
            now_iso()'
            expires_at_iso'
        )'
    )
    conn.commit()
    conn.close()

    return token


# ============================================================
# Imports & data helpers
# ============================================================
def import_referees_csv(df: pd.DataFrame):
    cols = {c.lower().strip(): c for c in df.columns}
    if "name" not in cols or "email" not in cols:
        raise ValueError("Referees CSV must contain columns: name' email")

    phone_col = cols.get("phone")  # optional

    conn = db()
    cur = conn.cursor()
    added = 0
    updated = 0

    for _' row in df.iterrows():
        name = str(row[cols["name"]]).strip()
        email = str(row[cols["email"]]).strip().lower()
        phone = ""
        if phone_col:
            phone = str(row[phone_col]).strip()
            if phone.lower() == "nan":
                phone = ""

        if not name or not email or email == "nan":
            continue

        cur.execute("SELECT id' name' COALESCE(phone''') AS phone FROM referees WHERE email=?"' (email'))
        existing = cur.fetchone()

        if existing:
            needs_update = False
            if existing["name"] != name:
                needs_update = True
            if phone_col and (existing["phone"] or "") != phone:
                needs_update = True

            if needs_update:
                cur.execute(
                    "UPDATE referees SET name=?' phone=? WHERE email=?"'
                    (name' phone' email)'
                )
                updated += 1
        else:
            cur.execute(
                "INSERT INTO referees(name' email' phone' active) VALUES (?' ?' ?' 1)"'
                (name' email' phone)'
            )
            added += 1

    conn.commit()
    conn.close()
    return added' updated


def replace_referees_csv(df: pd.DataFrame):
    cols = {c.lower().strip(): c for c in df.columns}
    if "name" not in cols or "email" not in cols:
        raise ValueError("Referees CSV must contain columns: name' email")

    phone_col = cols.get("phone")  # optional

    new_refs = []
    for _' row in df.iterrows():
        name = str(row[cols["name"]]).strip()
        email = str(row[cols["email"]]).strip().lower()

        phone = ""
        if phone_col:
            phone = str(row[phone_col]).strip()
            if phone.lower() == "nan":
                phone = ""

        if not name or not email or email == "nan":
            continue

        new_refs.append((name' email' phone))

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
            SET referee_id=NULL' status='EMPTY'' updated_at=?
            """'
            (now_iso()')'
        )
        cur.execute("DELETE FROM blackouts")
        cur.execute("DELETE FROM referees")

        try:
            cur.execute("DELETE FROM sqlite_sequence WHERE name IN ('referees'''blackouts'''offers')")
        except Exception:
            pass

        cur.executemany(
            "INSERT INTO referees(name' email' phone' active) VALUES (?' ?' ?' 1)"'
            new_refs'
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
    for k in ["game_id"' "game_key"' "id"]:
        if k in cols:
            key_col = cols[k]
            break

    has_date_time = "date" in cols and "start_time" in cols
    has_start_dt = "start_datetime" in cols

    required = ["home_team"' "away_team"' "field"]
    missing = [r for r in required if r not in cols]
    if not key_col:
        missing.append("game_id")
    if not (has_date_time or has_start_dt):
        missing.append("date + start_time (or start_datetime)")

    if missing:
        raise ValueError(f"Games CSV missing columns: {'' '.join(missing)}")

    conn = db()
    cur = conn.cursor()

    inserted' updated = 0' 0
    for _' row in df.iterrows():
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

        cur.execute("SELECT id FROM games WHERE game_key=?"' (game_key'))
        g = cur.fetchone()

        if g:
            cur.execute(
                """
                UPDATE games
                SET home_team=?' away_team=?' field_name=?' start_dt=?
                WHERE game_key=?
                """'
                (home' away' field' start_iso' game_key)'
            )
            updated += 1
            game_id = g["id"]
        else:
            cur.execute(
                """
                INSERT INTO games(game_key' home_team' away_team' field_name' start_dt)
                VALUES (?' ?' ?' ?' ?)
                """'
                (game_key' home' away' field' start_iso)'
            )
            inserted += 1
            game_id = cur.lastrowid

        for slot in (1' 2):
            cur.execute(
                "SELECT 1 FROM assignments WHERE game_id=? AND slot_no=?"'
                (game_id' slot)'
            )
            if not cur.fetchone():
                cur.execute(
                    """
                    INSERT INTO assignments(game_id' slot_no' referee_id' status' updated_at)
                    VALUES (?' ?' NULL' 'EMPTY'' ?)
                    """'
                    (game_id' slot' now_iso())'
                )

    conn.commit()
    conn.close()
    return inserted' updated


def import_blackouts_csv(df: pd.DataFrame):
    cols = {c.lower().strip(): c for c in df.columns}
    if "email" not in cols or "blackout_date" not in cols:
        raise ValueError("Blackouts CSV must contain columns: email' blackout_date")

    conn = db()
    cur = conn.cursor()

    added' skipped = 0' 0
    for _' row in df.iterrows():
        email = str(row[cols["email"]]).strip().lower()
        d_raw = str(row[cols["blackout_date"]]).strip()
        if not email or email == "nan" or not d_raw or d_raw == "nan":
            continue

        cur.execute("SELECT id FROM referees WHERE email=?"' (email'))
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
                "INSERT INTO blackouts(referee_id' blackout_date) VALUES (?' ?)"'
                (r["id"]' d.isoformat())'
            )
            added += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    return added' skipped


def get_games():
    conn = db()
    rows = conn.execute(
        """
        SELECT id' game_key' home_team' away_team' field_name' start_dt
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
        SELECT id' name' email' COALESCE(phone''') AS phone
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
            a.id'
            a.slot_no'
            a.referee_id'
            a.status'
            a.updated_at'
            r.name AS ref_name'
            r.email AS ref_email
        FROM assignments a
        LEFT JOIN referees r ON r.id = a.referee_id
        WHERE a.game_id=?
        ORDER BY a.slot_no ASC
        """'
        (game_id')'
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
            a.id'
            a.game_id'
            a.slot_no'
            a.referee_id'
            a.status'
            a.updated_at'
            r.name  AS ref_name'
            r.email AS ref_email'
            COALESCE(r.phone''') AS ref_phone
        FROM assignments a
        LEFT JOIN referees r ON r.id = a.referee_id
        WHERE a.id = ?
        LIMIT 1
        """'
        (int(assignment_id)')'
    ).fetchone()
    conn.close()
    return row

# ============================================================
# Assignment helpers
# ============================================================

def set_assignment_status(assignment_id: int' status: str):
    status = (status or "EMPTY").strip().upper()
    conn = db()
    conn.execute(
        """
        UPDATE assignments
        SET status=?' updated_at=?
        WHERE id=?
        """'
        (status' now_iso()' int(assignment_id))'
    )
    conn.commit()
    conn.close()


def set_assignment_ref(assignment_id: int' referee_id: int):
    """
    Set the referee for a slot.
    If a referee is set and the slot was EMPTY' we move it to NOT_OFFERED
    (your UI treats any non-empty referee as 'NOT OFFERED YET' unless OFFERED/DECLINED/ACCEPTED/ASSIGNED).
    """
    conn = db()

    # Keep status consistent with your UI badges
    cur = conn.execute(
        "SELECT referee_id' status FROM assignments WHERE id=? LIMIT 1"'
        (int(assignment_id)')'
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
        SET referee_id=?' status=?' updated_at=?
        WHERE id=?
        """'
        (int(referee_id)' new_status' now_iso()' int(assignment_id))'
    )
    conn.commit()
    conn.close()


def _delete_offers_for_assignment(conn: sqlite3.Connection' assignment_id: int):
    """
    Internal helper: ensure no stale offer links remain for this assignment.
    """
    conn.execute("DELETE FROM offers WHERE assignment_id=?"' (int(assignment_id)'))


def clear_assignment(assignment_id: int):
    """
    Clears the slot back to EMPTY and removes any offers for that assignment.
    This is what your RESET/DELETE action needs.
    """
    conn = db()
    try:
        _delete_offers_for_assignment(conn' assignment_id)
        conn.execute(
            """
            UPDATE assignments
            SET referee_id=NULL' status='EMPTY'' updated_at=?
            WHERE id=?
            """'
            (now_iso()' int(assignment_id))'
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
    "Division 1"'
    "Division 2"'
    "Division 3"'
    "Golden Oldies"'
    "Other"'
]


def ensure_ladder_tables():
    """
    Safe migrations for ladder system:
    - teams: team name + division + opening_balance
    - game_results: one row per game with admin-entered scoring inputs
    """
    conn = db()
    try:
        cur = conn.cursor()

        # New installs: include opening_balance in CREATE TABLE
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS teams (
                id INTEGER PRIMARY KEY AUTOINCREMENT'
                name TEXT NOT NULL UNIQUE'
                division TEXT NOT NULL'
                opening_balance INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        # Existing DBs: add column if missing
        cols = conn.execute("PRAGMA table_info(teams);").fetchall()
        col_names = {c["name"] for c in cols}
        if "opening_balance" not in col_names:
            conn.execute("ALTER TABLE teams ADD COLUMN opening_balance INTEGER NOT NULL DEFAULT 0;")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS game_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT'
                game_id INTEGER NOT NULL UNIQUE'

                home_score INTEGER NOT NULL DEFAULT 0'
                away_score INTEGER NOT NULL DEFAULT 0'

                home_female_tries INTEGER NOT NULL DEFAULT 0'
                away_female_tries INTEGER NOT NULL DEFAULT 0'

                home_conduct INTEGER NOT NULL DEFAULT 0'   -- 0..10
                away_conduct INTEGER NOT NULL DEFAULT 0'   -- 0..10

                home_unstripped INTEGER NOT NULL DEFAULT 0'
                away_unstripped INTEGER NOT NULL DEFAULT 0'

                updated_at TEXT NOT NULL'

                FOREIGN KEY(game_id) REFERENCES games(id) ON DELETE CASCADE
            );
            """
        )

        conn.commit()
    finally:
        conn.close()


def upsert_team(name: str' division: str' opening_balance: int = 0):
    name = (name or "").strip()
    division = (division or "").strip()
    if not name or not division:
        return

    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO teams(name' division' opening_balance)
            VALUES (?' ?' ?)
            ON CONFLICT(name) DO UPDATE SET
                division=excluded.division'
                opening_balance=excluded.opening_balance
            """'
            (name' division' int(opening_balance or 0))'
        )
        conn.commit()
    finally:
        conn.close()


def list_teams() -> list[sqlite3.Row]:
    conn = db()
    rows = conn.execute(
        """
        SELECT
            id'
            name'
            division'
            COALESCE(opening_balance'0) AS opening_balance
        FROM teams
        ORDER BY division ASC' name ASC
        """
    ).fetchall()
    conn.close()
    return rows


def get_team_division(name: str) -> str:
    conn = db()
    row = conn.execute(
        "SELECT division FROM teams WHERE name=? LIMIT 1"'
        ((name or "").strip()')'
    ).fetchone()
    conn.close()
    return (row["division"] if row else "").strip()


def get_game_result(game_id: int) -> sqlite3.Row | None:
    conn = db()
    row = conn.execute(
        """
        SELECT
            game_id'
            home_score' away_score'
            home_female_tries' away_female_tries'
            home_conduct' away_conduct'
            home_unstripped' away_unstripped'
            COALESCE(home_defaulted'0) AS home_defaulted'
            COALESCE(away_defaulted'0) AS away_defaulted'
            updated_at
        FROM game_results
        WHERE game_id=?
        LIMIT 1
        """'
        (game_id')'
    ).fetchone()
    conn.close()
    return row


def upsert_game_result(
    *'
    game_id: int'
    home_score: int'
    away_score: int'
    home_female_tries: int'
    away_female_tries: int'
    home_conduct: int'
    away_conduct: int'
    home_unstripped: int'
    away_unstripped: int'
    home_defaulted: int = 0'
    away_defaulted: int = 0'
):
    # Defensive: never allow both to be 1
    hd = 1 if int(home_defaulted or 0) else 0
    ad = 1 if int(away_defaulted or 0) else 0
    if hd == 1 and ad == 1:
        hd = 0
        ad = 0

    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO game_results(
                game_id'
                home_score' away_score'
                home_female_tries' away_female_tries'
                home_conduct' away_conduct'
                home_unstripped' away_unstripped'
                home_defaulted' away_defaulted'
                updated_at
            )
            VALUES (?' ?' ?' ?' ?' ?' ?' ?' ?' ?' ?' ?)
            ON CONFLICT(game_id) DO UPDATE SET
                home_score=excluded.home_score'
                away_score=excluded.away_score'
                home_female_tries=excluded.home_female_tries'
                away_female_tries=excluded.away_female_tries'
                home_conduct=excluded.home_conduct'
                away_conduct=excluded.away_conduct'
                home_unstripped=excluded.home_unstripped'
                away_unstripped=excluded.away_unstripped'
                home_defaulted=excluded.home_defaulted'
                away_defaulted=excluded.away_defaulted'
                updated_at=excluded.updated_at
            """'
            (
                int(game_id)'
                int(home_score)' int(away_score)'
                int(home_female_tries)' int(away_female_tries)'
                int(home_conduct)' int(away_conduct)'
                int(home_unstripped)' int(away_unstripped)'
                int(hd)' int(ad)'
                now_iso()'
            )'
        )
        conn.commit()
    finally:
        conn.close()


def ladder_audit_df_for_date(selected_date: date) -> pd.DataFrame:
    """
    Returns per-team per-game computed scoring' plus inputs' for fault finding.
    Only games on selected_date.
    """
    start_min = datetime.combine(selected_date' datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(selected_date + timedelta(days=1)' datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    rows = conn.execute(
        """
        WITH base AS (
            SELECT
                g.id AS game_id'
                g.start_dt'
                g.field_name'

                g.home_team AS home_team'
                g.away_team AS away_team'

                COALESCE(t1.division''') AS home_division'
                COALESCE(t2.division''') AS away_division'

                COALESCE(t1.opening_balance'0) AS home_opening'
                COALESCE(t2.opening_balance'0) AS away_opening'

                gr.home_score' gr.away_score'
                gr.home_female_tries' gr.away_female_tries'
                gr.home_conduct' gr.away_conduct'
                gr.home_unstripped' gr.away_unstripped'
                gr.updated_at'
                COALESCE(gr.home_defaulted'0) AS home_defaulted'
                COALESCE(gr.away_defaulted'0) AS away_defaulted

            FROM games g
            LEFT JOIN teams t1 ON t1.name = g.home_team
            LEFT JOIN teams t2 ON t2.name = g.away_team
            LEFT JOIN game_results gr ON gr.game_id = g.id
            WHERE g.start_dt >= ? AND g.start_dt < ?
        )'
        teamsplit AS (
            SELECT
                game_id' start_dt' field_name'
                home_team AS team'
                away_team AS opponent'
                home_division AS division'
                home_opening AS opening_balance'

                home_score AS pf'
                away_score AS pa'

                home_female_tries AS female_tries'
                home_conduct AS conduct'
                home_unstripped AS unstripped'

                home_defaulted AS team_defaulted'
                away_defaulted AS opp_defaulted'

                updated_at
            FROM base

            UNION ALL

            SELECT
                game_id' start_dt' field_name'
                away_team AS team'
                home_team AS opponent'
                away_division AS division'
                away_opening AS opening_balance'

                away_score AS pf'
                home_score AS pa'

                away_female_tries AS female_tries'
                away_conduct AS conduct'
                away_unstripped AS unstripped'

                away_defaulted AS team_defaulted'
                home_defaulted AS opp_defaulted'

                updated_at
            FROM base
        )

        SELECT
            game_id'
            start_dt'
            field_name'
            division'
            team'
            opponent'
            opening_balance'
            pf'
            pa'
            (pf - pa) AS margin'

            CASE
              WHEN team_defaulted = 1 THEN 'L'
              WHEN opp_defaulted = 1 THEN 'W'
              WHEN pf > pa THEN 'W'
              WHEN pf = pa THEN 'D'
              ELSE 'L'
            END AS result'

            CASE
              WHEN team_defaulted = 1 THEN 0
              WHEN opp_defaulted = 1 THEN ?
              WHEN pf > pa THEN ?
              WHEN pf = pa THEN ?
              ELSE ?
            END AS match_pts'


            CASE
              WHEN pf < pa AND (pa - pf) IN (1'2) THEN 1 ELSE 0
            END AS close_loss_bp'

            female_tries'
            CASE WHEN female_tries >= 4 THEN 1 ELSE 0 END AS female_bp'

            CASE
              WHEN team_defaulted = 1 OR opp_defaulted = 1 THEN 10
              ELSE conduct
            END AS conduct'

            unstripped'
            CASE WHEN unstripped >= 3 THEN -2 ELSE 0 END AS unstripped_pen'

            updated_at
        FROM teamsplit
        ORDER BY start_dt ASC' field_name ASC' team ASC
        """'
        (start_min' start_max' LADDER_WIN_PTS' LADDER_WIN_PTS' LADDER_DRAW_PTS' LADDER_LOSS_PTS)'
    ).fetchall()

    conn.close()

    out = []
    for r in rows:
        match_pts = int(r["match_pts"] or 0)
        close_bp = int(r["close_loss_bp"] or 0)
        female_bp = int(r["female_bp"] or 0)
        conduct = int(r["conduct"] or 0)
        pen = int(r["unstripped_pen"] or 0)

        total = match_pts + close_bp + female_bp + conduct + pen

        out.append(
            {
                "Start": _time_12h(dtparser.parse(r["start_dt"])) if r["start_dt"] else "—"'
                "Field": r["field_name"] or "—"'
                "Division": (r["division"] or "").strip() or "—"'
                "Team": r["team"] or "—"'
                "Opponent": r["opponent"] or "—"'
                "Opening": int(r["opening_balance"] or 0)'
                "PF": int(r["pf"] or 0)'
                "PA": int(r["pa"] or 0)'
                "Res": r["result"] or "—"'
                "Match": match_pts'
                "CloseBP": close_bp'
                "FemTries": int(r["female_tries"] or 0)'
                "FemBP": female_bp'
                "Conduct": conduct'
                "Unstrip": int(r["unstripped"] or 0)'
                "Pen": pen'
                "Points": total'
                "Updated": (r["updated_at"] or "")[:19]'
            }
        )

    return pd.DataFrame(out)


def ladder_table_df_for_date(selected_date: date' division: str) -> pd.DataFrame:
    """
    Aggregated ladder for the selected date only (plus Opening Balance).
    """
    df = ladder_audit_df_for_date(selected_date)
    if df.empty:
        return pd.DataFrame()

    div_label = (division or "—").strip() or "—"
    df = df[df["Division"].fillna("—") == div_label]
    if df.empty:
        return pd.DataFrame()

    grouped = df.groupby("Team"' dropna=False).agg(
        P=("Team"' "count")'
        W=("Res"' lambda s: int((s == "W").sum()))'
        D=("Res"' lambda s: int((s == "D").sum()))'
        L=("Res"' lambda s: int((s == "L").sum()))'
        PF=("PF"' "sum")'
        PA=("PA"' "sum")'
        Opening=("Opening"' "max")'  # should be constant per team; max is safe
        Match=("Match"' "sum")'
        CloseBP=("CloseBP"' "sum")'
        FemBP=("FemBP"' "sum")'
        Conduct=("Conduct"' "sum")'
        Pen=("Pen"' "sum")'
        Points=("Points"' "sum")'
    ).reset_index()

    grouped["PD"] = grouped["PF"] - grouped["PA"]
    grouped["Total"] = grouped["Opening"] + grouped["Points"]

    grouped = grouped.sort_values(
        by=["Total"' "PD"' "PF"' "Team"]'
        ascending=[False' False' False' True]'
    ).reset_index(drop=True)

    grouped = grouped[
        ["Team"' "P"' "W"' "D"' "L"' "PF"' "PA"' "PD"' "Opening"' "Match"' "CloseBP"' "FemBP"' "Conduct"' "Pen"' "Points"' "Total"]
    ]
    return grouped


def ladder_validation_warnings_for_date(selected_date: date) -> list[str]:
    """
    Collects human-friendly warnings to help admins fault-find quickly.
    """
    warnings: list[str] = []

    games = get_games()
    todays = [g for g in games if game_local_date(g) == selected_date]
    if not todays:
        return warnings

    # Team division coverage + opening balance awareness
    missing_div = set()
    cross_div_games = []

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
            cross_div_games.append(f"{h} vs {a} ({dh} / {da})")

    if missing_div:
        warnings.append("Missing team division for: " + "' ".join(sorted(missing_div)))

    if cross_div_games:
        warnings.append("Cross-division games detected: " + "; ".join(cross_div_games))

    # Results completeness + sanity checks
    for g in todays:
        gr = get_game_result(int(g["id"]))
        if not gr:
            warnings.append(f"Missing result entry: {g['home_team']} vs {g['away_team']}")
            continue

        for side in ("home"' "away"):
            c = int(gr[f"{side}_conduct"] or 0)
            if c < 0 or c > 10:
                warnings.append(
                    f"Conduct out of range (0-10): {g['home_team']} vs {g['away_team']} ({side}={c})"
                )

        for k in [
            "home_score"'
            "away_score"'
            "home_female_tries"'
            "away_female_tries"'
            "home_unstripped"'
            "away_unstripped"'
        ]:
            v = int(gr[k] or 0)
            if v < 0:
                warnings.append(f"Negative value {k} for game: {g['home_team']} vs {g['away_team']}")

    return warnings


# ============================================================
# Acceptance progress helpers
# ============================================================
def iso_week_window(d: date) -> tuple[date' date]:
    start = d - timedelta(days=d.weekday())  # Monday
    end_excl = start + timedelta(days=7)
    return start' end_excl


def get_acceptance_progress_for_window(start_date: date' end_date_exclusive: date) -> tuple[int' int]:
    start_min = datetime.combine(start_date' datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(end_date_exclusive' datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN UPPER(COALESCE(a.status''')) IN ('ACCEPTED'''ASSIGNED') THEN 1 ELSE 0 END) AS accepted_slots'
            COUNT(a.id) AS total_slots
        FROM games g
        JOIN assignments a ON a.game_id = g.id
        WHERE g.start_dt >= ? AND g.start_dt < ?
        """'
        (start_min' start_max)'
    ).fetchone()
    conn.close()

    accepted = int(row["accepted_slots"] or 0)
    total = int(row["total_slots"] or 0)
    return accepted' total


def list_referees_not_accepted_for_window(start_date: date' end_date_exclusive: date) -> list[str]:
    start_min = datetime.combine(start_date' datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(end_date_exclusive' datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    rows = conn.execute(
        """
        SELECT DISTINCT TRIM(r.name) AS name
        FROM games g
        JOIN assignments a ON a.game_id = g.id
        JOIN referees r ON r.id = a.referee_id
        WHERE g.start_dt >= ? AND g.start_dt < ?
          AND a.referee_id IS NOT NULL
          AND UPPER(COALESCE(a.status''')) NOT IN ('ACCEPTED'''ASSIGNED')
          AND TRIM(COALESCE(r.name''')) <> ''
        ORDER BY name ASC
        """'
        (start_min' start_max)'
    ).fetchall()
    conn.close()

    return [row["name"] for row in rows]


def has_any_offers_for_window(start_date: date' end_date_exclusive: date) -> bool:
    """
    True if at least one offer exists for any assignment whose game start_dt falls within the window.
    This is the cleanest signal that an OFFER has been sent (or at least created).
    """
    start_min = datetime.combine(start_date' datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(end_date_exclusive' datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    row = conn.execute(
        """
        SELECT 1
        FROM games g
        JOIN assignments a ON a.game_id = g.id
        JOIN offers o ON o.assignment_id = a.id
        WHERE g.start_dt >= ? AND g.start_dt < ?
        LIMIT 1
        """'
        (start_min' start_max)'
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
            r.id AS referee_id'
            TRIM(r.name) AS name'
            TRIM(r.email) AS email'
            TRIM(COALESCE(r.phone''')) AS phone'
            SUM(
                CASE
                    WHEN UPPER(COALESCE(a.status''')) IN ('ACCEPTED'''ASSIGNED')
                    THEN 1 ELSE 0
                END
            ) AS accepted_slots
        FROM referees r
        LEFT JOIN assignments a
            ON a.referee_id = r.id
        WHERE r.active = 1
        GROUP BY r.id' r.name' r.email' r.phone
        ORDER BY accepted_slots ASC' name ASC
        """
    ).fetchall()
    conn.close()

    df = pd.DataFrame(
        [
            {
                "Referee": (row["name"] or "").strip() or "—"'
                "Phone": (row["phone"] or "").strip() or "—"'
                "Email": (row["email"] or "").strip() or "—"'
                "Accepted": int(row["accepted_slots"] or 0)'
            }
            for row in rows
        ]
    )

    if df.empty:
        df = pd.DataFrame(columns=["Referee"' "Phone"' "Email"' "Accepted"])

    return df


# ============================================================
# Printable PDF helpers
# ============================================================
def get_admin_print_rows_for_date(selected_date: date):
    start_min = datetime.combine(selected_date' datetime.min.time()).isoformat(timespec="seconds")
    start_max = datetime.combine(selected_date + timedelta(days=1)' datetime.min.time()).isoformat(timespec="seconds")

    conn = db()
    rows = conn.execute(
        """
        SELECT
            g.id AS game_id'
            g.home_team'
            g.away_team'
            g.field_name'
            g.start_dt'
            a.slot_no'
            a.status'
            r.name AS ref_name
        FROM games g
        LEFT JOIN assignments a ON a.game_id = g.id
        LEFT JOIN referees r ON r.id = a.referee_id
        WHERE g.start_dt >= ? AND g.start_dt < ?
        ORDER BY g.start_dt ASC' g.field_name ASC' g.home_team ASC' a.slot_no ASC
        """'
        (start_min' start_max)'
    ).fetchall()
    conn.close()

    games_map = {}
    for row in rows:
        gid = int(row["game_id"])
        if gid not in games_map:
            games_map[gid] = {
                "home_team": row["home_team"]'
                "away_team": row["away_team"]'
                "field_name": row["field_name"]'
                "start_dt": row["start_dt"]'
                "slots": {
                    1: {"name": ""' "status": "EMPTY"}'
                    2: {"name": ""' "status": "EMPTY"}'
                }'
            }

        slot_no = row["slot_no"]
        if slot_no in (1' 2):
            nm = (row["ref_name"] or "").strip()
            stt = (row["status"] or "EMPTY").strip().upper()
            games_map[gid]["slots"][int(slot_no)] = {"name": nm' "status": stt}

    out = list(games_map.values())
    out.sort(key=lambda x: x["start_dt"])
    return out


def _format_ref_name(name: str' status: str) -> str:
    name = (name or "").strip()
    status = (status or "EMPTY").strip().upper()
    if not name:
        return "—"
    if status in ("ACCEPTED"' "ASSIGNED"):
        return name
    if status in ("OFFERED"' "DECLINED"):
        return f"{name} ({status})"
    return name


def build_admin_summary_pdf_bytes(selected_date: date) -> bytes:
    games = get_admin_print_rows_for_date(selected_date)

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer'
        pagesize=landscape(A4)'
        leftMargin=16'
        rightMargin=16'
        topMargin=16'
        bottomMargin=16'
        title=f"Game Summary {selected_date.isoformat()}"'
    )

    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"<b>Game Summary</b> — {selected_date.isoformat()}"' styles["Title"]))
    story.append(Spacer(1' 4))

    if not games:
        story.append(Paragraph("No games found for this date."' styles["Normal"]))
        doc.build(story)
        return buffer.getvalue()

    grouped = {}
    for g in games:
        dt = dtparser.parse(g["start_dt"])
        key = _time_12h(dt)
        grouped.setdefault(key' []).append((dt' g))

    group_keys = sorted(grouped.keys()' key=lambda k: min(dt for dt' _ in grouped[k]))

    for time_key in group_keys:
        story.append(Paragraph(f"<b>Start time: {time_key}</b>"' styles["Heading3"]))
        story.append(Spacer(1' 2))

        data = [["Teams"' "Field"' "Start"' "Referees"]]
        for dt' g in grouped[time_key]:
            teams = f"{g['home_team']} vs {g['away_team']}"
            field = g["field_name"]
            start_str = _time_12h(dt)

            r1 = _format_ref_name(g["slots"][1]["name"]' g["slots"][1]["status"])
            r2 = _format_ref_name(g["slots"][2]["name"]' g["slots"][2]["status"])
            refs = f"{r1} / {r2}"

            data.append([teams' field' start_str' refs])

        table = Table(
            data'
            colWidths=[360' 110' 75' 210]'
            repeatRows=1'
        )
        table.setStyle(
            TableStyle(
                [
                    ("FONTNAME"' (0' 0)' (-1' 0)' "Helvetica-Bold")'
                    ("FONTSIZE"' (0' 0)' (-1' 0)' 9)'
                    ("BACKGROUND"' (0' 0)' (-1' 0)' colors.lightgrey)'
                    ("TEXTCOLOR"' (0' 0)' (-1' 0)' colors.black)'
                    ("FONTNAME"' (0' 1)' (-1' -1)' "Helvetica")'
                    ("FONTSIZE"' (0' 1)' (-1' -1)' 8)'
                    ("GRID"' (0' 0)' (-1' -1)' 0.5' colors.grey)'
                    ("VALIGN"' (0' 0)' (-1' -1)' "MIDDLE")'
                    ("LEFTPADDING"' (0' 0)' (-1' -1)' 3)'
                    ("RIGHTPADDING"' (0' 0)' (-1' -1)' 3)'
                    ("TOPPADDING"' (0' 0)' (-1' -1)' 1)'
                    ("BOTTOMPADDING"' (0' 0)' (-1' -1)' 1)'
                ]
            )
        )

        story.append(table)
        story.append(Spacer(1' 4))

    doc.build(story)
    return buffer.getvalue()


def _refs_names_only_for_game(g: dict) -> str:
    r1 = (g["slots"][1]["name"] or "").strip() or "—"
    r2 = (g["slots"][2]["name"] or "").strip() or "—"
    return f"{r1} / {r2}"


def build_referee_scorecards_pdf_bytes(selected_date: date) -> bytes:
    games = get_admin_print_rows_for_date(selected_date)

    buf = BytesIO()
    c = canvas.Canvas(buf' pagesize=A4)
    page_w' page_h = A4

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

    def fit_bold_font_size(text: str' max_width: float' start_size: int' min_size: int) -> int:
        size = start_size
        while size > min_size:
            if c.stringWidth(text' "Helvetica-Bold"' size) <= max_width:
                return size
            size -= 1
        return min_size

    def fit_left_text_size(text: str' max_width: float' start_size: int' min_size: int) -> int:
        size = start_size
        while size > min_size:
            if c.stringWidth(text' "Helvetica-Bold"' size) <= max_width:
                return size
            size -= 1
        return min_size

    def draw_card(x0: float' y0: float' g: dict):
        c.setLineWidth(1)
        c.rect(x0' y0' card_w' card_h)

        left = x0 + pad
        right = x0 + card_w - pad
        y_top = y0 + card_h - pad
        max_text_w = right - left
        cx = x0 + card_w / 2.0

        c.setFont("Helvetica-Bold"' TITLE_SIZE)
        c.drawCentredString(cx' y_top' "REFEREE SCORECARD")

        c.setLineWidth(0.8)
        c.line(left' y_top - 8' right' y_top - 8)

        teams_line = f"{g['home_team']} vs {g['away_team']}"
        teams_size = fit_bold_font_size(teams_line' max_text_w' TEAMS_MAX_SIZE' TEAMS_MIN_SIZE)
        c.setFont("Helvetica-Bold"' teams_size)
        c.drawCentredString(cx' y_top - 26' teams_line)

        refs_line = _refs_names_only_for_game(g)
        c.setFont("Helvetica"' REFS_SIZE)
        c.drawCentredString(cx' y_top - 44' refs_line)

        dt = dtparser.parse(g["start_dt"])
        field_time = f"{g['field_name']} @ {_time_12h(dt)}"
        c.setFont("Helvetica"' FIELD_TIME_SIZE)
        c.drawCentredString(cx' y_top - 62' field_time)

        field_div_y = (y_top - 62) - 10
        c.setLineWidth(0.8)
        c.line(left' field_div_y' right' field_div_y)

        nums_left = left
        nums_right = right
        nums_span = nums_right - nums_left
        step = nums_span / 10.0

        wld_text = "W  /  L  /  D"
        wld_w = c.stringWidth(wld_text' "Helvetica-Bold"' FOOT_LABEL_SIZE)

        team1_name_y = field_div_y - 16
        team1_nums_y = team1_name_y - 20

        INTER_TEAM_GAP = int(18 * 2.0)

        team2_name_y = team1_nums_y - INTER_TEAM_GAP
        team2_nums_y = team2_name_y - 20

        def draw_team_name_with_wld(team_name: str' y: float):
            nm = str(team_name)
            max_name_w = max_text_w - (wld_w + 6)
            size = fit_left_text_size(nm' max_name_w' TEAM_ABOVE_NUM_MAX' TEAM_ABOVE_NUM_MIN)

            c.setFont("Helvetica-Bold"' size)
            c.drawString(left' y' nm)

            c.setFont("Helvetica-Bold"' FOOT_LABEL_SIZE)
            c.drawRightString(right' y' wld_text)

        def draw_nums_row(y: float):
            c.setFont("Helvetica-Bold"' TRIES_NUM_SIZE)
            for i in range(10):
                n = str(i + 1)
                x = nums_left + (step * (i + 0.5))
                c.drawCentredString(x' y' n)

        draw_team_name_with_wld(g["home_team"]' team1_name_y)
        draw_nums_row(team1_nums_y)

        draw_team_name_with_wld(g["away_team"]' team2_name_y)
        draw_nums_row(team2_nums_y)

        line_y = team2_nums_y - 14
        c.setLineWidth(0.8)
        c.line(left' line_y' right' line_y)

        line_x2 = right
        line_x1 = right - WRITE_LINE_W

        conduct_y = line_y - 18
        c.setFont("Helvetica-Bold"' FOOT_LABEL_SIZE)
        c.drawString(left' conduct_y' "Conduct (/10)")
        c.setLineWidth(WRITE_LINE_THICK)
        c.line(line_x1' conduct_y - 3' line_x2' conduct_y - 3)

        unstrip_y = conduct_y - 22
        c.setFont("Helvetica-Bold"' FOOT_LABEL_SIZE)
        c.drawString(left' unstrip_y' "Unstripped Players")
        c.setLineWidth(WRITE_LINE_THICK)
        c.line(line_x1' unstrip_y - 3' line_x2' unstrip_y - 3)

    for idx' g in enumerate(games):
        if idx > 0 and idx % 6 == 0:
            c.showPage()

        pos = idx % 6
        r = pos // 2
        col = pos % 2

        x0 = outer_margin + col * (card_w + gutter_x)
        y0 = page_h - outer_margin - (r + 1) * card_h - r * gutter_y

        draw_card(x0' y0' g)

    c.save()
    return buf.getvalue()


# ============================================================
# Printable XLSX helpers
# ============================================================
def build_admin_summary_xlsx_bytes(selected_date: date) -> bytes:
    games = get_admin_print_rows_for_date(selected_date)

    wb = Workbook()
    ws = wb.active
    ws.title = "Game Summary"

    # Styles
    title_font = Font(bold=True' size=16)
    header_font = Font(bold=True' size=11)
    bold_font = Font(bold=True)
    center = Alignment(horizontal="center"' vertical="center")
    left = Alignment(horizontal="left"' vertical="center"' wrap_text=True)

    header_fill = PatternFill("solid"' fgColor="D9D9D9")  # light grey
    group_fill = PatternFill("solid"' fgColor="F2F2F2")

    # Title row
    ws["A1"] = "Game Summary"
    ws["A1"].font = title_font
    ws["A2"] = selected_date.isoformat()
    ws["A2"].font = bold_font

    row = 4

    if not games:
        ws[f"A{row}"] = "No games found for this date."
        out = BytesIO()
        wb.save(out)
        return out.getvalue()

    # Group by start time (same as PDF)
    grouped: dict[str' list[tuple[datetime' dict]]] = {}
    for g in games:
        dt = dtparser.parse(g["start_dt"])
        key = _time_12h(dt)
        grouped.setdefault(key' []).append((dt' g))

    group_keys = sorted(grouped.keys()' key=lambda k: min(dt for dt' _ in grouped[k]))

    # Column headings
    cols = ["Teams"' "Field"' "Start"' "Referees"]

    for time_key in group_keys:
        # Group header
        ws[f"A{row}"] = f"Start time: {time_key}"
        ws[f"A{row}"].font = Font(bold=True' size=12)
        ws[f"A{row}"].fill = group_fill
        ws.merge_cells(start_row=row' start_column=1' end_row=row' end_column=len(cols))
        row += 1

        # Table header
        for c' name in enumerate(cols' start=1):
            cell = ws.cell(row=row' column=c' value=name)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center
        row += 1

        # Rows
        for dt' g in grouped[time_key]:
            teams = f"{g['home_team']} vs {g['away_team']}"
            field = g["field_name"]
            start_str = _time_12h(dt)

            r1 = _format_ref_name(g["slots"][1]["name"]' g["slots"][1]["status"])
            r2 = _format_ref_name(g["slots"][2]["name"]' g["slots"][2]["status"])
            refs = f"{r1} / {r2}"

            ws.cell(row=row' column=1' value=teams).alignment = left
            ws.cell(row=row' column=2' value=field).alignment = left
            ws.cell(row=row' column=3' value=start_str).alignment = center
            ws.cell(row=row' column=4' value=refs).alignment = left
            row += 1

        row += 1  # spacer line

    # Column widths
    widths = [48' 16' 12' 34]
    for i' w in enumerate(widths' start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    # Freeze panes at first header area (nice UX)
    ws.freeze_panes = "A4"

    out = BytesIO()
    wb.save(out)
    return out.getvalue()


# ============================================================
# Offers
# ============================================================
def create_offer(assignment_id: int) -> str:
    token = secrets.token_urlsafe(24)
    conn = db()
    conn.execute(
        """
        INSERT INTO offers(assignment_id' token' created_at)
        VALUES (?' ?' ?)
        """'
        (assignment_id' token' now_iso())'
    )
    conn.commit()
    conn.close()
    return token


def delete_offer_by_token(token: str):
    conn = db()
    conn.execute("DELETE FROM offers WHERE token=?"' (token'))
    conn.commit()
    conn.close()


def resolve_offer(token: str' response: str) -> tuple[bool' str]:
    response = (response or "").strip().upper()
    if response not in ("ACCEPTED"' "DECLINED"):
        return False' "Invalid response."

    conn = db()
    offer = conn.execute(
        """
        SELECT id' assignment_id
        FROM offers
        WHERE token=?
        """'
        (token')'
    ).fetchone()

    if not offer:
        conn.close()
        return False' "Invalid or unknown offer link."

    conn.execute(
        """
        UPDATE offers
        SET responded_at=?' response=?
        WHERE id=?
        """'
        (now_iso()' response' offer["id"])'
    )

    new_status = "ACCEPTED" if response == "ACCEPTED" else "DECLINED"
    conn.execute(
        """
        UPDATE assignments
        SET status=?' updated_at=?
        WHERE id=?
        """'
        (new_status' now_iso()' offer["assignment_id"])'
    )

    conn.commit()
    conn.close()
    return True' f"Thanks — you have {response.lower()} the offer."


def send_offer_email_and_mark_offered(
    *'
    assignment_id: int'
    referee_name: str'
    referee_email: str'
    game'
    start_dt'
    msg_key: str'
):
    token = create_offer(assignment_id)

    try:
        cfg = smtp_settings()
        base = cfg.get("app_base_url"' "").rstrip("/")
        if not base:
            raise RuntimeError("APP_BASE_URL is missing. Add it in Render environment variables.")

        game_line = f"{game['home_team']} vs {game['away_team']}"
        when_line = start_dt.strftime("%Y-%m-%d %I:%M %p").lstrip("0")
        subject = f"{referee_name} — Match assignment: {game_line}"

        portal_url = f"{base}/?offer_token={token}"

        text = (
            f"Hi {referee_name}'\n\n"
            f"You have a match assignment offer:\n"
            f"- Game: {game_line}\n"
            f"- Field: {game['field_name']}\n"
            f"- Start: {when_line}\n\n"
            f"View and respond here:\n{portal_url}\n"
        )

        html = f"""
        <div style="font-family: Arial' sans-serif; line-height:1.4;">
          <p>Hi {referee_name}'</p>
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
            If the button doesn’t work' copy and paste this link:<br>{portal_url}
          </p>
        </div>
        """

        send_html_email(referee_email' referee_name' subject' html' text_body=text)

        set_assignment_status(assignment_id' "OFFERED")
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
            o.id AS offer_id'
            o.token'
            o.created_at'
            o.responded_at'
            o.response'
            a.id AS assignment_id'
            a.slot_no'
            a.referee_id'
            a.status'
            r.name AS ref_name'
            r.email AS ref_email'
            g.home_team'
            g.away_team'
            g.field_name'
            g.start_dt
        FROM offers o
        JOIN assignments a ON a.id = o.assignment_id
        JOIN games g ON g.id = a.game_id
        LEFT JOIN referees r ON r.id = a.referee_id
        WHERE o.token=?
        LIMIT 1
        """'
        (token')'
    ).fetchone()
    conn.close()
    return row


def list_offers_for_referee(referee_id: int):
    conn = db()
    rows = conn.execute(
        """
        SELECT
            o.id AS offer_id'
            o.token'
            o.created_at'
            o.responded_at'
            o.response'
            a.id AS assignment_id'
            a.slot_no'
            a.status'
            g.home_team'
            g.away_team'
            g.field_name'
            g.start_dt
        FROM offers o
        JOIN assignments a ON a.id = o.assignment_id
        JOIN games g ON g.id = a.game_id
        WHERE a.referee_id=?
        ORDER BY o.created_at DESC
        """'
        (referee_id')'
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

    st.query_params.pop("offer_token"' None)
    st.rerun()


def referee_logout_button():
    if st.session_state.get("referee_id"):
        c1' c2 = st.columns([3' 1])
        with c1:
            st.caption(
                f"Logged in as: {st.session_state.get('referee_name')} "
                f"({st.session_state.get('referee_email')})"
            )
        with c2:
            if st.button("Log out"' key="ref_logout_btn"):
                st.session_state.pop("referee_id"' None)
                st.session_state.pop("referee_name"' None)
                st.session_state.pop("referee_email"' None)
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
                        "<div style='font-weight:700;color:#c62828;'>Response recorded: DECLINED</div>"'
                        unsafe_allow_html=True'
                    )
                elif current_resp == "ACCEPTED":
                    st.markdown(
                        "<div style='font-weight:700;color:#2e7d32;'>Response recorded: ACCEPTED</div>"'
                        unsafe_allow_html=True'
                    )
                else:
                    st.info(f"Response recorded: {current_resp}")

                st.caption("You can change your response below if needed.")

            c1' c2 = st.columns(2)

            if c1.button("Accept"' key=f"portal_acc_{o['token']}"):
                ok' msg = resolve_offer(o["token"]' "ACCEPTED")
                if ok:
                    st.success("Accepted. Thank you.")
                else:
                    st.error(msg)
                st.rerun()

            if c2.button("Decline"' key=f"portal_dec_{o['token']}"):
                ok' msg = resolve_offer(o["token"]' "DECLINED")
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
    if token and action in ("accept"' "decline"):
        response = "ACCEPTED" if action == "accept" else "DECLINED"
        ok' msg = resolve_offer(token' response)
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
    first_email = st.text_input("Your admin email"' key="first_admin_email")
    if st.button("Create first admin"' key="create_first_admin_btn"):
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
    email = st.text_input("Admin email"' key="login_email")

    if st.button("Send login link"' key="send_login_link_btn"):
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

    # ------------------------------------------------------------
    # Emergency / DEV: show admin login link on screen (no email)
    # Enable by setting environment variable:
    #   SHOW_ADMIN_LINK = "true"
    # ------------------------------------------------------------
    if os.getenv("SHOW_ADMIN_LINK"' "false").lower() == "true":
        st.markdown("---")
        st.subheader("Emergency Admin Link (no email)")

        if st.button("Generate admin login link (display here)"' key="show_admin_link_btn"):
            if not email.strip():
                st.error("Please enter an email above first.")
            elif not is_admin_email_allowed(email):
                st.error("That email is not an authorised administrator.")
            else:
                cfg = smtp_settings()
                base = (cfg.get("app_base_url") or "").rstrip("/")
                if not base:
                    st.error("APP_BASE_URL is missing. Set it in Render environment variables.")
                else:
                    token = create_admin_login_token(email.strip().lower()' minutes_valid=15)
                    login_url = f"{base}/?admin_login=1&token={token}"
                    st.success("Login link generated (valid 15 minutes):")
                    st.code(login_url)
                    st.caption("Open that link in a new tab to log in.")


    # ------------------------------------------------------------
    # DEV Admin URL (no email) — enabled via environment variable
    # ------------------------------------------------------------
    if os.getenv("DEV_ADMIN_URL_ENABLED"' "false").lower() == "true":
        st.markdown("---")
        st.subheader("DEV Admin Login (no email)")

        if st.button("Generate DEV admin login URL"' key="dev_admin_url_btn"):
            dev_email = os.getenv("DEV_ADMIN_EMAIL"' "").strip().lower()

            if not dev_email:
                st.error("DEV_ADMIN_EMAIL is not set in environment variables.")
            elif not is_admin_email_allowed(dev_email):
                st.error(f"{dev_email} is not an active admin.")
            else:
                cfg = smtp_settings()
                base = (cfg.get("app_base_url") or "").rstrip("/")

                if not base:
                    st.error("APP_BASE_URL is missing.")
                else:
                    # 90-day expiry (adjust if needed)
                    expires_at = (
                        datetime.now(timezone.utc) + timedelta(days=90)
                    ).isoformat(timespec="seconds")

                    token = create_admin_session_with_expires_at(dev_email' expires_at)
                    url = f"{base}/?session={token}"

                    st.success("DEV admin login URL created:")
                    st.code(url)
                    st.caption(
                        "Bookmark this URL. Disable DEV_ADMIN_URL_ENABLED when finished."
                    )


    st.stop()

# Logged in view
admin_logout_button()

tabs = st.tabs(["Admin"' "Ladder"' "Import"' "Blackouts"' "Administrators"])

# ============================================================
# Admin tab
# ============================================================
with tabs[0]:
    st.subheader("Games & Assignments")

    auto = st.toggle("Auto-refresh every 5 seconds"' value=True' key="auto_refresh_toggle")
    if auto:
        st_autorefresh(interval=5000' key="auto_refresh_tick")

    if st.button("Refresh status"' key="refresh_status_btn"):
        st.rerun()

    games = get_games()
    refs = get_referees()

    if not games:
        st.info("Import a Games CSV first (Import tab).")
        st.stop()

    all_dates = sorted({game_local_date(g) for g in games})
    today = date.today()
    default_idx = 0
    for i' d in enumerate(all_dates):
        if d >= today:
            default_idx = i
            break

    selected_date = st.selectbox(
        "Show games for date"'
        all_dates'
        index=default_idx'
        key="games_date_select"'
    )

    preserve_scroll(f"refalloc_admin_games_tab_{selected_date.isoformat()}")

    count_games = sum(1 for g in games if game_local_date(g) == selected_date)
    st.caption(f"{count_games} game(s) on {selected_date.isoformat()}")

    week_start' week_end_excl = iso_week_window(selected_date)

    main_col' side_col = st.columns([3' 1]' gap="large")

    with side_col:
        st.markdown("### Referee workload")
        st.caption("All-time accepted/assigned (all games)")
        df_work = get_referee_workload_all_time()

        if df_work.empty:
            st.info("No referees found.")
        else:
            st.dataframe(df_work' use_container_width=True' hide_index=True)
            total_acc = int(df_work["Accepted"].sum()) if "Accepted" in df_work.columns else 0
            st.caption(f"Total accepted/assigned slots (all-time): {total_acc}")

    with main_col:
        accepted_slots' total_slots = get_acceptance_progress_for_window(week_start' week_end_excl)

        pct = (accepted_slots / total_slots) if total_slots else 0.0
        pct_clamped = max(0.0' min(1.0' pct))

        if total_slots == 0:
            bar_color = "#9e9e9e"
        elif pct_clamped < 0.50:
            bar_color = "#c62828"
        elif pct_clamped < 0.90:
            bar_color = "#ffb300"
        else:
            bar_color = "#2e7d32"

        not_accepted_names = list_referees_not_accepted_for_window(week_start' week_end_excl)

        c_bar' c_list = st.columns([1' 2]' vertical_alignment="center")

        with c_bar:
            height_px = 24
            st.markdown(
                f"""
                <div style="font-size:12px; color:#666; margin-bottom:6px;">
                  <b>Week acceptance (ISO)</b> — {accepted_slots}/{total_slots}
                </div>

                <div style="width:100%; background:#e0e0e0; border-radius:{height_px}px; height:{height_px}px; overflow:hidden;">
                  <div style="width:{pct_clamped*100:.1f}%; background:{bar_color}; height:{height_px}px;"></div>
                </div>
                """'
                unsafe_allow_html=True'
            )

        with c_list:
            has_offers = has_any_offers_for_window(week_start' week_end_excl)

            if not has_offers:
                st.caption("")
            else:
                st.markdown(
                    "<div style='font-size:12px; color:#666; margin-bottom:6px;'><b>Yet to ACCEPT (unique)</b></div>"'
                    unsafe_allow_html=True'
                )

                if not not_accepted_names:
                    st.markdown(
                        "<div style='font-size:12px; color:#2e7d32;'>All accepted ✅</div>"'
                        unsafe_allow_html=True'
                    )
                else:
                    items_html = "' ".join([f"<span>{n}</span>" for n in not_accepted_names])
                    st.markdown(
                        f"""
                        <div style="
                            font-size:12px;
                            color:#ffb300;
                            line-height:1.6;
                            display:flex;
                            flex-wrap:wrap;
                            gap:6px;
                            align-items:center;
                        ">
                          {items_html}
                        </div>
                        """'
                        unsafe_allow_html=True'
                    )

        st.markdown("---")
        st.subheader("Printable Summary")

        c_pdf1' c_pdf2' c_x1' c_x2' c_pdf3' c_pdf4 = st.columns([1' 2' 1' 2' 1' 2])

        with c_pdf1:
            if st.button("Build Summary PDF"' key="build_pdf_btn"):
                try:
                    pdf_bytes = build_admin_summary_pdf_bytes(selected_date)
                    st.session_state["admin_summary_pdf_bytes"] = pdf_bytes
                    st.success("Summary PDF built.")
                except Exception as e:
                    st.error(f"Failed to build Summary PDF: {e}")

        with c_pdf2:
            pdf_bytes = st.session_state.get("admin_summary_pdf_bytes")
            if pdf_bytes:
                filename = f"game_summary_{selected_date.isoformat()}.pdf"
                st.download_button(
                    label="Download Summary PDF"'
                    data=pdf_bytes'
                    file_name=filename'
                    mime="application/pdf"'
                    key="download_pdf_btn"'
                )
            else:
                st.caption("Click **Build Summary PDF** to generate the printable schedule.")

        with c_x1:
            if st.button("Build Summary XLSX"' key="build_xlsx_btn"):
                try:
                    xlsx_bytes = build_admin_summary_xlsx_bytes(selected_date)
                    st.session_state["admin_summary_xlsx_bytes"] = xlsx_bytes
                    st.success("Summary XLSX built.")
                except Exception as e:
                    st.error(f"Failed to build Summary XLSX: {e}")

        with c_x2:
            xlsx_bytes = st.session_state.get("admin_summary_xlsx_bytes")
            if xlsx_bytes:
                filename = f"game_summary_{selected_date.isoformat()}.xlsx"
                st.download_button(
                    label="Download Summary XLSX"'
                    data=xlsx_bytes'
                    file_name=filename'
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"'
                    key="download_xlsx_btn"'
                )
            else:
                st.caption("Click **Build Summary XLSX** to generate the Excel schedule.")


        with c_pdf3:
            if st.button("Build Referee Scorecards"' key="build_scorecards_btn"):
                try:
                    pdf_bytes = build_referee_scorecards_pdf_bytes(selected_date)
                    st.session_state["ref_scorecards_pdf_bytes"] = pdf_bytes
                    st.success("Scorecards PDF built.")
                except Exception as e:
                    st.error(f"Failed to build Scorecards PDF: {e}")

        with c_pdf4:
            pdf_bytes = st.session_state.get("ref_scorecards_pdf_bytes")
            if pdf_bytes:
                filename = f"referee_scorecards_{selected_date.isoformat()}.pdf"
                st.download_button(
                    label="Download Scorecards PDF"'
                    data=pdf_bytes'
                    file_name=filename'
                    mime="application/pdf"'
                    key="download_scorecards_btn"'
                )
            else:
                st.caption("Click **Build Referee Scorecards** to generate the 6-up scorecards.")

        for g in games:
            if game_local_date(g) != selected_date:
                continue

            start_dt = dtparser.parse(g["start_dt"])
            gdate = game_local_date(g)

            ref_options = ["— Select referee —"]
            ref_lookup = {}
            for r in refs:
                label = f"{r['name']} ({r['email']})"
                if referee_has_blackout(r["id"]' gdate):
                    label = f"🚫 {label} — blackout"
                ref_options.append(label)
                ref_lookup[label] = r["id"]

            with st.container(border=True):
                st.markdown(
                    f"**{g['home_team']} vs {g['away_team']}**  \n"
                    f"Field: **{g['field_name']}**  \n"
                    f"Start: **{start_dt.strftime('%Y-%m-%d %I:%M %p').lstrip('0')}**"
                )

                assigns = get_assignments_for_game(g["id"])
                cols = st.columns(2)

                for col_idx' a in enumerate(assigns):
                    with cols[col_idx]:
                        st.markdown(f"#### Slot {a['slot_no']}")

                        status = (a["status"] or "").strip().upper()
                        st.caption(f"assignment_id={a['id']} | status={status} | updated_at={a['updated_at']}")

                        current_ref_label = None
                        if a["referee_id"] is not None and a["ref_name"] and a["ref_email"]:
                            current_ref_label = f"{a['ref_name']} ({a['ref_email']})"
                            if referee_has_blackout(a["referee_id"]' gdate):
                                current_ref_label = f"🚫 {current_ref_label} — blackout"

                        default_index = 0
                        if current_ref_label and current_ref_label in ref_options:
                            default_index = ref_options.index(current_ref_label)

                        refpick_key = f"refpick_{g['id']}_{a['slot_no']}"
                        pick = st.selectbox(
                            "Referee"'
                            ref_options'
                            index=default_index'
                            key=refpick_key'
                            disabled=(status in ("ACCEPTED"' "ASSIGNED"))'
                        )

                        if pick != "— Select referee —":
                            chosen_ref_id = ref_lookup[pick]
                            if status in ("ACCEPTED"' "ASSIGNED"):
                                st.info("This slot is locked (ACCEPTED/ASSIGNED). Use Action → RESET to change it.")
                            else:
                                if a["referee_id"] != chosen_ref_id:
                                    set_assignment_ref(a["id"]' chosen_ref_id)
                                    st.rerun()
                        else:
                            if a["referee_id"] is not None:
                                clear_assignment(a["id"])
                                st.session_state[refpick_key] = "— Select referee —"
                                st.rerun()

                        blackout = False
                        if a["referee_id"] is not None:
                            blackout = referee_has_blackout(a["referee_id"]' gdate)

                        if status == "ACCEPTED":
                            status_badge(f"✅ {a['ref_name']} — ACCEPTED"' bg="#2e7d32")
                        elif status == "ASSIGNED":
                            status_badge(f"✅ {a['ref_name']} — ASSIGNED"' bg="#2e7d32")
                        elif status == "DECLINED":
                            status_badge(f"❌ {a['ref_name']} — DECLINED"' bg="#c62828")
                        elif status == "OFFERED":
                            status_badge(f"⬜ {a['ref_name']} — OFFERED"' bg="#546e7a")
                        elif a["referee_id"] is not None:
                            status_badge(f"⬛ {a['ref_name']} — NOT OFFERED YET"' bg="#424242")
                        else:
                            st.caption("EMPTY")

                        if blackout:
                            st.warning(f"Blackout date conflict: {gdate.isoformat()}")

                        action_key = f"action_{a['id']}"
                        msg_key = f"msg_{a['id']}"
                        st.session_state.setdefault(action_key' "—")

                        action_options = ["—"' "OFFER"' "ASSIGN"' "DELETE"' "RESET"]
                        if status in ("ACCEPTED"' "ASSIGNED"):
                            action_options = ["—"' "RESET"' "DELETE"]

                        def on_action_change(
                            assignment_id=a["id"]'
                            game_row=g'
                            start_dt=start_dt'
                            gdate=gdate'
                            action_key=action_key'
                            msg_key=msg_key'
                            refpick_key=refpick_key'
                        ):
                            choice = st.session_state.get(action_key' "—")
                            st.session_state.pop(msg_key' None)

                            if choice == "—":
                                return

                            live_a = get_assignment_live(assignment_id)
                            if not live_a:
                                st.session_state[msg_key] = "Could not load assignment."
                                st.session_state[action_key] = "—"
                                st.rerun()
                                return

                            live_ref_id = live_a["referee_id"]
                            live_ref_name = live_a["ref_name"]
                            live_ref_email = live_a["ref_email"]
                            live_status = (live_a["status"] or "").strip().upper()

                            live_blackout = False
                            if live_ref_id is not None:
                                live_blackout = referee_has_blackout(int(live_ref_id)' gdate)

                            if live_ref_id is None and choice in ("OFFER"' "ASSIGN"):
                                st.session_state[msg_key] = "Select a referee first."
                                st.session_state[action_key] = "—"
                                return

                            if choice == "OFFER" and live_status in ("ACCEPTED"' "ASSIGNED"):
                                st.session_state[msg_key] = "This slot is already confirmed (ACCEPTED/ASSIGNED)."
                                st.session_state[action_key] = "—"
                                return

                            if choice == "OFFER":
                                if live_blackout:
                                    st.session_state[msg_key] = (
                                        "Offer blocked: referee is unavailable on this date (blackout). "
                                        "You can still ASSIGN manually if needed."
                                    )
                                    st.session_state[action_key] = "—"
                                    return

                                send_offer_email_and_mark_offered(
                                    assignment_id=assignment_id'
                                    referee_name=live_ref_name'
                                    referee_email=live_ref_email'
                                    game=game_row'
                                    start_dt=start_dt'
                                    msg_key=msg_key'
                                )

                            elif choice == "ASSIGN":
                                set_assignment_status(assignment_id' "ASSIGNED")
                                st.session_state[msg_key] = "Assigned."

                            elif choice in ("DELETE"' "RESET"):
                                clear_assignment(assignment_id)
                                st.session_state[refpick_key] = "— Select referee —"
                                st.session_state[msg_key] = "Slot cleared (EMPTY)."

                            st.session_state[action_key] = "—"
                            st.rerun()

                        st.selectbox(
                            "Action"'
                            action_options'
                            key=action_key'
                            on_change=on_action_change'
                        )

                        if st.session_state.get(msg_key):
                            st.info(st.session_state[msg_key])

# ============================================================
# Ladder tab
# ============================================================
with tabs[1]:
    st.subheader("Competition Ladder (Admin)")
    st.caption("Enter team divisions + opening balance + game results' then view ladder + audit breakdown for fault finding.")

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
    for i' d in enumerate(all_dates):
        if d >= today:
            default_idx = i
            break

    selected_date = st.selectbox(
        "Ladder date"'
        all_dates'
        index=default_idx'
        key="ladder_date_select"'
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
        r["name"]: {
            "division": (r["division"] or "").strip()'
            "opening": int(r["opening_balance"] or 0)'
        }
        for r in teams_rows
    }

    if not teams_today:
        st.info("No teams found for this date.")
        st.stop()

    div_col1' div_col2 = st.columns([2' 1]' gap="large")

    with div_col1:
        st.write("Set division + opening balance per team (saved immediately).")

        for t in teams_today:
            cur_div = (existing.get(t' {}).get("division") or "").strip()
            cur_open = int(existing.get(t' {}).get("opening") or 0)

            default_idx = DIVISIONS.index(cur_div) if cur_div in DIVISIONS else 0

            r1' r2 = st.columns([2' 1]' gap="medium")
            with r1:
                new_div = st.selectbox(
                    label=t'
                    options=DIVISIONS'
                    index=default_idx'
                    key=f"div_select_{selected_date.isoformat()}_{t}"'
                )
            with r2:
                new_open = st.number_input(
                    label="Opening"'
                    min_value=0'
                    step=1'
                    value=cur_open'
                    key=f"open_{selected_date.isoformat()}_{t}"'
                )

            if new_div != cur_div or int(new_open) != cur_open:
                upsert_team(t' new_div' int(new_open))
                existing[t] = {"division": new_div' "opening": int(new_open)}

    with div_col2:
        st.write("Teams (today)")

        df_teams_today = pd.DataFrame(
            [
                {
                    "Team": t'
                    "Division": (existing.get(t' {}).get("division") or "").strip() or "—"'
                    "Opening": int(existing.get(t' {}).get("opening") or 0)'
                }
                for t in teams_today
            ]
        )

        if df_teams_today.empty:
            st.caption("No teams found.")
        else:
            # Sort: Division (asc)' Opening (desc)' Team (asc)
            df_teams_today = df_teams_today.sort_values(
                by=["Division"' "Opening"' "Team"]'
                ascending=[True' False' True]'
            ).reset_index(drop=True)

            # Fit height so there's no internal scroll bar
            # ~35px per row + header padding is a good Streamlit rule of thumb
            row_h = 35
            height = min(900' (len(df_teams_today) + 1) * row_h + 10)

            st.dataframe(
                df_teams_today'
                use_container_width=True'
                hide_index=True'
                height=height'
            )

    st.markdown("---")
    st.markdown("### 2) Enter game results (scores + referee inputs)")

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

        with st.container(border=True):
            st.markdown(f"**{title}**")

            c1' c2' c3 = st.columns([1' 1' 1]' gap="large")

            with c1:
                st.markdown("**Score**")
                home_score = st.number_input(
                    f"{g['home_team']} score"'
                    min_value=0'
                    step=1'
                    value=d_home_score'
                    key=f"hs_{g['id']}"'
                )
                away_score = st.number_input(
                    f"{g['away_team']} score"'
                    min_value=0'
                    step=1'
                    value=d_away_score'
                    key=f"as_{g['id']}"'
                )

            with c2:
                st.markdown("**Female tries**")
                home_ft = st.number_input(
                    f"{g['home_team']} female tries"'
                    min_value=0'
                    step=1'
                    value=d_hft'
                    key=f"hft_{g['id']}"'
                )
                away_ft = st.number_input(
                    f"{g['away_team']} female tries"'
                    min_value=0'
                    step=1'
                    value=d_aft'
                    key=f"aft_{g['id']}"'
                )

            with c3:
                st.markdown("**Conduct / Unstripped**")
                home_conduct = st.number_input(
                    f"{g['home_team']} conduct (/10)"'
                    min_value=0'
                    max_value=10'
                    step=1'
                    value=d_hc'
                    key=f"hc_{g['id']}"'
                )
                away_conduct = st.number_input(
                    f"{g['away_team']} conduct (/10)"'
                    min_value=0'
                    max_value=10'
                    step=1'
                    value=d_ac'
                    key=f"ac_{g['id']}"'
                )

                home_un = st.number_input(
                    f"{g['home_team']} unstripped"'
                    min_value=0'
                    step=1'
                    value=d_hu'
                    key=f"hu_{g['id']}"'
                )
                away_un = st.number_input(
                    f"{g['away_team']} unstripped"'
                    min_value=0'
                    step=1'
                    value=d_au'
                    key=f"au_{g['id']}"'
                )

            if st.button("Save result"' key=f"save_res_{g['id']}"):
                upsert_game_result(
                    game_id=int(g["id"])'
                    home_score=int(home_score)'
                    away_score=int(away_score)'
                    home_female_tries=int(home_ft)'
                    away_female_tries=int(away_ft)'
                    home_conduct=int(home_conduct)'
                    away_conduct=int(away_conduct)'
                    home_unstripped=int(home_un)'
                    away_unstripped=int(away_un)'
                )
                st.success("Saved.")
                st.rerun()

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

    div_choice = st.selectbox("Division (view)"' divisions' key="ladder_div_select")

    df_ladder = ladder_table_df_for_date(selected_date' div_choice)
    if df_ladder.empty:
        st.info("No ladder rows for this division/date.")
    else:
        st.markdown("#### Ladder")
        st.dataframe(df_ladder' use_container_width=True' hide_index=True)

        st.download_button(
            "Download ladder CSV"'
            data=df_ladder.to_csv(index=False).encode("utf-8")'
            file_name=f"ladder_{div_choice}_{selected_date.isoformat()}.csv".replace(" "' "_")'
            mime="text/csv"'
            key="ladder_csv_btn"'
        )

    st.markdown("#### Audit table (per team per game)")
    df_audit_show = df_audit[df_audit["Division"].fillna("—") == (div_choice or "—")]
    st.dataframe(df_audit_show' use_container_width=True' hide_index=True)

    st.download_button(
        "Download audit CSV"'
        data=df_audit_show.to_csv(index=False).encode("utf-8")'
        file_name=f"audit_{div_choice}_{selected_date.isoformat()}.csv".replace(" "' "_")'
        mime="text/csv"'
        key="audit_csv_btn"'
    )


# ============================================================
# Import tab
# ============================================================
with tabs[2]:
    st.subheader("Import CSVs")

    st.markdown("### Games CSV")
    st.caption("Required columns: game_id' date' start_time' home_team' away_team' field")

    games_file = st.file_uploader("Upload Games CSV"' type=["csv"]' key="games_csv")
    if games_file:
        df_games = pd.read_csv(games_file)
        st.dataframe(df_games.head(20)' use_container_width=True)
        if st.button("Import Games"' key="import_games_btn"):
            ins' upd = import_games_csv(df_games)
            st.success(f"Imported games. Inserted: {ins}' Updated: {upd}")
            st.rerun()

    st.markdown("---")

    st.markdown("### Referees CSV")
    st.caption("Required columns: name' email  (optional: phone)")

    replace_mode = st.checkbox(
        "Replace ALL referees with this CSV (overwrite existing list)"'
        value=False'
        key="replace_refs_mode"'
    )

    refs_file = st.file_uploader("Upload Referees CSV"' type=["csv"]' key="refs_csv")
    if refs_file:
        df_refs = pd.read_csv(refs_file)
        st.dataframe(df_refs.head(20)' use_container_width=True)

        if st.button("Import Referees"' key="import_refs_btn"):
            try:
                if replace_mode:
                    count = replace_referees_csv(df_refs)
                    st.success(
                        f"Replaced referee list successfully. Imported {count} referee(s). "
                        "All assignments were reset to EMPTY."
                    )
                    for k in [k for k in st.session_state.keys() if str(k).startswith("refpick_")]:
                        st.session_state.pop(k' None)
                else:
                    added' updated = import_referees_csv(df_refs)
                    st.success(f"Imported referees. Added: {added}' Updated: {updated}")

                st.rerun()
            except Exception as e:
                st.error(str(e))

    st.markdown("---")

    st.markdown("### Blackouts CSV (optional)")
    st.caption("Required columns: email' blackout_date")

    bl_file = st.file_uploader("Upload Blackouts CSV"' type=["csv"]' key="bl_csv")
    if bl_file:
        df_bl = pd.read_csv(bl_file)
        st.dataframe(df_bl.head(20)' use_container_width=True)
        if st.button("Import Blackouts"' key="import_bl_btn"):
            added' skipped = import_blackouts_csv(df_bl)
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
        choice = st.selectbox("Select referee"' list(ref_map.keys())' key="blackout_ref_select")
        ref_id = ref_map[choice]

        add_date = st.date_input("Add blackout date"' value=date.today()' key="blackout_add_date")
        if st.button("Add date"' key="blackout_add_btn"):
            conn = db()
            try:
                conn.execute(
                    "INSERT INTO blackouts(referee_id' blackout_date) VALUES (?' ?)"'
                    (ref_id' add_date.isoformat())'
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
            """'
            (ref_id')'
        ).fetchall()
        conn.close()

        if rows:
            dates = [r["blackout_date"] for r in rows]
            del_date = st.selectbox("Remove blackout date"' dates' key="blackout_del_select")
            if st.button("Remove selected date"' key="blackout_del_btn"):
                conn = db()
                conn.execute(
                    "DELETE FROM blackouts WHERE referee_id=? AND blackout_date=?"'
                    (ref_id' del_date)'
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
                    "email": a["email"]'
                    "active": "YES" if a["active"] == 1 else "NO"'
                    "created_at": a["created_at"]'
                }
                for a in admins
            ]
        )
        st.dataframe(df_admins' use_container_width=True)

    st.markdown("### Add admin")
    new_admin = st.text_input("Email to add"' key="add_admin_email")
    if st.button("Add admin"' key="add_admin_btn"):
        if not new_admin.strip():
            st.error("Enter an email.")
        else:
            add_admin(new_admin)
            st.success("Admin added (or already existed).")
            st.rerun()

    st.markdown("### Remove/disable admin")
    active_emails = [a["email"] for a in admins if a["active"] == 1]
    if active_emails:
        disable_email = st.selectbox("Select admin to disable"' active_emails' key="disable_admin_select")
        if st.button("Disable selected admin"' key="disable_admin_btn"):
            if disable_email == st.session_state.get("admin_email"):
                st.error("You can't disable yourself while logged in.")
            else:
                set_admin_active(disable_email' False)
                st.success("Admin disabled.")
                st.rerun()
    else:
        st.info("No active admins found (you should add at least one).")
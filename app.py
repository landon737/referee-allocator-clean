import os
import sqlite3
import secrets
import smtplib
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pandas as pd
import streamlit as st
from dateutil import parser as dtparser
from streamlit_autorefresh import st_autorefresh

# ============================================================
# CONFIG
# ============================================================
BASE_DIR = Path(__file__).resolve().parent

# Production: set DB_PATH in Render Environment to your persistent disk path.
# Local: falls back to ./league.db next to app.py
DB_PATH = os.getenv("DB_PATH", str(BASE_DIR / "league.db"))
Path(DB_PATH).expanduser().parent.mkdir(parents=True, exist_ok=True)

LEAGUE_TZ = "Pacific/Auckland"

# Feature flag for referee portal
REF_PORTAL_ENABLED = os.getenv("REF_PORTAL_ENABLED", "false").lower() == "true"

st.set_page_config(page_title="Referee Allocator (MVP)", layout="wide")


# ============================================================
# UI helpers
# ============================================================
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
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def init_db():
    conn = db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS referees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            active INTEGER NOT NULL DEFAULT 1
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
            status TEXT NOT NULL DEFAULT 'EMPTY', -- EMPTY, OFFERED, ACCEPTED, DECLINED, ASSIGNED
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
            response TEXT, -- ACCEPTED or DECLINED
            FOREIGN KEY(assignment_id) REFERENCES assignments(id)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS blackouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referee_id INTEGER NOT NULL,
            blackout_date TEXT NOT NULL, -- YYYY-MM-DD
            UNIQUE(referee_id, blackout_date),
            FOREIGN KEY(referee_id) REFERENCES referees(id)
        );
        """
    )

    # Admin allowlist + magic links
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
    conn.close()


# ============================================================
# Email (SMTP)
# ============================================================
def smtp_settings():
    # Env vars (Render) take priority; secrets.toml (local) is fallback.
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
        "from_email": get("SMTP_FROM_EMAIL", get("SMTP_USER", "")),
        "from_name": get("SMTP_FROM_NAME", "Referee Allocator"),
        "app_base_url": get("APP_BASE_URL", "").rstrip("/"),  # https://xxxx.onrender.com
    }


def send_html_email(to_email: str, to_name: str, subject: str, html_body: str, text_body: str | None = None):
    """
    Multipart/alternative: always include text/plain + text/html (helps Gmail deliverability).
    """
    cfg = smtp_settings()
    if not (cfg["host"] and cfg["user"] and cfg["password"] and cfg["from_email"] and cfg["app_base_url"]):
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
# Admin auth helpers
# ============================================================
def is_admin_email_allowed(email: str) -> bool:
    conn = db()
    row = conn.execute(
        "SELECT 1 FROM admins WHERE email=? AND active=1 LIMIT 1",
        (email.strip().lower(),),
    ).fetchone()
    conn.close()
    return bool(row)


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
    rows = conn.execute("SELECT email, active, created_at FROM admins ORDER BY email ASC").fetchall()
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
            st.query_params.clear()


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
      <p>Use the button below to sign in as an administrator. This link expires in <b>15 minutes</b>.</p>
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
            st.query_params.clear()
            st.query_params["session"] = session_token
            st.rerun()
        else:
            st.title("Admin Login")
            st.error(value)
            st.info("Please go back and request a new login link.")
            st.stop()


def admin_logout_button():
    if st.session_state.get("admin_email"):
        col1, col2 = st.columns([3, 1])
        with col1:
            st.caption(f"Logged in as: {st.session_state['admin_email']}")
        with col2:
            if st.button("Log out"):
                st.session_state.pop("admin_email", None)
                st.query_params.clear()
                st.rerun()


# ============================================================
# CSV Import helpers
# ============================================================
def import_referees_csv(df: pd.DataFrame):
    cols = {c.lower().strip(): c for c in df.columns}
    if "name" not in cols or "email" not in cols:
        raise ValueError("Referees CSV must contain columns: name, email")

    conn = db()
    cur = conn.cursor()
    added = 0
    updated = 0

    for _, row in df.iterrows():
        name = str(row[cols["name"]]).strip()
        email = str(row[cols["email"]]).strip().lower()
        if not name or not email or email == "nan":
            continue

        cur.execute("SELECT id, name FROM referees WHERE email=?", (email,))
        existing = cur.fetchone()
        if existing:
            if existing["name"] != name:
                cur.execute("UPDATE referees SET name=? WHERE email=?", (name, email))
                updated += 1
        else:
            cur.execute("INSERT INTO referees(name, email, active) VALUES (?, ?, 1)", (name, email))
            added += 1

    conn.commit()
    conn.close()
    return added, updated


def replace_referees_csv(df: pd.DataFrame):
    cols = {c.lower().strip(): c for c in df.columns}
    if "name" not in cols or "email" not in cols:
        raise ValueError("Referees CSV must contain columns: name, email")

    new_refs = []
    for _, row in df.iterrows():
        name = str(row[cols["name"]]).strip()
        email = str(row[cols["email"]]).strip().lower()
        if not name or not email or email == "nan":
            continue
        new_refs.append((name, email))

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
            "INSERT INTO referees(name, email, active) VALUES (?, ?, 1)",
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


# ============================================================
# Data helpers
# ============================================================
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
        SELECT id, name, email
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
            r.name AS ref_name,
            r.email AS ref_email
        FROM assignments a
        LEFT JOIN referees r ON r.id = a.referee_id
        WHERE a.id=?
        LIMIT 1
        """,
        (assignment_id,),
    ).fetchone()
    conn.close()
    return row


def game_local_date(game_row):
    start = dtparser.parse(game_row["start_dt"])
    return start.date()


def referee_has_blackout(ref_id: int, d: date) -> bool:
    conn = db()
    row = conn.execute(
        """
        SELECT 1 FROM blackouts
        WHERE referee_id=? AND blackout_date=?
        LIMIT 1
        """,
        (ref_id, d.isoformat()),
    ).fetchone()
    conn.close()
    return bool(row)


def set_assignment_ref(assignment_id: int, ref_id: int | None):
    conn = db()
    conn.execute(
        """
        UPDATE assignments
        SET referee_id=?,
            status='EMPTY',
            updated_at=?
        WHERE id=?
        """,
        (ref_id, now_iso(), assignment_id),
    )
    conn.commit()
    conn.close()


def set_assignment_status(assignment_id: int, status: str):
    conn = db()
    conn.execute(
        """
        UPDATE assignments
        SET status=?, updated_at=?
        WHERE id=?
        """,
        (status, now_iso(), assignment_id),
    )
    conn.commit()
    conn.close()


def clear_assignment(assignment_id: int):
    conn = db()
    conn.execute(
        """
        UPDATE assignments
        SET referee_id=NULL,
            status='EMPTY',
            updated_at=?
        WHERE id=?
        """,
        (now_iso(), assignment_id),
    )
    conn.execute("DELETE FROM offers WHERE assignment_id=?", (assignment_id,))
    conn.commit()
    conn.close()


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


def resolve_offer(token: str, response: str) -> tuple[bool, str]:
    conn = db()
    offer = conn.execute(
        """
        SELECT id, assignment_id, responded_at, response
        FROM offers
        WHERE token=?
        """,
        (token,),
    ).fetchone()

    if not offer:
        conn.close()
        return False, "Invalid or unknown offer link."

    if offer["responded_at"] is not None:
        conn.close()
        return False, f"This offer was already responded to ({offer['response']})."

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

    # Clean the URL
    st.query_params.clear()
    st.rerun()


def referee_logout_button():
    if st.session_state.get("referee_id"):
        c1, c2 = st.columns([3, 1])
        with c1:
            st.caption(f"Logged in as: {st.session_state.get('referee_name')} ({st.session_state.get('referee_email')})")
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
        subtitle = f"Field: {o['field_name']} • Start: {start_dt.strftime('%Y-%m-%d %H:%M')} • Slot {o['slot_no']}"

        with st.container(border=True):
            st.subheader(title)
            st.caption(subtitle)

            if o["responded_at"]:
                st.success(f"Response recorded: {o['response']}")
            else:
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


# ============================================================
# Public offer handler (legacy accept/decline links)
# ============================================================
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

# Referee portal: allow login before admin login
maybe_handle_referee_portal_login()

# Legacy accept/decline links still supported
maybe_handle_offer_response()

# If referee is logged in, show portal and stop (no admin UI)
if render_my_offers_page():
    st.stop()

# Admin login flow
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
                login_url = send_admin_login_email(email)
                st.success("Login link created. If the email doesn’t arrive, use the link below:")
                st.code(login_url)
                st.markdown(f"[Open admin login link]({login_url})")
            except Exception as e:
                st.error(str(e))

    st.stop()

# Logged in view
admin_logout_button()

tabs = st.tabs(["Admin", "Import", "Blackouts", "Administrators"])

# ============================================================
# Administrators tab
# ============================================================
with tabs[3]:
    st.subheader("Administrators (allowlist)")
    st.caption("Add/remove admins by email. Removed admins lose access immediately.")

    admins = list_admins()
    if admins:
        df_admins = pd.DataFrame(
            [{"email": a["email"], "active": "YES" if a["active"] == 1 else "NO", "created_at": a["created_at"]} for a in admins]
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

# ============================================================
# Import tab
# ============================================================
with tabs[1]:
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
    st.caption("Required columns: name, email")

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
            st.success(f"Imported blackouts. Added: {added}. Skipped (unknown referee email): {skipped}")
            st.rerun()

# ============================================================
# Blackouts tab
# ============================================================
with tabs[2]:
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
# Admin tab
# ============================================================
with tabs[0]:
    st.subheader("Games & Assignments")

    auto = st.toggle("Auto-refresh every 5 seconds", value=True, key="auto_refresh_toggle")
    if auto:
        st_autorefresh(interval=5000, key="auto_refresh_tick")

    if st.button("Refresh status", key="refresh_status_btn"):
        st.rerun()

    games = get_games()
    refs = get_referees()

    if not games:
        st.info("Import a Games CSV first (Import tab).")
        st.stop()

    all_dates = sorted({game_local_date(g) for g in games})
    today = date.today()
    default_idx = 0
    for i, d in enumerate(all_dates):
        if d >= today:
            default_idx = i
            break

    selected_date = st.selectbox("Show games for date", all_dates, index=default_idx, key="games_date_select")
    count_games = sum(1 for g in games if game_local_date(g) == selected_date)
    st.caption(f"{count_games} game(s) on {selected_date.isoformat()}")

    for g in games:
        if game_local_date(g) != selected_date:
            continue

        start_dt = dtparser.parse(g["start_dt"])
        gdate = game_local_date(g)

        ref_options = ["— Select referee —"]
        ref_lookup = {}
        for r in refs:
            label = f"{r['name']} ({r['email']})"
            if referee_has_blackout(r["id"], gdate):
                label = f"🚫 {label} — blackout"
            ref_options.append(label)
            ref_lookup[label] = r["id"]

        with st.container(border=True):
            st.markdown(
                f"**{g['home_team']} vs {g['away_team']}**  \n"
                f"Field: **{g['field_name']}**  \n"
                f"Start: **{start_dt.strftime('%Y-%m-%d %H:%M')}**"
            )

            assigns = get_assignments_for_game(g["id"])
            cols = st.columns(2)

            for idx, a in enumerate(assigns):
                with cols[idx]:
                    st.markdown(f"#### Slot {a['slot_no']}")

                    status = (a["status"] or "").strip().upper()
                    st.caption(f"assignment_id={a['id']} | status={status} | updated_at={a['updated_at']}")

                    current_ref_label = None
                    if a["referee_id"] is not None and a["ref_name"] and a["ref_email"]:
                        current_ref_label = f"{a['ref_name']} ({a['ref_email']})"
                        if referee_has_blackout(a["referee_id"], gdate):
                            current_ref_label = f"🚫 {current_ref_label} — blackout"

                    default_index = 0
                    if current_ref_label and current_ref_label in ref_options:
                        default_index = ref_options.index(current_ref_label)

                    refpick_key = f"refpick_{g['id']}_{a['slot_no']}"
                    pick = st.selectbox(
                        "Referee",
                        ref_options,
                        index=default_index,
                        key=refpick_key,
                        disabled=(status in ("ACCEPTED", "ASSIGNED")),
                    )

                    if pick != "— Select referee —":
                        chosen_ref_id = ref_lookup[pick]
                        if status in ("ACCEPTED", "ASSIGNED"):
                            st.info("This slot is locked (ACCEPTED/ASSIGNED). Use Action → RESET to change it.")
                        else:
                            if a["referee_id"] != chosen_ref_id:
                                set_assignment_ref(a["id"], chosen_ref_id)
                                st.rerun()
                    else:
                        if a["referee_id"] is not None:
                            clear_assignment(a["id"])
                            st.session_state[refpick_key] = "— Select referee —"
                            st.rerun()

                    blackout = False
                    if a["referee_id"] is not None:
                        blackout = referee_has_blackout(a["referee_id"], gdate)

                    if status == "ACCEPTED":
                        status_badge(f"✅ {a['ref_name']} — ACCEPTED", bg="#2e7d32")
                    elif status == "ASSIGNED":
                        status_badge(f"✅ {a['ref_name']} — ASSIGNED", bg="#2e7d32")
                    elif status == "DECLINED":
                        status_badge(f"❌ {a['ref_name']} — DECLINED", bg="#c62828")
                    elif status == "OFFERED":
                        status_badge(f"⬜ {a['ref_name']} — OFFERED", bg="#546e7a")
                    elif a["referee_id"] is not None:
                        status_badge(f"⬛ {a['ref_name']} — NOT OFFERED YET", bg="#424242")
                    else:
                        st.caption("EMPTY")

                    if blackout:
                        st.warning(f"Blackout date conflict: {gdate.isoformat()}")

                    action_key = f"action_{a['id']}"
                    msg_key = f"msg_{a['id']}"
                    st.session_state.setdefault(action_key, "—")

                    action_options = ["—", "OFFER", "ASSIGN", "DELETE", "RESET"]
                    if status in ("ACCEPTED", "ASSIGNED"):
                        action_options = ["—", "RESET", "DELETE"]

                    def on_action_change(
                        assignment_id=a["id"],
                        g=g,
                        status=status,
                        blackout=blackout,
                        start_dt=start_dt,
                        action_key=action_key,
                        msg_key=msg_key,
                        refpick_key=refpick_key,
                    ):
                        choice = st.session_state.get(action_key, "—")
                        st.session_state.pop(msg_key, None)

                        if choice == "—":
                            return

                        live_a = get_assignment_live(assignment_id)
                        if not live_a:
                            st.session_state[msg_key] = "Could not load assignment."
                            st.session_state[action_key] = "—"
                            st.rerun()

                        live_ref_id = live_a["referee_id"]
                        live_ref_name = live_a["ref_name"]
                        live_ref_email = live_a["ref_email"]

                        if live_ref_id is None and choice in ("OFFER", "ASSIGN"):
                            st.session_state[msg_key] = "Select a referee first."
                            st.session_state[action_key] = "—"
                            return

                        if choice == "OFFER" and status in ("ACCEPTED", "ASSIGNED"):
                            st.session_state[msg_key] = "This slot is already confirmed (ACCEPTED/ASSIGNED)."
                            st.session_state[action_key] = "—"
                            return

                        if choice == "OFFER":
                            if blackout:
                                st.session_state[msg_key] = (
                                    "Offer blocked: referee is unavailable on this date (blackout). "
                                    "You can still ASSIGN manually if needed."
                                )
                                st.session_state[action_key] = "—"
                                return

                            try:
                                token = create_offer(assignment_id)
                                cfg = smtp_settings()
                                base = cfg.get("app_base_url", "").rstrip("/")

                                game_line = f"{g['home_team']} vs {g['away_team']}"
                                when_line = start_dt.strftime("%Y-%m-%d %H:%M")

                                # Better deliverability: personalised subject
                                subject = f"{live_ref_name} — Match assignment: {g['home_team']} vs {g['away_team']}"

                                if REF_PORTAL_ENABLED:
                                    portal_url = f"{base}/?offer_token={token}"

                                    text = (
                                        f"Hi {live_ref_name},\n\n"
                                        f"You have a match assignment offer:\n"
                                        f"- Game: {game_line}\n"
                                        f"- Field: {g['field_name']}\n"
                                        f"- Start: {when_line}\n\n"
                                        f"View and respond here:\n{portal_url}\n"
                                    )

                                    html = f"""
                                    <div style="font-family: Arial, sans-serif; line-height:1.4;">
                                      <p>Hi {live_ref_name},</p>
                                      <p>You have a match assignment offer:</p>
                                      <ul>
                                        <li><b>Game:</b> {game_line}</li>
                                        <li><b>Field:</b> {g['field_name']}</li>
                                        <li><b>Start:</b> {when_line}</li>
                                      </ul>
                                      <p>
                                        <a href="{portal_url}" style="display:inline-block;padding:10px 14px;background:#1565c0;color:#fff;text-decoration:none;border-radius:6px;">
                                          View offer
                                        </a>
                                      </p>
                                      <p style="color:#666;font-size:12px;">If the button doesn’t work, copy and paste this link:<br>{portal_url}</p>
                                    </div>
                                    """
                                    send_html_email(live_ref_email, live_ref_name, subject, html, text_body=text)

                                else:
                                    accept_url = f"{base}/?action=accept&token={token}"
                                    decline_url = f"{base}/?action=decline&token={token}"

                                    text = (
                                        f"Hi {live_ref_name},\n\n"
                                        f"You have a referee assignment offer:\n"
                                        f"- Game: {game_line}\n"
                                        f"- Field: {g['field_name']}\n"
                                        f"- Start: {when_line}\n\n"
                                        f"Accept: {accept_url}\n"
                                        f"Decline: {decline_url}\n"
                                    )

                                    html = f"""
                                    <div style="font-family: Arial, sans-serif; line-height:1.4;">
                                      <p>Hi {live_ref_name},</p>
                                      <p>You have been offered a referee assignment:</p>
                                      <ul>
                                        <li><b>Game:</b> {game_line}</li>
                                        <li><b>Field:</b> {g['field_name']}</li>
                                        <li><b>Start:</b> {when_line}</li>
                                      </ul>
                                      <p>
                                        <a href="{accept_url}">ACCEPT</a> |
                                        <a href="{decline_url}">DECLINE</a>
                                      </p>
                                    </div>
                                    """
                                    send_html_email(live_ref_email, live_ref_name, subject, html, text_body=text)

                                set_assignment_status(assignment_id, "OFFERED")
                                st.session_state[msg_key] = "Offer sent and marked as OFFERED."

                            except Exception as e:
                                st.session_state[msg_key] = str(e)

                        elif choice == "ASSIGN":
                            set_assignment_status(assignment_id, "ASSIGNED")
                            st.session_state[msg_key] = "Assigned."

                        elif choice in ("DELETE", "RESET"):
                            clear_assignment(assignment_id)
                            st.session_state[refpick_key] = "— Select referee —"
                            st.session_state[msg_key] = "Slot cleared (EMPTY)."

                        st.session_state[action_key] = "—"
                        st.rerun()

                    st.selectbox(
                        "Action",
                        action_options,
                        key=action_key,
                        on_change=on_action_change,
                    )

                    if st.session_state.get(msg_key):
                        st.info(st.session_state[msg_key])

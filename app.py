# app.py
# Referee Allocator (MVP) — Admin + Referee Portal + Offers + Blackouts + Printable A4 Landscape PDF

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
# Small utilities
# ============================================================
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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


def _time_12h(dt: datetime) -> str:
    # Cross-platform safe 12hr format with no leading zero
    return dt.strftime("%I:%M %p").lstrip("0")


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
    """
    Env vars (Render) take priority; secrets.toml (local) is fallback.

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
    """
    Multipart/alternative: include text/plain + text/html (helps deliverability).
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
            st.query_params.clear()


def send_admin_login_email(email: str) -> str:
    """
    Sends the admin login email. Returns URL (useful for debugging),
    but we do NOT display it in the UI.
    """
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
        c1, c2 = st.columns([3, 1])
        with c1:
            st.caption(f"Logged in as: {st.session_state['admin_email']}")
        with c2:
            if st.button("Log out"):
                st.session_state.pop("admin_email", None)
                st.query_params.clear()
                st.rerun()


# ============================================================
# Imports & data helpers
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


# ============================================================
# Printable PDF helpers
# ============================================================
def get_admin_print_rows_for_date(selected_date: date):
    """
    Per-game rows for ONE date, with slot referee names/status.
    """
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
    """
    A4 landscape, grouped by start time.
    - Title: NO "(A4 Landscape)"
    - 12hr time format
    - Compressed layout (tight margins/padding/font) to fit on 1 page where possible
    """
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

    # group by exact start time (datetime), but label as 12hr
    grouped = {}
    for g in games:
        dt = dtparser.parse(g["start_dt"])
        key = _time_12h(dt)
        grouped.setdefault(key, []).append((dt, g))

    # chronological sort without reparsing strings (safe)
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
            colWidths=[360, 110, 75, 210],  # tuned for A4 landscape
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

def _refs_names_only_for_game(g: dict) -> str:
    r1 = (g["slots"][1]["name"] or "").strip()
    r2 = (g["slots"][2]["name"] or "").strip()
    if not r1:
        r1 = "—"
    if not r2:
        r2 = "—"
    return f"{r1} / {r2}"


def build_referee_scorecards_pdf_bytes(selected_date: date) -> bytes:
    """
    A4 portrait. 6 scorecards per page (2 across x 3 down).
    One scorecard per GAME (not per referee).
    Helvetica everywhere.
    Each team has its own 1–10 tries row.
    """
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

    TRIES_LABEL_SIZE = 10
    TRIES_NUM_SIZE = 18

    FOOT_LABEL_SIZE = 12

    BOX_W = int(26 * 1.5)
    BOX_H = int(14 * 1.5)

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

        tries_label_y = y_top - 92
        c.setFont("Helvetica-Bold", TRIES_LABEL_SIZE)
        c.drawString(left, tries_label_y, "Tries:")
        c.setFont("Helvetica", TRIES_LABEL_SIZE)
        c.drawString(left + 34, tries_label_y, "(circle 2 numbers for a female try)")

        row1_y = tries_label_y - 26
        row2_y = row1_y - 22

        label_max_w = max_text_w * 0.38
        label_left = left
        numbers_left = left + label_max_w + 6
        numbers_right = right
        numbers_span = max(10, numbers_right - numbers_left)

        tries_row_num_size = max(13, TRIES_NUM_SIZE - 4)
        team_row_max_size = 11
        team_row_min_size = 8

        def draw_team_tries_row(team_name: str, y: float):
            nm = str(team_name)
            size = fit_left_text_size(nm, label_max_w, team_row_max_size, team_row_min_size)
            c.setFont("Helvetica-Bold", size)
            c.drawString(label_left, y, nm)

            step = numbers_span / 10.0
            c.setFont("Helvetica-Bold", tries_row_num_size)
            for i in range(10):
                n = str(i + 1)
                n_x = numbers_left + (step * (i + 0.5))
                c.drawCentredString(n_x, y, n)

        draw_team_tries_row(g["home_team"], row1_y)
        draw_team_tries_row(g["away_team"], row2_y)

        line_y = row2_y - 14
        c.setLineWidth(0.8)
        c.line(left, line_y, right, line_y)

        box_x = right - BOX_W

        wld1_y = line_y - 18
        wld2_y = wld1_y - 18
        wld_right_text = "W  /  L  /  D"

        wld_team_max_w = (box_x - 14) - left

        team1 = str(g["home_team"])
        size1 = fit_left_text_size(team1, wld_team_max_w, FOOT_LABEL_SIZE, 9)
        c.setFont("Helvetica-Bold", size1)
        c.drawString(left, wld1_y, team1)
        c.setFont("Helvetica-Bold", FOOT_LABEL_SIZE)
        c.drawRightString(box_x - 10, wld1_y, wld_right_text)

        team2 = str(g["away_team"])
        size2 = fit_left_text_size(team2, wld_team_max_w, FOOT_LABEL_SIZE, 9)
        c.setFont("Helvetica-Bold", size2)
        c.drawString(left, wld2_y, team2)
        c.setFont("Helvetica-Bold", FOOT_LABEL_SIZE)
        c.drawRightString(box_x - 10, wld2_y, wld_right_text)

        conduct_y = wld2_y - 22
        c.setFont("Helvetica-Bold", FOOT_LABEL_SIZE)
        c.drawString(left, conduct_y, "Conduct (/10)")
        c.setLineWidth(1)
        c.rect(box_x, conduct_y - 6, BOX_W, BOX_H)

        unstrip_y = conduct_y - 26
        c.setFont("Helvetica-Bold", FOOT_LABEL_SIZE)
        c.drawString(left, unstrip_y, "Unstripped Players")
        c.rect(box_x, unstrip_y - 6, BOX_W, BOX_H)

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
    """
    Record (or overwrite) a referee response for an offer token.

    Overwrite is allowed:
    - offers.responded_at is set to now again
    - offers.response is overwritten
    - assignments.status is set to ACCEPTED/DECLINED accordingly
    """
    response = (response or "").strip().upper()
    if response not in ("ACCEPTED", "DECLINED"):
        return False, "Invalid response."

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

    # Always overwrite the response (even if already responded before)
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
    """
    Create offer, email referee, mark OFFERED.
    If email fails, delete the offer record (keeps state consistent).
    """
    token = create_offer(assignment_id)

    try:
        cfg = smtp_settings()
        base = cfg.get("app_base_url", "").rstrip("/")
        if not base:
            raise RuntimeError("APP_BASE_URL is missing. Add it in Render environment variables.")

        game_line = f"{game['home_team']} vs {game['away_team']}"
        when_line = start_dt.strftime("%Y-%m-%d %I:%M %p").lstrip("0")
        subject = f"{referee_name} — Match assignment: {game['home_team']} vs {game['away_team']}"

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

    st.query_params.clear()
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

            # Show current status (if any), but DO NOT lock the UI
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


# ============================================================
# Legacy offer response handler (kept for backwards compatibility)
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

maybe_handle_referee_portal_login()
maybe_handle_offer_response()

# If referee logged in, show portal and stop (no admin UI)
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

    # Printable PDF UI (inside Admin tab — correct indentation)
    st.markdown("---")
    st.subheader("Printable Summary")

    c_pdf1, c_pdf2, c_pdf3, c_pdf4 = st.columns([1, 2, 1, 2])

    # Summary PDF (existing)
    with c_pdf1:
        if st.button("Build Summary PDF", key="build_pdf_btn"):
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
                label="Download Summary PDF",
                data=pdf_bytes,
                file_name=filename,
                mime="application/pdf",
                key="download_pdf_btn",
            )
        else:
            st.caption("Click **Build Summary PDF** to generate the printable schedule.")

    # NEW: Referee Scorecards PDF
    with c_pdf3:
        if st.button("Build Referee Scorecards", key="build_scorecards_btn"):
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
                label="Download Scorecards PDF",
                data=pdf_bytes,
                file_name=filename,
                mime="application/pdf",
                key="download_scorecards_btn",
            )
        else:
            st.caption("Click **Build Referee Scorecards** to generate the 6-up scorecards.")


    # Games list + assignments UI
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
                f"Start: **{start_dt.strftime('%Y-%m-%d %I:%M %p').lstrip('0')}**"
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

                            send_offer_email_and_mark_offered(
                                assignment_id=assignment_id,
                                referee_name=live_ref_name,
                                referee_email=live_ref_email,
                                game=g,
                                start_dt=start_dt,
                                msg_key=msg_key,
                            )

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

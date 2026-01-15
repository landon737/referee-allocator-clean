# =========================
# PDF helpers (CLEAN)
# =========================
from io import BytesIO
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from datetime import date

def get_admin_print_rows_for_date(selected_date: date):
    """
    Returns per-game rows for the printable admin summary (for ONE date).
    Each row includes teams, field, start_dt, and slot referee names/statuses.
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

    games_map: dict[int, dict] = {}

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
    """
    Display referee name with optional status (helpful for admins).
    """
    name = (name or "").strip()
    status = (status or "EMPTY").strip().upper()

    if not name:
        return "—"
    if status in ("ACCEPTED", "ASSIGNED"):
        return name
    if status in ("OFFERED", "DECLINED"):
        return f"{name} ({status})"
    return name


def _time_12h(dt: datetime) -> str:
    """
    12-hour time formatting with no leading zero.
    Cross-platform safe.
    """
    return dt.strftime("%I:%M %p").lstrip("0")


def build_admin_summary_pdf_bytes(selected_date: date) -> bytes:
    """
    Creates an A4 landscape PDF (bytes), grouped by start time.

    Changes requested:
    1) Title: removed "(A4 Landscape)"
    2) 12-hour time format
    3) Compressed layout to fit on one page as much as practical
    """
    games = get_admin_print_rows_for_date(selected_date)

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=18,
        rightMargin=18,
        topMargin=18,
        bottomMargin=18,
        title=f"Game Summary {selected_date.isoformat()}",
    )

    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"<b>Game Summary</b> — {selected_date.isoformat()}", styles["Title"]))
    story.append(Spacer(1, 6))

    if not games:
        story.append(Paragraph("No games found for this date.", styles["Normal"]))
        doc.build(story)
        return buffer.getvalue()

    # Group by start time (12-hour)
    grouped: dict[str, list[tuple[datetime, dict]]] = {}
    for g in games:
        dt = dtparser.parse(g["start_dt"])
        key = _time_12h(dt)
        grouped.setdefault(key, []).append((dt, g))

    # Sort time keys chronologically by converting back using the date
    group_keys = sorted(grouped.keys(), key=lambda t: dtparser.parse(f"{selected_date.isoformat()} {t}"))

    for time_key in group_keys:
        story.append(Paragraph(f"<b>Start time: {time_key}</b>", styles["Heading3"]))
        story.append(Spacer(1, 3))

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
            colWidths=[360, 120, 70, 220],  # slightly tighter than before
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

                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                ]
            )
        )

        story.append(table)
        story.append(Spacer(1, 6))

    doc.build(story)
    return buffer.getvalue()

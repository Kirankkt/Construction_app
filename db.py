from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Iterable, List, Dict, Any, Tuple

import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, Text, Float, TIMESTAMP,
    ForeignKey, select, func, UniqueConstraint, and_, or_, text, insert, update, delete
)
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

# ---------- Connection ----------

def get_pg_url_from_secrets() -> str:
    # Support a couple of common keys
    import streamlit as st
    if "pg" in st.secrets and "url" in st.secrets["pg"]:
        return st.secrets["pg"]["url"]
    if "POSTGRES_URL" in st.secrets:
        return st.secrets["POSTGRES_URL"]
    raise RuntimeError("PostgreSQL URL not found in secrets. Add pg.url to .streamlit/secrets.toml")

def get_engine() -> Engine:
    url = get_pg_url_from_secrets()
    return create_engine(url, pool_pre_ping=True, pool_recycle=1800)

metadata = MetaData()

sheets = Table("sheets", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", Text, unique=True, nullable=False),
    Column("created_at", TIMESTAMP, server_default=func.now()),
)

rows = Table("rows", metadata,
    Column("id", Integer, primary_key=True),
    Column("sheet_id", Integer, ForeignKey("sheets.id", ondelete="CASCADE"), nullable=False),
    Column("section", Text),
    Column("subsection", Text),
    Column("row_order", Integer, nullable=False),
    UniqueConstraint("sheet_id", "row_order", name="uq_rows_sheet_roworder")
)

day_cells = Table("day_cells", metadata,
    Column("id", Integer, primary_key=True),
    Column("row_id", Integer, ForeignKey("rows.id", ondelete="CASCADE"), nullable=False),
    Column("day", Integer, nullable=False),
    Column("task", Text),
    Column("hours", Float),
    Column("labor_code", Text),
    Column("updated_at", TIMESTAMP, server_default=func.now()),
    UniqueConstraint("row_id", "day", name="uq_cells_row_day")
)

audit_log = Table("audit_log", metadata,
    Column("id", Integer, primary_key=True),
    Column("who", Text),
    Column("action", Text, nullable=False),
    Column("payload", Text),
    Column("ts", TIMESTAMP, server_default=func.now()),
)

# ---------- Bootstrap ----------

def init_db(engine: Engine) -> None:
    metadata.create_all(engine)

# ---------- Sheets ----------

def get_or_create_sheet(conn: Connection, name: str) -> int:
    r = conn.execute(select(sheets.c.id).where(sheets.c.name == name)).scalar()
    if r:
        return r
    r = conn.execute(sheets.insert().values(name=name).returning(sheets.c.id)).scalar()
    return int(r)

def list_sheets(conn: Connection) -> list[dict]:
    res = conn.execute(select(sheets.c.id, sheets.c.name).order_by(sheets.c.created_at.desc()))
    return [{"id": r.id, "name": r.name} for r in res]

# ---------- Rows & Cells ----------

def add_row(conn: Connection, sheet_id: int, section: str, subsection: str, row_order: int) -> int:
    r = conn.execute(rows.insert().values(
        sheet_id=sheet_id, section=section, subsection=subsection, row_order=row_order
    ).returning(rows.c.id)).scalar()
    return int(r)

def max_row_order(conn: Connection, sheet_id: int) -> int:
    val = conn.execute(select(func.coalesce(func.max(rows.c.row_order), 0)).where(rows.c.sheet_id == sheet_id)).scalar()
    return int(val or 0)

def get_sections(conn: Connection, sheet_id: int) -> list[str]:
    res = conn.execute(select(rows.c.section).where(rows.c.sheet_id == sheet_id).distinct().order_by(rows.c.section))
    return [r.section for r in res if r.section]

def get_subsections(conn: Connection, sheet_id: int, section: str) -> list[str]:
    res = conn.execute(
        select(rows.c.subsection).where(and_(rows.c.sheet_id == sheet_id, rows.c.section == section))
        .distinct().order_by(rows.c.subsection)
    )
    return [r.subsection for r in res if r.subsection]

def get_rows_for_subsection(conn: Connection, sheet_id: int, section: str, subsection: str) -> list[dict]:
    res = conn.execute(
        select(rows.c.id, rows.c.row_order)
        .where(and_(rows.c.sheet_id == sheet_id, rows.c.section == section, rows.c.subsection == subsection))
        .order_by(rows.c.row_order)
    )
    return [{"id": r.id, "row_order": r.row_order} for r in res]

def swap_row_order(conn: Connection, row_id_a: int, row_id_b: int) -> None:
    a = conn.execute(select(rows.c.row_order, rows.c.sheet_id).where(rows.c.id == row_id_a)).first()
    b = conn.execute(select(rows.c.row_order, rows.c.sheet_id).where(rows.c.id == row_id_b)).first()
    if not a or not b or a.sheet_id != b.sheet_id:
        return
    conn.execute(update(rows).where(rows.c.id == row_id_a).values(row_order=b.row_order))
    conn.execute(update(rows).where(rows.c.id == row_id_b).values(row_order=a.row_order))

def upsert_cell(conn: Connection, row_id: int, day: int,
                task: Optional[str]=None, hours: Optional[float]=None, labor_code: Optional[str]=None) -> None:
    # Try update first
    r = conn.execute(select(day_cells.c.id).where(and_(day_cells.c.row_id == row_id, day_cells.c.day == day))).scalar()
    payload = {}
    if task is not None: payload["task"] = task
    if hours is not None: payload["hours"] = float(hours)
    if labor_code is not None: payload["labor_code"] = labor_code
    if not payload:
        return
    if r:
        conn.execute(update(day_cells).where(day_cells.c.id == r).values(**payload))
    else:
        conn.execute(day_cells.insert().values(row_id=row_id, day=day, **payload))

def read_cell_preview(conn: Connection, row_id: int) -> str:
    # first non-empty task across days
    r = conn.execute(
        select(day_cells.c.day, day_cells.c.task)
        .where(and_(day_cells.c.row_id == row_id, day_cells.c.task != None, day_cells.c.task != ""))
        .order_by(day_cells.c.day.asc()).limit(1)
    ).first()
    if not r:
        return "(empty)"
    return f"Day {r.day}: {str(r.task)[:40]}"

# ---------- Labor helpers ----------

def people_from_labor_code(code: Optional[str]) -> int:
    if not code:
        return 0
    try:
        return int(str(code).split(".")[0])
    except Exception:
        return 0

# ---------- Import / Export ----------

MAJOR_SECTION_ANCHORS = ["Outside", "Ground Floor", "1st Floor", "First Floor", "Roof"]

def import_wide_csv(conn: Connection, csv_path: str, sheet_name: str) -> int:
    """
    Parse a wide CSV like yours and populate normalized tables.
    """
    df = pd.read_csv(csv_path)
    first_col = df.columns[0]
    day_cols = [c for c in df.columns if str(c).strip().lower().startswith("day ")]
    if not day_cols:
        raise ValueError("No 'Day N' columns found in CSV.")

    # find time/labor next to each Day
    def trio(i, c):
        time_c = df.columns[i+1] if i+1 < len(df.columns) else None
        labor_c = df.columns[i+2] if i+2 < len(df.columns) else None
        return time_c, labor_c

    triplets = []
    for i, c in enumerate(df.columns):
        if str(c).strip().lower().startswith("day "):
            dnum = int(str(c).split()[-1])
            t, l = trio(i, c)
            triplets.append((c, t, l, dnum))

    sheet_id = get_or_create_sheet(conn, sheet_name)
    # wipe existing contents for this sheet
    conn.execute(delete(day_cells).where(day_cells.c.row_id.in_(select(rows.c.id).where(rows.c.sheet_id == sheet_id))))
    conn.execute(delete(rows).where(rows.c.sheet_id == sheet_id))

    current_section = None
    order = 0
    for _, r in df.iterrows():
        label = str(r[first_col]).strip() if pd.notna(r[first_col]) else None
        # Section header: label present and all day/time/labor blank
        is_header = True
        for (dc, tc, lc, _) in triplets:
            for cc in [dc, tc, lc]:
                if cc is not None and pd.notna(r.get(cc, None)) and str(r.get(cc, "")).strip() != "":
                    is_header = False
                    break
            if not is_header:
                break
        if is_header and label:
            current_section = label
            continue
        if label is None and current_section is None:
            continue

        subsection = label
        order += 1
        row_id = add_row(conn, sheet_id, current_section, subsection, order)

        for (dc, tc, lc, dnum) in triplets:
            task = r.get(dc, None)
            hours = r.get(tc, None) if tc else None
            labor = r.get(lc, None) if lc else None
            if (pd.isna(task) or str(task).strip()=="") and pd.isna(hours) and (labor is None or str(labor).strip()==""):
                continue
            upsert_cell(conn, row_id, dnum,
                        task=str(task).strip() if pd.notna(task) and str(task).strip()!="" else None,
                        hours=float(hours) if hours is not None and pd.notna(hours) else None,
                        labor_code=str(labor).strip() if labor is not None and str(labor).strip()!="" else None)

    return sheet_id

def export_wide_csv(conn: Connection, sheet_id: int, out_path: str) -> str:
    """
    Rebuild wide CSV (Day/Time/Labor triplets) so it looks like your original.
    """
    # compute max day for that sheet
    mday = conn.execute(select(func.coalesce(func.max(day_cells.c.day), 1)).select_from(
        day_cells.join(rows, day_cells.c.row_id == rows.c.id).where(rows.c.sheet_id == sheet_id)
    )).scalar() or 1

    # columns
    cols = ["Section/Subsection"]
    for d in range(1, mday+1):
        suffix = "" if d == 1 else f".{d-1}"
        cols.extend([f"Day {d}", f"Time (hours){suffix}", f"Labor (workers){suffix}"])
    out = []

    # iterate by section -> subsection order
    sec_res = conn.execute(
        select(rows.c.section).where(rows.c.sheet_id == sheet_id).distinct().order_by(rows.c.section)
    ).fetchall()
    for sec_row in sec_res:
        section = sec_row.section
        # section header row
        hdr = [section] + [""]*(len(cols)-1)
        out.append(hdr)

        subs = conn.execute(
            select(rows.c.id, rows.c.subsection, rows.c.row_order)
            .where(and_(rows.c.sheet_id == sheet_id, rows.c.section == section))
            .order_by(rows.c.row_order.asc())
        ).fetchall()
        for rr in subs:
            row = [rr.subsection or ""]
            # cells for each day
            for d in range(1, mday+1):
                cell = conn.execute(
                    select(day_cells.c.task, day_cells.c.hours, day_cells.c.labor_code)
                    .where(and_(day_cells.c.row_id == rr.id, day_cells.c.day == d))
                ).first()
                if cell:
                    task, hours, labor = cell
                    row.extend([task or "", hours if hours is not None else "", labor or ""])
                else:
                    row.extend(["", "", ""])
            out.append(row)

    df = pd.DataFrame(out, columns=cols)
    df.to_csv(out_path, index=False)
    return out_path

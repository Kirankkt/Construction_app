from __future__ import annotations

import os
from typing import Optional, List, Dict, Any

import streamlit as st
import pandas as pd
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float, Text, DateTime,
    ForeignKey, UniqueConstraint, select, func, delete, and_, update
)
from sqlalchemy.engine import Engine, Connection

# ---------------------------
# Secrets / engine
# ---------------------------
def get_pg_url_from_secrets() -> str:
    # Primary: Streamlit secrets
    try:
        return st.secrets["pg"]["url"]
    except Exception:
        pass
    # Fallback: env var
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "Postgres URL not found. Set [pg].url in Streamlit Secrets "
            "or DATABASE_URL env var."
        )
    return url


def get_engine() -> Engine:
    url = get_pg_url_from_secrets()
    # Fail fast on unreachable DBs
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={"connect_timeout": 10},  # <- THIS MAKES HANGS FAIL FAST
    )


# ---------------------------
# Schema (SQLAlchemy Core)
# ---------------------------
metadata = MetaData()

sheets = Table(
    "sheets", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", Text, nullable=False),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
)

rows = Table(
    "rows", metadata,
    Column("id", Integer, primary_key=True),
    Column("sheet_id", Integer, ForeignKey("sheets.id", ondelete="CASCADE"), index=True, nullable=False),
    Column("section", Text, nullable=False),
    Column("subsection", Text, nullable=False),
    Column("row_order", Integer, nullable=False, index=True),
    Column("created_at", DateTime, server_default=func.now(), nullable=False),
)

day_cells = Table(
    "day_cells", metadata,
    Column("id", Integer, primary_key=True),
    Column("row_id", Integer, ForeignKey("rows.id", ondelete="CASCADE"), index=True, nullable=False),
    Column("day", Integer, nullable=False, index=True),
    Column("task", Text, nullable=True),
    Column("hours", Float, nullable=True),
    Column("labor_code", String(32), nullable=True),
    UniqueConstraint("row_id", "day", name="uq_row_day"),
)

audit_log = Table(
    "audit_log", metadata,
    Column("id", Integer, primary_key=True),
    Column("at", DateTime, server_default=func.now(), nullable=False),
    Column("op", String(32), nullable=False),
    Column("object", String(32), nullable=False),
    Column("meta", Text, nullable=True),
)


def init_db(engine: Engine) -> None:
    metadata.create_all(engine)


# ---------------------------
# CRUD helpers
# ---------------------------
def list_sheets(conn: Connection) -> List[Dict[str, Any]]:
    res = conn.execute(select(sheets.c.id, sheets.c.name).order_by(sheets.c.created_at.asc()))
    return [{"id": r.id, "name": r.name} for r in res.fetchall()]


def get_or_create_sheet(conn: Connection, name: str) -> int:
    row = conn.execute(select(sheets.c.id).where(sheets.c.name == name)).fetchone()
    if row:
        return row.id
    rid = conn.execute(sheets.insert().values(name=name).returning(sheets.c.id)).scalar()
    return int(rid)


def get_sections(conn: Connection, sheet_id: int) -> List[str]:
    q = select(rows.c.section).where(rows.c.sheet_id == sheet_id).distinct().order_by(rows.c.section.asc())
    return [r.section for r in conn.execute(q).fetchall()]


def get_subsections(conn: Connection, sheet_id: int, section: str) -> List[str]:
    q = (
        select(rows.c.subsection)
        .where(and_(rows.c.sheet_id == sheet_id, rows.c.section == section))
        .distinct()
        .order_by(rows.c.subsection.asc())
    )
    return [r.subsection for r in conn.execute(q).fetchall()]


def get_rows_for_subsection(conn: Connection, sheet_id: int, section: str, subsection: str) -> List[Dict[str, Any]]:
    q = (
        select(rows.c.id, rows.c.row_order)
        .where(and_(rows.c.sheet_id == sheet_id, rows.c.section == section, rows.c.subsection == subsection))
        .order_by(rows.c.row_order.asc())
    )
    return [{"id": r.id, "row_order": r.row_order} for r in conn.execute(q).fetchall()]


def swap_row_order(conn: Connection, row_id_a: int, row_id_b: int) -> None:
    a = conn.execute(select(rows.c.row_order).where(rows.c.id == row_id_a)).fetchone()
    b = conn.execute(select(rows.c.row_order).where(rows.c.id == row_id_b)).fetchone()
    if not a or not b:
        return
    conn.execute(update(rows).where(rows.c.id == row_id_a).values(row_order=b.row_order))
    conn.execute(update(rows).where(rows.c.id == row_id_b).values(row_order=a.row_order))
    conn.execute(audit_log.insert().values(op="swap", object="row_order",
                                           meta=f"{row_id_a}<->{row_id_b}"))


def people_from_labor_code(code: Optional[str]) -> int:
    if not code:
        return 0
    try:
        return int(str(code).split(".")[0])
    except Exception:
        return 0


def upsert_cell(conn: Connection, row_id: int, day: int,
                task: Optional[str], hours: Optional[float], labor_code: Optional[str]) -> None:
    existing = conn.execute(
        select(day_cells.c.id).where(and_(day_cells.c.row_id == row_id, day_cells.c.day == day))
    ).fetchone()
    if existing:
        conn.execute(
            day_cells.update().where(day_cells.c.id == existing.id).values(
                task=task, hours=hours, labor_code=labor_code
            )
        )
        conn.execute(audit_log.insert().values(op="update", object="day_cell",
                                               meta=f"row={row_id}, day={day}"))
    else:
        conn.execute(
            day_cells.insert().values(row_id=row_id, day=day, task=task, hours=hours, labor_code=labor_code)
        )
        conn.execute(audit_log.insert().values(op="insert", object="day_cell",
                                               meta=f"row={row_id}, day={day}"))


def read_cell_preview(conn: Connection, row_id: int) -> str:
    q = (
        select(day_cells.c.day, day_cells.c.task, day_cells.c.hours, day_cells.c.labor_code)
        .where(day_cells.c.row_id == row_id)
        .order_by(day_cells.c.day.asc())
        .limit(1)
    )
    r = conn.execute(q).fetchone()
    if not r:
        return "empty"
    parts = []
    if r.task:
        parts.append(r.task)
    if r.hours is not None:
        parts.append(f"{r.hours}h")
    if r.labor_code:
        parts.append(f"{r.labor_code}")
    return f"D{r.day}: " + (", ".join(parts) if parts else "—")


# ---------------------------
# Import / Export
# ---------------------------
def import_wide_csv(conn: Connection, csv_path: str, sheet_name: str) -> int:
    """
    Wide CSV format:
      Col0 = Section/Subsection label (headers have only this col filled)
      Then repeated triplets by adjacency: [Day N] [Time (hours)] [Labor (workers)]
      (pandas may suffix duplicate headers like 'Time (hours).1' – that's OK.)
    No imputation; empty cells remain null.
    """
    df = pd.read_csv(csv_path)
    if df.shape[1] < 2:
        raise ValueError("CSV has too few columns.")

    first_col = df.columns[0]
    cols = list(df.columns)

    # Build (day_col, time_col, labor_col, day_number) triplets using adjacency
    triplets = []
    for i, c in enumerate(cols):
        if str(c).strip().lower().startswith("day "):
            try:
                dnum = int(str(c).split()[-1])
            except Exception:
                continue
            tcol = cols[i + 1] if i + 1 < len(cols) else None
            lcol = cols[i + 2] if i + 2 < len(cols) else None
            triplets.append((c, tcol, lcol, dnum))
    if not triplets:
        raise ValueError("No 'Day N' columns found in CSV.")

    sheet_id = get_or_create_sheet(conn, sheet_name)

    # Clear existing data for idempotent re-import
    conn.execute(delete(day_cells).where(
        day_cells.c.row_id.in_(select(rows.c.id).where(rows.c.sheet_id == sheet_id))
    ))
    conn.execute(delete(rows).where(rows.c.sheet_id == sheet_id))

    current_section: Optional[str] = None
    row_order = 0
    cells_bulk = []

    def _is_header(row) -> bool:
        for (dc, tc, lc, _) in triplets:
            if any(
                cc is not None and pd.notna(row.get(cc, None)) and str(row.get(cc, "")).strip() != ""
                for cc in (dc, tc, lc)
            ):
                return False
        return True

    for _, r in df.iterrows():
        label = str(r[first_col]).strip() if pd.notna(r[first_col]) else None

        if _is_header(r):
            if label:
                current_section = label
            continue

        if current_section is None:
            # orphan row before a header; skip
            continue

        subsection = label or ""
        row_order += 1

        # Insert row and capture row_id (avoid executemany+RETURNING)
        row_id = conn.execute(
            rows.insert().values(
                sheet_id=sheet_id, section=current_section, subsection=subsection, row_order=row_order
            ).returning(rows.c.id)
        ).scalar()

        for (dc, tc, lc, dnum) in triplets:
            task = r.get(dc, None)
            hours = r.get(tc, None) if tc else None
            labor = r.get(lc, None) if lc else None

            if (pd.isna(task) or str(task).strip() == "") and pd.isna(hours) and (labor is None or str(labor).strip() == ""):
                continue

            cells_bulk.append({
                "row_id": int(row_id),
                "day": int(dnum),
                "task": str(task).strip() if pd.notna(task) and str(task).strip() != "" else None,
                "hours": float(hours) if hours is not None and pd.notna(hours) else None,
                "labor_code": str(labor).strip() if labor is not None and str(labor).strip() != "" else None,
            })

    if cells_bulk:
        conn.execute(day_cells.insert(), cells_bulk)

    return int(sheet_id)


def export_wide_csv(conn: Connection, sheet_id: int, out_path: str) -> None:
    # collect rows ordered
    rws = conn.execute(
        select(rows.c.id, rows.c.section, rows.c.subsection, rows.c.row_order)
        .where(rows.c.sheet_id == sheet_id)
        .order_by(rows.c.section.asc(), rows.c.subsection.asc(), rows.c.row_order.asc())
    ).fetchall()
    if not rws:
        pd.DataFrame({"Empty": []}).to_csv(out_path, index=False)
        return

    row_ids = [r.id for r in rws]
    dcs = conn.execute(
        select(day_cells.c.row_id, day_cells.c.day, day_cells.c.task, day_cells.c.hours, day_cells.c.labor_code)
        .where(day_cells.c.row_id.in_(row_ids))
        .order_by(day_cells.c.row_id.asc(), day_cells.c.day.asc())
    ).fetchall()

    # Determine day range
    days = sorted({dc.day for dc in dcs}) if dcs else []
    max_day = max(days) if days else 0

    # Build dataframe: first column + triplets
    data = []
    for r in rws:
        label = r.subsection or ""
        row_map = {(dc.row_id, dc.day): dc for dc in dcs if dc.row_id == r.id}
        row_obj: Dict[str, Any] = {"Section/Subsection": label}
        for d in range(1, max_day + 1):
            dc = row_map.get((r.id, d))
            row_obj[f"Day {d}"] = (dc.task if dc else None)
            row_obj[f"Time (hours)"] = (float(dc.hours) if dc and dc.hours is not None else None)
            row_obj[f"Labor (workers)"] = (dc.labor_code if dc else None)
        data.append(row_obj)

    cols = ["Section/Subsection"]
    for d in range(1, max_day + 1):
        cols.extend([f"Day {d}", "Time (hours)", "Labor (workers)"])

    pd.DataFrame(data)[cols].to_csv(out_path, index=False)

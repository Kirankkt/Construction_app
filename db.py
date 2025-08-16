from __future__ import annotations

import os
from typing import Optional, List, Dict, Any

import pandas as pd
import streamlit as st
import sqlalchemy as sa
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String, Float, Text, DateTime,
    ForeignKey, UniqueConstraint, select, func, delete, and_, update
)
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.dialects.postgresql import insert as pg_insert


# ---------------------------
# Secrets / engine
# ---------------------------
def get_pg_url_from_secrets() -> str:
    try:
        return st.secrets["pg"]["url"]
    except Exception:
        pass
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "Postgres URL not found. Put it in Streamlit Secrets as [pg].url or in DATABASE_URL."
        )
    return url


def get_engine() -> Engine:
    url = get_pg_url_from_secrets()
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={"connect_timeout": 10},
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
# Safe audit helper (never breaks writes)
# ---------------------------
def _audit(conn: Connection, op: str, target: str, meta: str = "") -> None:
    try:
        ins = sa.insert(audit_log).values({"op": op, "object": target, "meta": meta})
        conn.execute(ins)
    except Exception:
        pass


# ---------------------------
# CRUD helpers / queries
# ---------------------------
def list_sheets(conn: Connection) -> List[Dict[str, Any]]:
    res = conn.execute(select(sheets.c.id, sheets.c.name).order_by(sheets.c.created_at.asc()))
    return [{"id": r.id, "name": r.name} for r in res.fetchall()]


def get_or_create_sheet(conn: Connection, name: str) -> int:
    row = conn.execute(select(sheets.c.id).where(sheets.c.name == name)).fetchone()
    if row:
        return int(row.id)
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
    _audit(conn, "swap", "row_order", f"{row_id_a}<->{row_id_b}")


def people_from_labor_code(code: Optional[str]) -> int:
    if not code:
        return 0
    try:
        return int(str(code).split(".")[0])
    except Exception:
        return 0


# ---------------------------
# Atomic UPSERTs (fixes the error you saw)
# ---------------------------
def upsert_cell(conn: Connection, row_id: int, day: int,
                task: Optional[str], hours: Optional[float], labor_code: Optional[str]) -> None:
    """
    Atomic upsert using ON CONFLICT (row_id, day) DO UPDATE.
    Eliminates unique-constraint races and aborted-transaction cascades.
    """
    stmt = pg_insert(day_cells).values(
        row_id=row_id, day=day, task=task, hours=hours, labor_code=labor_code
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[day_cells.c.row_id, day_cells.c.day],
        set_={
            "task": stmt.excluded.task,
            "hours": stmt.excluded.hours,
            "labor_code": stmt.excluded.labor_code,
        },
    )
    conn.execute(stmt)
    _audit(conn, "upsert", "day_cell", f"row={row_id}, day={day}")


def bulk_upsert_cells(conn: Connection, records: List[Dict[str, Any]]) -> None:
    """
    Fast bulk version for Save-to-DB from the grid.
    Each record: {row_id, day, task, hours, labor_code}
    """
    if not records:
        return
    stmt = pg_insert(day_cells).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=[day_cells.c.row_id, day_cells.c.day],
        set_={
            "task": stmt.excluded.task,
            "hours": stmt.excluded.hours,
            "labor_code": stmt.excluded.labor_code,
        },
    )
    conn.execute(stmt)
    _audit(conn, "bulk_upsert", "day_cell", f"n={len(records)}")


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


def get_day_bounds(conn: Connection, sheet_id: int) -> tuple[int, int]:
    r = conn.execute(
        select(func.min(day_cells.c.day), func.max(day_cells.c.day))
        .select_from(day_cells.join(rows, rows.c.id == day_cells.c.row_id))
        .where(rows.c.sheet_id == sheet_id)
    ).fetchone()
    if not r or r[0] is None or r[1] is None:
        return (1, 90)
    return (int(r[0]), int(r[1]))


# ---------------------------
# Import / Export
# ---------------------------

_CANON_SECTIONS = {"Outside", "Ground Floor", "1st Floor", "Roof"}

def _normalize_section(label: str) -> Optional[str]:
    lab = (label or "").strip()
    if lab in _CANON_SECTIONS:
        return lab
    if lab.lower() == "first floor":
        return "1st Floor"
    return None


def import_wide_csv(conn: Connection, csv_path: str, sheet_name: str) -> int:
    df = pd.read_csv(csv_path)
    if df.shape[1] < 2:
        raise ValueError("CSV has too few columns.")

    first_col = df.columns[0]
    cols = list(df.columns)

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

    # Idempotent re-import for this sheet
    conn.execute(delete(day_cells).where(
        day_cells.c.row_id.in_(select(rows.c.id).where(rows.c.sheet_id == sheet_id))
    ))
    conn.execute(delete(rows).where(rows.c.sheet_id == sheet_id))

    current_section: Optional[str] = None
    row_order = 0
    cells_bulk = []

    def _row_has_any_triplet_values(row) -> bool:
        for (dc, tc, lc, _) in triplets:
            for cc in (dc, tc, lc):
                if cc is not None and pd.notna(row.get(cc, None)) and str(row.get(cc, "")).strip() != "":
                    return True
        return False

    def _as_text(v) -> Optional[str]:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        s = str(v).strip()
        return s if s != "" else None

    def _coerce_float(v) -> Optional[float]:
        if v is None:
            return None
        s = str(v).strip().replace(",", "")
        if s == "":
            return None
        try:
            return float(s)
        except Exception:
            return None

    for _, r in df.iterrows():
        label = str(r[first_col]).strip() if pd.notna(r[first_col]) else None
        is_header = not _row_has_any_triplet_values(r)

        if is_header:
            canon = _normalize_section(label or "")
            if canon:
                current_section = canon
            continue

        if current_section is None:
            continue

        subsection = label or ""
        row_order += 1

        row_id = conn.execute(
            rows.insert().values(
                sheet_id=sheet_id, section=current_section, subsection=subsection, row_order=row_order
            ).returning(rows.c.id)
        ).scalar()

        for (dc, tc, lc, dnum) in triplets:
            raw_task  = r.get(dc, None)
            raw_hours = r.get(tc, None) if tc else None
            raw_labor = r.get(lc, None) if lc else None

            task  = _as_text(raw_task)
            hours = _coerce_float(raw_hours)
            labor = _as_text(raw_labor)

            if task is None and hours is None:
                hours_text = _as_text(raw_hours)
                if hours_text is not None and any(ch.isalpha() for ch in hours_text):
                    task = hours_text

            if task is None and hours is None and labor is None:
                continue

            cells_bulk.append({
                "row_id": int(row_id),
                "day": int(dnum),
                "task": task,
                "hours": hours,
                "labor_code": labor,
            })

    if cells_bulk:
        bulk_upsert_cells(conn, cells_bulk)

    return int(sheet_id)


def export_wide_csv(conn: Connection, sheet_id: int, out_path: str) -> None:
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

    days = sorted({dc.day for dc in dcs}) if dcs else []
    max_day = max(days) if days else 0

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


# ---------------------------
# Spreadsheet helpers
# ---------------------------
def delete_cell(conn: Connection, row_id: int, day: int) -> None:
    conn.execute(
        delete(day_cells).where(and_(day_cells.c.row_id == row_id, day_cells.c.day == day))
    )
    _audit(conn, "delete", "day_cell", f"row={row_id}, day={day}")


def fetch_wide_block(conn: Connection, sheet_id: int, section: str,
                     subsection: str, start_day: int, end_day: int) -> pd.DataFrame:
    rws = conn.execute(
        select(rows.c.id, rows.c.subsection, rows.c.row_order)
        .where(and_(rows.c.sheet_id == sheet_id,
                    rows.c.section == section,
                    rows.c.subsection == subsection))
        .order_by(rows.c.row_order.asc())
    ).fetchall()
    if not rws:
        return pd.DataFrame()

    row_ids = [r.id for r in rws]
    dcs = conn.execute(
        select(day_cells.c.row_id, day_cells.c.day, day_cells.c.task,
               day_cells.c.hours, day_cells.c.labor_code)
        .where(and_(day_cells.c.row_id.in_(row_ids),
                    day_cells.c.day >= start_day,
                    day_cells.c.day <= end_day))
        .order_by(day_cells.c.row_id.asc(), day_cells.c.day.asc())
    ).fetchall()

    by_rd = {(c.row_id, c.day): c for c in dcs}
    data = []
    for r in rws:
        row_obj = {"RowID": int(r.id), "Subsection": r.subsection or ""}
        for d in range(start_day, end_day + 1):
            c = by_rd.get((r.id, d))
            row_obj[f"Day {d}"]   = (c.task if c else None)
            row_obj[f"Time {d}"]  = (float(c.hours) if c and c.hours is not None else None)
            row_obj[f"Labor {d}"] = (c.labor_code if c else None)
        data.append(row_obj)

    cols = ["RowID", "Subsection"]
    for d in range(start_day, end_day + 1):
        cols.extend([f"Day {d}", f"Time {d}", f"Labor {d}"])
    return pd.DataFrame(data, columns=cols)

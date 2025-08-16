# app.py
from __future__ import annotations

import os, glob, re, time
import pandas as pd
import sqlalchemy as sa
import streamlit as st

from db import (
    get_engine, init_db, list_sheets, get_or_create_sheet, get_sections, get_subsections,
    get_rows_for_subsection, swap_row_order, upsert_cell, read_cell_preview,
    import_wide_csv, export_wide_csv, people_from_labor_code
)

st.set_page_config(page_title="Fast Construction Editor (DB-backed)", layout="wide")

# ---------- DB ----------
engine = get_engine()
init_db(engine)

# ---------- Helpers ----------
def _find_latest_csv(data_dir: str) -> str | None:
    csvs = glob.glob(os.path.join(data_dir, "*.csv"))
    if not csvs:
        return None

    def _day_from_name(p: str) -> int:
        m = re.search(r"day\s*(\d+)", os.path.basename(p), re.IGNORECASE)
        return int(m.group(1)) if m else -1

    # Prefer highest "DayN" in filename, else newest mtime
    csvs.sort(key=lambda p: (_day_from_name(p), os.path.getmtime(p)), reverse=True)
    return csvs[0]

def _ensure_one_sheet(engine) -> int | None:
    """If no sheets exist in DB, auto-import newest CSV from ./data. Return a sheet_id or None."""
    with engine.begin() as conn:
        sheets = list_sheets(conn)
        if sheets:
            return sheets[0]["id"]

    data_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(data_dir, exist_ok=True)
    candidate = _find_latest_csv(data_dir)
    if not candidate:
        # No DB sheets and no CSV seed — user can still upload manually
        return None

    with st.spinner(f"Importing seed CSV {os.path.basename(candidate)} into Postgres..."):
        with engine.begin() as conn:
            sheet_id = import_wide_csv(conn, candidate, sheet_name=os.path.basename(candidate))
    st.sidebar.success(f"Imported {os.path.basename(candidate)} → sheet #{sheet_id}.")
    return sheet_id

# Try auto-import if DB empty
seed_sheet_id = _ensure_one_sheet(engine)

# ---------- Sidebar ----------
with st.sidebar:
    st.header("Data")

    if st.button("Test DB connection"):
        try:
            with engine.connect() as c:
                c.execute(sa.text("select 1"))
            st.success("DB connection OK ✅")
        except Exception as e:
            st.error(f"DB connection failed: {e}")

    # Always show sheets and upload UI
    with engine.begin() as conn:
        _sheets = list_sheets(conn)

    active_sheet_id_default = seed_sheet_id if seed_sheet_id is not None else (_sheets[0]["id"] if _sheets else None)
    active_sheet_id = st.selectbox(
        "Active sheet",
        options=[s["id"] for s in _sheets] if _sheets else [],
        format_func=lambda sid: next((s["name"] for s in _sheets if s["id"] == sid), str(sid)),
        index=0 if active_sheet_id_default is None and _sheets else
              ([s["id"] for s in _sheets].index(active_sheet_id_default) if (_sheets and active_sheet_id_default in [s["id"] for s in _sheets]) else 0) if _sheets else None
    )

    st.subheader("Import/Export")

    # Upload (optional, becomes a new sheet)
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(data_dir, exist_ok=True)
    uploaded = st.file_uploader("Import wide CSV", type=["csv"], accept_multiple_files=False)
    if st.button("Import CSV into DB", disabled=uploaded is None):
        path = os.path.join(data_dir, f"import_{int(time.time())}.csv")
        with open(path, "wb") as f:
            f.write(uploaded.getbuffer())
        with engine.begin() as conn:
            new_sheet_id = import_wide_csv(conn, path, sheet_name=uploaded.name)
        st.success(f"Imported into sheet #{new_sheet_id} ({uploaded.name}). Select it above.")
        # Refresh list & select the newly created sheet
        with engine.begin() as conn:
            _sheets = list_sheets(conn)
        active_sheet_id = new_sheet_id

    if active_sheet_id:
        if st.button("Export current sheet as CSV"):
            out_path = os.path.join(data_dir, f"export_{active_sheet_id}_{int(time.time())}.csv")
            with engine.begin() as conn:
                export_wide_csv(conn, active_sheet_id, out_path)
            with open(out_path, "rb") as f:
                st.download_button("Download CSV", f, file_name=os.path.basename(out_path), mime="text/csv")

st.title("🧱 Fast Construction Editor (DB-backed)")
st.caption("Quick cell entry, labor×hours cost, and row reordering. Data persisted in PostgreSQL via secrets.")

# If still no sheets and no seed CSV, stop here (user can upload in the sidebar)
if not _sheets:
    st.info("No sheets yet. Upload a CSV in the sidebar, or add one to ./data and refresh.")
    st.stop()

# ---------- Main Editor ----------
with engine.begin() as conn:
    section_names = get_sections(conn, active_sheet_id)

cA, cB = st.columns(2)
section = cA.selectbox("Section", section_names, index=0 if section_names else None)
with engine.begin() as conn:
    subsections = get_subsections(conn, active_sheet_id, section) if section else []
subsection = cB.selectbox("Subsection", subsections, index=0 if subsections else None)

# Select a row within that subsection
row_choices, row_ids = [], []
with engine.begin() as conn:
    if section and subsection:
        rws = get_rows_for_subsection(conn, active_sheet_id, section, subsection)
        for r in rws:
            prev = read_cell_preview(conn, r["id"])
            row_choices.append(f"Row#{r['row_order']} – {prev}")
            row_ids.append(r["id"])
row_label = st.selectbox("Row", row_choices, index=0 if row_choices else None)
row_id = row_ids[row_choices.index(row_label)] if row_choices else None

st.markdown("### Place a value")
c1, c2, c3 = st.columns(3)
day = c1.number_input("Day", min_value=1, value=1, step=1)
task = c1.text_input("Task name", value="")
hours = c2.number_input("Time (hours)", min_value=0.0, value=0.0, step=0.5, format="%.1f")
people = c2.number_input("# people (from labor code)", min_value=0, value=0, step=1)
group_id = c3.number_input("Group ID (e.g., 6 for '.06')", min_value=0, value=6, step=1)
labor_code = c3.text_input("Labor code", value=f"{people}.{group_id:02d}")

rate = st.number_input("Rate per person-hour", min_value=0.0, value=0.0, step=50.0)
if rate and (hours or labor_code):
    ppl = people_from_labor_code(labor_code)
    lh = ppl * float(hours or 0)
    st.caption(f"Labor-hours: **{lh:.1f}**  |  People: **{ppl}**  |  Cost @ rate: **{(lh*rate):,.2f}**")

cL, cR = st.columns(2)
if cL.button("Apply to this Day", type="primary", disabled=row_id is None):
    with engine.begin() as conn:
        upsert_cell(conn, row_id, int(day),
                    task if task.strip() else None,
                    float(hours) if hours > 0 else None,
                    labor_code.strip() if labor_code.strip() else None)
    st.success("Saved.")

start_day = cR.number_input("Range start", min_value=1, value=1, step=1)
end_day = cR.number_input("Range end", min_value=max(1, int(start_day)), value=int(start_day), step=1)
if st.button("Apply to range", disabled=row_id is None):
    a, b = sorted((int(start_day), int(end_day)))
    with engine.begin() as conn:
        for d in range(a, b+1):
            upsert_cell(conn, row_id, d,
                        task if task.strip() else None,
                        float(hours) if hours > 0 else None,
                        labor_code.strip() if labor_code.strip() else None)
    st.success(f"Saved Days {a}–{b}.")

st.markdown("### Reorder rows (within this subsection)")
u, d, _ = st.columns(3)
with engine.begin() as conn:
    rws = get_rows_for_subsection(conn, active_sheet_id, section, subsection) if (section and subsection) else []
if rws and row_id is not None:
    ids = [r["id"] for r in rws]
    idx = ids.index(row_id)
    if u.button("Move ↑", disabled=(idx == 0)):
        with engine.begin() as conn:
            swap_row_order(conn, ids[idx-1], ids[idx])
        st.experimental_rerun()
    if d.button("Move ↓", disabled=(idx == len(ids)-1)):
        with engine.begin() as conn:
            swap_row_order(conn, ids[idx], ids[idx+1])
        st.experimental_rerun()

st.divider()
st.subheader("Tips")
st.write("- Labor code format **P.GG** → **people = P** (e.g., `7.06` → 7 people).")
st.write("- Use **Apply to range** to fill multiple days in one go.")
st.write("- Export CSV anytime for sharing; exported layout matches the wide sheet.")

from __future__ import annotations

import os, glob, re, time
import sqlalchemy as sa
import streamlit as st

from db import (
    get_engine, init_db, list_sheets, get_sections, get_subsections,
    get_rows_for_subsection, swap_row_order, upsert_cell, read_cell_preview,
    import_wide_csv, export_wide_csv, people_from_labor_code, rows,
    fetch_wide_block, delete_cell, get_day_bounds
)

st.set_page_config(page_title="Fast Construction Editor (DB-backed)", layout="wide")


# ---------------------------
# DB init
# ---------------------------
engine = get_engine()
init_db(engine)


# ---------------------------
# Helpers
# ---------------------------
def _find_latest_csv(data_dir: str) -> str | None:
    csvs = glob.glob(os.path.join(data_dir, "*.csv"))
    if not csvs:
        return None

    def _day_from_name(p: str) -> int:
        m = re.search(r"day\s*(\d+)", os.path.basename(p), re.IGNORECASE)
        return int(m.group(1)) if m else -1

    # Prefer highest DayN; else newest modified
    csvs.sort(key=lambda p: (_day_from_name(p), os.path.getmtime(p)), reverse=True)
    return csvs[0]


def _ensure_one_sheet(engine) -> int | None:
    """If DB has no usable data, auto-import newest CSV from ./data and return its id."""
    with engine.begin() as conn:
        _sheets = list_sheets(conn)
        total_rows = conn.execute(sa.select(sa.func.count()).select_from(rows)).scalar() or 0
        if _sheets and total_rows > 0:
            return _sheets[0]["id"]

    data_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(data_dir, exist_ok=True)
    candidate = _find_latest_csv(data_dir)
    if not candidate:
        return None

    with st.status(f"Importing seed CSV **{os.path.basename(candidate)}** into Postgres…", expanded=True) as status:
        try:
            with engine.begin() as conn:
                sheet_id = import_wide_csv(conn, candidate, sheet_name=os.path.basename(candidate))
            status.update(state="complete", label=f"Imported {os.path.basename(candidate)} → sheet #{sheet_id}")
            return sheet_id
        except Exception as e:
            status.update(state="error", label="Import failed")
            st.exception(e)
            return None


# If empty, do the one-time auto-import
seed_sheet_id = _ensure_one_sheet(engine)


# ---------------------------
# Sidebar: data + upload
# ---------------------------
with st.sidebar:
    st.header("Data")

    if st.button("Test DB connection"):
        try:
            with engine.connect() as c:
                c.execute(sa.text("select 1"))
            st.success("DB connection OK ✅")
        except Exception as e:
            st.error(f"DB connection failed: {e}")

    # Debug counts (instant truth)
    with engine.begin() as conn:
        counts = conn.execute(
            sa.text("select "
                    "(select count(*) from sheets) as sheets_ct, "
                    "(select count(*) from rows) as rows_ct, "
                    "(select count(*) from day_cells) as cells_ct")
        ).mappings().first()
    with st.expander("Debug: table counts", expanded=False):
        st.write(counts)

    with engine.begin() as conn:
        _sheets = list_sheets(conn)

    default_sheet = seed_sheet_id if seed_sheet_id is not None else (_sheets[0]["id"] if _sheets else None)
    active_sheet_id = st.selectbox(
        "Active sheet",
        options=[s["id"] for s in _sheets] if _sheets else [],
        format_func=lambda sid: next((s["name"] for s in _sheets if s["id"] == sid), str(sid)),
        index=( [s["id"] for s in _sheets].index(default_sheet) if (_sheets and default_sheet in [s["id"] for s in _sheets]) else 0 ) if _sheets else None,
    )

    st.subheader("Import/Export")

    # Upload UI (optional; creates a NEW sheet)
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(data_dir, exist_ok=True)
    uploaded = st.file_uploader("Import wide CSV", type=["csv"], accept_multiple_files=False)
    if st.button("Import CSV into DB", disabled=uploaded is None):
        path = os.path.join(data_dir, f"import_{int(time.time())}.csv")
        with open(path, "wb") as f:
            f.write(uploaded.getbuffer())
        try:
            with st.status("Importing uploaded CSV…", expanded=True) as status:
                with engine.begin() as conn:
                    new_sheet_id = import_wide_csv(conn, path, sheet_name=uploaded.name)
                status.update(state="complete", label=f"Imported {uploaded.name} → sheet #{new_sheet_id}")
            with engine.begin() as conn:
                _sheets = list_sheets(conn)
            active_sheet_id = new_sheet_id
        except Exception as e:
            st.exception(e)

    if active_sheet_id and st.button("Export current sheet as CSV"):
        out_path = os.path.join(data_dir, f"export_{active_sheet_id}_{int(time.time())}.csv")
        with engine.begin() as conn:
            export_wide_csv(conn, active_sheet_id, out_path)
        with open(out_path, "rb") as f:
            st.download_button("Download CSV", f, file_name=os.path.basename(out_path), mime="text/csv")


# If no sheets (no seed + nothing uploaded), stop early
if not _sheets:
    st.title("🧱 Fast Construction Editor (DB-backed)")
    st.info("No sheets yet. Upload a CSV in the sidebar, or add one in ./data and refresh.")
    st.stop()


# ---------------------------
# Main UI: Quick editor + Spreadsheet
# ---------------------------
st.title("🧱 Fast Construction Editor (DB-backed)")
st.caption("Quick cell entry, labor×hours cost, row reordering, and an Excel-like grid. Data is persisted to PostgreSQL via Streamlit Secrets.")

with engine.begin() as conn:
    sections = get_sections(conn, active_sheet_id)

cA, cB = st.columns(2)
section = cA.selectbox("Section", sections, index=0 if sections else None)

with engine.begin() as conn:
    subs = get_subsections(conn, active_sheet_id, section) if section else []
subsection = cB.selectbox("Subsection", subs, index=0 if subs else None)

tab_quick, tab_sheet = st.tabs(["Quick editor", "Spreadsheet view"])

# =======================
# Quick Editor
# =======================
with tab_quick:
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
    hours = c2.number_input("Time (hours)", min_value=0.0, value=0.0, step=0.5, format="%.1f")
    group_id = c3.number_input("Group ID (e.g., 6 for '.06')", min_value=0, value=6, step=1)

    task = c1.text_input("Task name", value="")
    people = c2.number_input("people (from labor code)", min_value=0, value=0, step=1)
    labor_code = c3.text_input("Labor code", value=f"{people}.{group_id:02d}")

    rate = st.number_input("Rate per person-hour", min_value=0.0, value=0.0, step=50.0)
    ppl = people_from_labor_code(labor_code)
    if rate or hours or labor_code:
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
            for d in range(a, b + 1):
                upsert_cell(conn, row_id, d,
                            task if task.strip() else None,
                            float(hours) if hours > 0 else None,
                            labor_code.strip() if labor_code.strip() else None)
        st.success(f"Saved Days {a}–{b}.")

    st.markdown("### Reorder rows (within this subsection)")
    u, dwn, _ = st.columns(3)
    with engine.begin() as conn:
        rws = get_rows_for_subsection(conn, active_sheet_id, section, subsection) if (section and subsection) else []
    if rws and row_id is not None:
        ids = [r["id"] for r in rws]
        idx = ids.index(row_id)
        if u.button("Move ↑", disabled=(idx == 0)):
            with engine.begin() as conn:
                swap_row_order(conn, ids[idx - 1], ids[idx])
            st.rerun()
        if dwn.button("Move ↓", disabled=(idx == len(ids) - 1)):
            with engine.begin() as conn:
                swap_row_order(conn, ids[idx], ids[idx + 1])
            st.rerun()


# =======================
# Spreadsheet View
# =======================
with tab_sheet:
    st.subheader("Excel-like grid")

    # day bounds for the sheet (fallback to 1..90)
    with engine.begin() as conn:
        min_day, max_day = get_day_bounds(conn, active_sheet_id)

    win = st.slider("Day range", min_value=min_day, max_value=max(max_day, min_day),
                    value=(min_day, min(min_day + 13, max_day)), step=1)

    if section and subsection:
        with engine.begin() as conn:
            sheet_df = fetch_wide_block(conn, active_sheet_id, section, subsection, win[0], win[1])

        if sheet_df.empty:
            st.info("No rows for this subsection yet.")
        else:
            cfg = {}
            for d in range(win[0], win[1] + 1):
                cfg[f"Day {d}"]   = st.column_config.TextColumn(width="medium")
                cfg[f"Time {d}"]  = st.column_config.NumberColumn(format="%.1f", width="small")
                cfg[f"Labor {d}"] = st.column_config.TextColumn(width="small")

            edited = st.data_editor(
                sheet_df,
                hide_index=True,
                use_container_width=True,
                num_rows="fixed",
                disabled=("RowID", "Subsection"),
                column_config=cfg,
                key="sheet_editor",
            )

            if st.button("Save changes to DB", type="primary"):
                with engine.begin() as conn:
                    for _, row in edited.iterrows():
                        rid = int(row["RowID"])
                        for d in range(win[0], win[1] + 1):
                            task  = row.get(f"Day {d}")
                            hours = row.get(f"Time {d}")
                            labor = row.get(f"Labor {d}")

                            # normalize empties
                            task  = (str(task).strip()  if task  not in (None, "", float("nan")) else None)
                            labor = (str(labor).strip() if labor not in (None, "", float("nan")) else None)
                            try:
                                hours = float(hours) if hours not in (None, "", float("nan")) else None
                            except Exception:
                                hours = None

                            if task is None and hours is None and labor is None:
                                delete_cell(conn, rid, d)
                            else:
                                upsert_cell(conn, rid, d, task, hours, labor)

                st.success("Saved. You can switch the day window and continue editing.")

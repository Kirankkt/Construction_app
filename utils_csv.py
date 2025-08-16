from __future__ import annotations
import re
from typing import Optional, List, Tuple
import pandas as pd

DAY_COL_PATTERN = re.compile(r"^Day\s*(\d+)$", re.IGNORECASE)

def detect_day_triplets(columns: List[str]) -> List[tuple[str, Optional[str], Optional[str], int, int]]:
    days = []
    for i, c in enumerate(columns):
        m = DAY_COL_PATTERN.match(str(c).strip())
        if m:
            dnum = int(m.group(1))
            time_col = columns[i+1] if i+1 < len(columns) else None
            labor_col = columns[i+2] if i+2 < len(columns) else None
            days.append((c, time_col, labor_col, dnum, i))
    return days

def max_day_from_columns(columns: List[str]) -> int:
    m = 0
    for c in columns:
        mm = DAY_COL_PATTERN.match(str(c).strip())
        if mm:
            m = max(m, int(mm.group(1)))
    return m if m else 1

def build_labor_code(people: int, group_id: int) -> str:
    people = max(0, int(people)); group_id = max(0, int(group_id))
    return f"{people}.{group_id:02d}"

def parse_people_from_labor_code(code: Optional[str]) -> int:
    if not code: return 0
    try:
        return int(str(code).split(".")[0])
    except Exception:
        return 0

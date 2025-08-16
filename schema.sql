-- Minimal normalized schema for fast editing

CREATE TABLE IF NOT EXISTS sheets (
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rows (
  id SERIAL PRIMARY KEY,
  sheet_id INTEGER NOT NULL REFERENCES sheets(id) ON DELETE CASCADE,
  section TEXT,
  subsection TEXT,
  row_order INTEGER NOT NULL,
  UNIQUE(sheet_id, row_order)
);

CREATE INDEX IF NOT EXISTS idx_rows_sheet_subsection ON rows(sheet_id, subsection);

CREATE TABLE IF NOT EXISTS day_cells (
  id SERIAL PRIMARY KEY,
  row_id INTEGER NOT NULL REFERENCES rows(id) ON DELETE CASCADE,
  day INTEGER NOT NULL,
  task TEXT,
  hours DOUBLE PRECISION,
  labor_code TEXT,
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(row_id, day)
);

CREATE INDEX IF NOT EXISTS idx_cells_row_day ON day_cells(row_id, day);

CREATE TABLE IF NOT EXISTS audit_log (
  id BIGSERIAL PRIMARY KEY,
  who TEXT,
  action TEXT NOT NULL,
  payload JSONB,
  ts TIMESTAMP DEFAULT NOW()
);

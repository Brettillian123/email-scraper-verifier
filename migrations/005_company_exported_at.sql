-- Add exported_at column to companies table for CSV export tracking.
ALTER TABLE companies ADD COLUMN IF NOT EXISTS exported_at TIMESTAMPTZ DEFAULT NULL;

-- migrations/003_email_verification_code.sql
--
-- Adds a short numeric verification code to email_verification_tokens.
-- The existing `id` (UUID) column remains as the primary key / DB identifier,
-- while the new `code` column holds the 6-digit code shown to the user.
--
-- Also adds an attempt counter to prevent brute-force guessing.
--
-- Idempotent: safe to run multiple times.

-- 6-digit numeric code the user enters
ALTER TABLE email_verification_tokens
  ADD COLUMN IF NOT EXISTS code TEXT;

-- Track failed verification attempts (lockout after N bad guesses)
ALTER TABLE email_verification_tokens
  ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;

-- Index for fast code lookups during verification
CREATE INDEX IF NOT EXISTS idx_email_verify_code
  ON email_verification_tokens(user_id, code);

-- migrations/002_user_approval.sql
--
-- Add user approval workflow
-- New users must be approved by an admin before accessing the app

-- Add is_approved column (default FALSE for new registrations)
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_approved BOOLEAN NOT NULL DEFAULT FALSE;

-- Create index for quick lookups
CREATE INDEX IF NOT EXISTS idx_users_is_approved ON users(is_approved);

-- Approve all existing users (so current users aren't locked out)
UPDATE users SET is_approved = TRUE WHERE is_approved = FALSE;

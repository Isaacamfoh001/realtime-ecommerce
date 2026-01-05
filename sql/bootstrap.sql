DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'realtime_user') THEN
    CREATE ROLE realtime_user LOGIN PASSWORD 'realtime_pass';
  END IF;
END
$$;

CREATE DATABASE realtime_ecommerce OWNER realtime_user;

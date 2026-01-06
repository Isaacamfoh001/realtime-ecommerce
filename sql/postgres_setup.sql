CREATE TABLE IF NOT EXISTS ecommerce_events (
 
  event_id TEXT PRIMARY KEY,
  event_time TIMESTAMPTZ NOT NULL,
  stored_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  user_id INTEGER NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('view', 'purchase')),
  product_id INTEGER NOT NULL,
  product_name TEXT,
  category TEXT,
  price NUMERIC(10, 2),
  quantity INTEGER,

  source TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_event_time ON ecommerce_events (event_time);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON ecommerce_events (user_id);
CREATE INDEX IF NOT EXISTS idx_events_product_id ON ecommerce_events (product_id);
CREATE INDEX IF NOT EXISTS idx_events_action ON ecommerce_events (action);

GRANT ALL PRIVILEGES ON TABLE ecommerce_events TO realtime_user;

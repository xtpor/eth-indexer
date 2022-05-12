
CREATE TABLE log(
  id TEXT NOT NULL PRIMARY KEY,
  insertion_id TEXT UNIQUE NOT NULL,
  block_number INTEGER NOT NULL,
  transaction_index INTEGER NOT NULL,
  log_index INTEGER NOT NULL,
  transaction_hash TEXT NOT NULL,
  block_hash TEXT NOT NULL,
  removed BOOLEAN NOT NULL,
  address TEXT NOT NULL,
  data TEXT NOT NULL,
  topic_0 TEXT,
  topic_1 TEXT,
  topic_2 TEXT,
  topic_3 TEXT
);

CREATE INDEX address_idx ON log(address);

CREATE TABLE checkpoint(
  lock BOOLEAN PRIMARY KEY DEFAULT (TRUE) CHECK (lock = TRUE),
  downloaded_lower_bound INTEGER NOT NULL,
  downloaded_upper_bound INTEGER NOT NULL,
  published_lower_bound INTEGER NOT NULL,
  published_upper_bound INTEGER NOT NULL
);
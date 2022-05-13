
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

CREATE TABLE completed_range(
  from_block INTEGER NOT NULL,
  to_block INTEGER NOT NULL,

  CHECK (from_block <= to_block),
  PRIMARY KEY (from_block, to_block)
);

CREATE TABLE locked_range(
  instance_id TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  from_block INTEGER NOT NULL,
  to_block INTEGER NOT NULL,

  CHECK (from_block <= to_block)
);
-- +goose Up
-- Migration to create the ff_task_metadata table and its index.

CREATE TABLE IF NOT EXISTS ff_task_metadata (
    key                     VARCHAR(2048) NOT NULL PRIMARY KEY,
    value                   TEXT,
    marked_for_deletion_at  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ff_key_pattern
    ON ff_task_metadata (key text_pattern_ops);

-- +goose Down
-- Migration to drop the ff_task_metadata table and its index.

DROP INDEX IF EXISTS ff_key_pattern;
DROP TABLE IF EXISTS ff_task_metadata;
-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS ff_ordered_id (
    namespace VARCHAR(2048) PRIMARY KEY,
    current_id BIGINT
);

CREATE TABLE IF NOT EXISTS ff_task_metadata (
    key VARCHAR(2048) PRIMARY KEY,
    value TEXT
);

CREATE INDEX ff_task_metadata_key_pattern_idx ON ff_task_metadata (key text_pattern_ops);

CREATE TABLE IF NOT EXISTS ff_locks (
    owner VARCHAR(255),
    key VARCHAR(2048) NOT NULL,
    expiration TIMESTAMP NOT NULL,
    PRIMARY KEY (key)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS ff_locks;
DROP TABLE IF EXISTS ff_task_metadata;
DROP TABLE IF EXISTS ff_ordered_id;
-- +goose StatementEnd
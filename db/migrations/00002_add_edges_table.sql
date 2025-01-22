-- +goose Up
-- +goose StatementBegin

-- Create edges table
CREATE TABLE edges (
    from_resource_type_int INTEGER NOT NULL,       -- This is the proto ID
    from_resource_name VARCHAR(255) NOT NULL,
    from_resource_variant VARCHAR(255) NOT NULL,
    to_resource_type_int INTEGER NOT NULL,         -- This is the proto ID
    to_resource_name VARCHAR(255) NOT NULL,
    to_resource_variant VARCHAR(255) NOT NULL,
    PRIMARY KEY (
        from_resource_type_int,
        from_resource_name,
        from_resource_variant,
        to_resource_type_int,
        to_resource_name,
        to_resource_variant
    )
);

-- Create index on 'from_resource' columns
CREATE INDEX idx_from_resource ON edges (
    from_resource_type_int,
    from_resource_name,
    from_resource_variant
);

-- Create index on 'to_resource' columns
CREATE INDEX idx_to_resource ON edges (
    to_resource_type_int,
    to_resource_name,
    to_resource_variant
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Drop indexes if they exist
DROP INDEX IF EXISTS idx_from_resource;
DROP INDEX IF EXISTS idx_to_resource;

-- Drop edges table if it exists
DROP TABLE IF EXISTS edges;

-- +goose StatementEnd
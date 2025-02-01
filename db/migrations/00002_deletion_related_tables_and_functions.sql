-- +goose Up
-- +goose StatementBegin

ALTER TABLE ff_task_metadata
ADD COLUMN marked_for_deletion_at TIMESTAMP DEFAULT null;

-- Create enum type for resource components
CREATE TYPE resource_component AS ENUM ('type', 'name', 'variant');

-- Function: parse_resource_key
CREATE FUNCTION parse_resource_key(
    resource_key text,
    component resource_component
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    parts text[];
BEGIN
    -- Split the key by '__' and ensure proper array length
    parts := string_to_array(resource_key, '__');
    IF array_length(parts, 1) < 2 THEN
        RAISE EXCEPTION 'Invalid resource key format: %. Expected at least 2 parts separated by "__"', resource_key;
    END IF;

    CASE component
        WHEN 'type' THEN
            RETURN parts[1];
        WHEN 'name' THEN
            RETURN parts[2];
        WHEN 'variant' THEN
            RETURN CASE WHEN array_length(parts, 1) >= 3 THEN parts[3] ELSE NULL END;
    END CASE;
END;
$$;

-- Function: create_resource_key
CREATE FUNCTION create_resource_key(resource_type integer, resource_name text, resource_variant text)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    resource_type_name TEXT;
BEGIN
    CASE resource_type
        WHEN 0 THEN resource_type_name := 'FEATURE';
        WHEN 1 THEN resource_type_name := 'LABEL';
        WHEN 2 THEN resource_type_name := 'TRAINING_SET';
        WHEN 3 THEN resource_type_name := 'SOURCE';
        WHEN 4 THEN resource_type_name := 'FEATURE_VARIANT';
        WHEN 5 THEN resource_type_name := 'LABEL_VARIANT';
        WHEN 6 THEN resource_type_name := 'TRAINING_SET_VARIANT';
        WHEN 7 THEN resource_type_name := 'SOURCE_VARIANT';
        WHEN 8 THEN resource_type_name := 'PROVIDER';
        WHEN 9 THEN resource_type_name := 'ENTITY';
        WHEN 10 THEN resource_type_name := 'MODEL';
        WHEN 11 THEN resource_type_name := 'USER';
        ELSE RAISE EXCEPTION 'Invalid resource_type: %', resource_type;
    END CASE;

    IF resource_variant IS NOT NULL AND resource_variant <> '' THEN
        RETURN CONCAT(resource_type_name, '__', resource_name, '__', resource_variant);
    ELSE
        RETURN CONCAT(resource_type_name, '__', resource_name, '__');
    END IF;
END;
$$;

-- Function: add_edge
CREATE FUNCTION add_edge(
    from_type integer,
    from_name text,
    from_variant text,
    to_type integer,
    to_name text,
    to_variant text
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Check if the key exists and is not marked for deletion
    IF EXISTS (
        SELECT 1
        FROM ff_task_metadata
        WHERE key = create_resource_key(from_type, from_name, from_variant)
    ) THEN
        -- Check if the key is marked for deletion
        IF EXISTS (
            SELECT 1
            FROM ff_task_metadata
            WHERE key = create_resource_key(from_type, from_name, from_variant)
              AND marked_for_deletion_at IS NULL
        ) THEN
            -- Insert the edge if the key is valid and not marked for deletion
            INSERT INTO edges (
                from_resource_proto_type, from_resource_name, from_resource_variant,
                to_resource_proto_type, to_resource_name, to_resource_variant
            )
            VALUES (
                from_type, from_name, from_variant,
                to_type, to_name, to_variant
            )
            ON CONFLICT DO NOTHING;
        ELSE
            RAISE EXCEPTION 'Cannot insert edge because key % is marked as deleted in ff_task_metadata',
                create_resource_key(from_type, from_name, from_variant);
        END IF;
    ELSE
        RAISE EXCEPTION 'Cannot insert edge because key % does not exist in ff_task_metadata',
            create_resource_key(from_type, from_name, from_variant);
    END IF;
END;
$$;

-- Function: process_feature_variant
CREATE FUNCTION process_feature_variant(feature_variant_key text, feature_variant_value text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    feature_type      INT := 4;  -- Resource type for feature_variant
    provider_type     INT := 8;  -- Resource type for provider
    training_set_type INT := 6;  -- Resource type for training_set_variant
    source_type       INT := 7;  -- Resource type for source_variant
    message           JSONB;     -- Parsed Message JSON
    item              JSONB;     -- Individual item in the array
BEGIN
    message := (feature_variant_value::jsonb ->> 'Message')::jsonb;

    -- Add an edge from FEATURE_VARIANT to its provider
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            feature_type,
            parse_resource_key(feature_variant_key, 'name'),
            parse_resource_key(feature_variant_key, 'variant')
        );
    END IF;

    -- Add an edge from FEATURE_VARIANT to its source
    IF message -> 'source' IS NOT NULL THEN
        PERFORM add_edge(
            source_type,
            ((message -> 'source')::jsonb) ->> 'name',
            ((message -> 'source')::jsonb) ->> 'variant',
            feature_type,
            parse_resource_key(feature_variant_key, 'name'),
            parse_resource_key(feature_variant_key, 'variant')
        );
    END IF;
END;
$$;

-- Function: process_label_variant
CREATE FUNCTION process_label_variant(label_variant_key text, label_variant_value text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    label_type        INT := 5;  -- Resource type for label_variant
    source_type       INT := 7;  -- Resource type for source_variant
    training_set_type INT := 6;  -- Resource type for training_set_variant
    provider_type     INT := 8;  -- Resource type for provider
    message           JSONB;     -- Parsed Message JSON
    item              JSONB;     -- Individual item in JSON arrays
BEGIN
    message := (label_variant_value::jsonb ->> 'Message')::jsonb;

    -- Add edge to provider
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            label_type,
            parse_resource_key(label_variant_key, 'name'),
            parse_resource_key(label_variant_key, 'variant')
        );
    END IF;

    -- Add edge to source
    IF message -> 'source' IS NOT NULL THEN
        PERFORM add_edge(
            source_type,
            ((message -> 'source')::jsonb) ->> 'name',
            ((message -> 'source')::jsonb) ->> 'variant',
            label_type,
            parse_resource_key(label_variant_key, 'name'),
            parse_resource_key(label_variant_key, 'variant')
        );
    END IF;
END;
$$;

-- Function: process_source_variant
CREATE FUNCTION process_source_variant(
    source_variant_key text,
    source_variant_value text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    source_type       INT := 7;  -- Resource type for source_variant
    provider_type     INT := 8;  -- Resource type for provider
    feature_type      INT := 4;  -- Resource type for feature_variant
    training_set_type INT := 6;  -- Resource type for training_set_variant
    label_type        INT := 5;  -- Resource type for label_variant
    message           JSONB;     -- Parsed Message JSON
    item              JSONB;     -- Individual item in lists
BEGIN
    message := (source_variant_value::jsonb ->> 'Message')::jsonb;

    -- Add an edge from SOURCE_VARIANT to its provider
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            source_type,
            parse_resource_key(source_variant_key, 'name'),
            parse_resource_key(source_variant_key, 'variant')
        );
    END IF;

    -- Process the "source" array in SQLTransformation or DFTransformation
    IF message -> 'transformation' -> 'SQLTransformation' -> 'source' IS NOT NULL THEN
        FOR item IN SELECT jsonb_array_elements(message -> 'transformation' -> 'SQLTransformation' -> 'source')
        LOOP
            PERFORM add_edge(
                source_type,
                item ->> 'name',
                item ->> 'variant',
                source_type,
                parse_resource_key(source_variant_key, 'name'),
                parse_resource_key(source_variant_key, 'variant')
            );
        END LOOP;
    ELSIF message -> 'transformation' -> 'DFTransformation' -> 'inputs' IS NOT NULL THEN
        FOR item IN SELECT jsonb_array_elements(message -> 'transformation' -> 'DFTransformation' -> 'inputs')
        LOOP
            PERFORM add_edge(
                source_type,
                item ->> 'name',
                item ->> 'variant',
                source_type,
                parse_resource_key(source_variant_key, 'name'),
                parse_resource_key(source_variant_key, 'variant')
            );
        END LOOP;
    END IF;
END;
$$;

-- Function: process_ts_variant
CREATE FUNCTION process_ts_variant(
    ts_key TEXT,
    ts_value TEXT
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    ts_type       INT := 6;  -- Resource type for TS_VARIANT
    provider_type INT := 8;  -- Resource type for provider
    label_type    INT := 5;  -- Resource type for label_variant
    feature_type  INT := 4;  -- Resource type for feature_variant
    message       JSONB;     -- Parsed Message JSON
    item          JSONB;     -- Individual item in lists
BEGIN
    message := (ts_value::jsonb ->> 'Message')::jsonb;

    -- Add an edge from TS_VARIANT to its provider
    IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM add_edge(
            provider_type,
            message ->> 'provider',
            '',
            ts_type,
            parse_resource_key(ts_key, 'name'),
            parse_resource_key(ts_key, 'variant')
        );
    END IF;

    -- Add an edge from TS_VARIANT to its label
    IF message ->> 'label' IS NOT NULL THEN
        PERFORM add_edge(
            label_type,
            ((message ->> 'label')::jsonb) ->> 'name',
            ((message ->> 'label')::jsonb) ->> 'variant',
            ts_type,
            parse_resource_key(ts_key, 'name'),
            parse_resource_key(ts_key, 'variant')
        );
    END IF;

    -- Add edges for features
    IF message -> 'features' IS NOT NULL THEN
        FOR item IN SELECT jsonb_array_elements(message -> 'features')
        LOOP
            PERFORM add_edge(
                feature_type,
                item ->> 'name',
                item ->> 'variant',
                ts_type,
                parse_resource_key(ts_key, 'name'),
                parse_resource_key(ts_key, 'variant')
            );
        END LOOP;
    END IF;
END;
$$;

-- Function: add_edge_from_ff_task_metadata_row
CREATE FUNCTION add_edge_from_ff_task_metadata_row(key TEXT, value TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF key LIKE 'FEATURE_VARIANT__%' THEN
        PERFORM process_feature_variant(key, value);
    ELSIF key LIKE 'SOURCE_VARIANT__%' THEN
        PERFORM process_source_variant(key, value);
    ELSIF key LIKE 'LABEL_VARIANT__%' THEN
        PERFORM process_label_variant(key, value);
    ELSIF key LIKE 'TRAINING_SET_VARIANT__%' THEN
        PERFORM process_ts_variant(key, value);
    ELSE
        RAISE NOTICE 'No matching function for key: %', key;
    END IF;
END;
$$;

-- Function: add_edges_from_all_ff_task_metadata
CREATE FUNCTION add_edges_from_all_ff_task_metadata()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    row RECORD;
BEGIN
    FOR row IN SELECT key, value FROM ff_task_metadata
    LOOP
        IF row.key LIKE 'FEATURE_VARIANT__%' THEN
            PERFORM process_feature_variant(row.key, row.value);
        ELSIF row.key LIKE 'SOURCE_VARIANT__%' THEN
            PERFORM process_source_variant(row.key, row.value);
        ELSIF row.key LIKE 'LABEL_VARIANT__%' THEN
            PERFORM process_label_variant(row.key, row.value);
        ELSIF row.key LIKE 'TRAINING_SET_VARIANT__%' THEN
            PERFORM process_ts_variant(row.key, row.value);
        ELSE
            RAISE NOTICE 'No matching function for key: %', row.key;
        END IF;
    END LOOP;
END;
$$;

-- Function: trigger_add_edge_from_ff_task_metadata_row
CREATE FUNCTION trigger_add_edge_from_ff_task_metadata_row()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM add_edge_from_ff_task_metadata_row(NEW.key, NEW.value);
    RETURN NEW;
END;
$$;

-- Create the edges table if it doesn't exist
CREATE TABLE IF NOT EXISTS edges (
    from_resource_proto_type INTEGER NOT NULL,
    from_resource_name VARCHAR(255) NOT NULL,
    from_resource_variant VARCHAR(255) NOT NULL,
    to_resource_proto_type INTEGER NOT NULL,
    to_resource_name VARCHAR(255) NOT NULL,
    to_resource_variant VARCHAR(255) NOT NULL,
    PRIMARY KEY (
        from_resource_proto_type,
        from_resource_name,
        from_resource_variant,
        to_resource_proto_type,
        to_resource_name,
        to_resource_variant
    )
);

-- Create indexes on edges
CREATE INDEX IF NOT EXISTS idx_from_resource ON edges (
    from_resource_proto_type,
    from_resource_name,
    from_resource_variant
);

CREATE INDEX IF NOT EXISTS idx_to_resource ON edges (
    to_resource_proto_type,
    to_resource_name,
    to_resource_variant
);

-- Populate the edges table if it is empty
DO $$
BEGIN
    PERFORM add_edges_from_all_ff_task_metadata();
END;
$$;

-- Create trigger on ff_task_metadata for new rows
CREATE TRIGGER after_insert_ff_task_metadata
    AFTER INSERT ON ff_task_metadata
    FOR EACH ROW
    EXECUTE PROCEDURE trigger_add_edge_from_ff_task_metadata_row();

-- Create the function
CREATE FUNCTION get_dependencies(
    p_from_resource_proto_type integer,
    p_from_resource_name character varying,
    p_from_resource_variant character varying
)
RETURNS TABLE (
    to_resource_proto_type integer,
    to_resource_name character varying,
    to_resource_variant character varying
)
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    WITH RECURSIVE dependency_chain AS (
        -- Base case: start from the given resource
        SELECT
            e.from_resource_proto_type,
            e.from_resource_name,
            e.from_resource_variant,
            e.to_resource_proto_type,
            e.to_resource_name,
            e.to_resource_variant,
            1 AS depth -- Initialize depth
        FROM edges e
        WHERE e.from_resource_proto_type = p_from_resource_proto_type
            AND e.from_resource_name = p_from_resource_name
            AND e.from_resource_variant = p_from_resource_variant

        UNION ALL

        -- Recursive step: traverse downstream dependencies
        SELECT
            e.from_resource_proto_type,
            e.from_resource_name,
            e.from_resource_variant,
            e.to_resource_proto_type,
            e.to_resource_name,
            e.to_resource_variant,
            dc.depth + 1 -- Increment depth
        FROM edges e
        INNER JOIN dependency_chain dc
            ON e.from_resource_proto_type = dc.to_resource_proto_type
            AND e.from_resource_name = dc.to_resource_name
            AND e.from_resource_variant = dc.to_resource_variant
        WHERE dc.depth < 500 -- Limit recursion depth
    )
SELECT DISTINCT
    dc.to_resource_proto_type,
    dc.to_resource_name,
    dc.to_resource_variant
FROM dependency_chain dc;

-- Note: Explicit cycle detection logic is handled in the WHERE clause above
END;
$function$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE ff_task_metadata
DROP COLUMN IF EXISTS marked_for_deletion_at;

DROP FUNCTION IF EXISTS get_dependencies(integer, varchar, varchar);

DROP TRIGGER IF EXISTS after_insert_ff_task_metadata ON ff_task_metadata;

-- Drop functions
DROP FUNCTION IF EXISTS trigger_add_edge_from_ff_task_metadata_row();
DROP FUNCTION IF EXISTS add_edges_from_all_ff_task_metadata();
DROP FUNCTION IF EXISTS add_edge_from_ff_task_metadata_row(TEXT, TEXT);
DROP FUNCTION IF EXISTS process_ts_variant(TEXT, TEXT);
DROP FUNCTION IF EXISTS process_source_variant(TEXT, TEXT);
DROP FUNCTION IF EXISTS process_label_variant(TEXT, TEXT);
DROP FUNCTION IF EXISTS process_feature_variant(TEXT, TEXT);
DROP FUNCTION IF EXISTS create_resource_key(INTEGER, TEXT, TEXT);
DROP FUNCTION IF EXISTS parse_resource_key(TEXT, resource_component);
DROP FUNCTION IF EXISTS add_edge(INTEGER, TEXT, TEXT, INTEGER, TEXT, TEXT);

-- Drop table and enum type
DROP TABLE IF EXISTS edges;
DROP TYPE IF EXISTS resource_component;

-- +goose StatementEnd
-- +goose Up
-- +goose StatementBegin

-- Function: extract_resource_details
CREATE
FUNCTION extract_resource_details(resource_key text, part integer) RETURNS text
    LANGUAGE sql
AS
$$
SELECT SPLIT_PART(resource_key, '__', part);
$$;

-- Function: generate_resource_name
CREATE
FUNCTION generate_resource_name(resource_type integer, resource_name text, resource_variant text) RETURNS text
    LANGUAGE plpgsql
AS
$$
DECLARE
    resource_type_name TEXT;
BEGIN CASE resource_type
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

-- Function: insert_edge
create
function insert_edge(from_type integer, from_name text, from_variant text, to_type integer, to_name text, to_variant text) returns void
    language plpgsql
as
$$
BEGIN
    -- Check if the key in ff_task_metadata has delete = false
    IF EXISTS (
        SELECT 1
        FROM ff_task_metadata
        WHERE key = generate_resource_name(from_type, from_name, from_variant)
          AND marked_for_deletion_at is null
    ) THEN
-- Perform the INSERT if the condition is satisfied
INSERT INTO edges (from_resource_type, from_resource_name, from_resource_variant,
                   to_resource_type, to_resource_name, to_resource_variant)
VALUES (from_type, from_name, from_variant, to_type, to_name, to_variant)
ON CONFLICT DO NOTHING;
ELSE
        -- Raise an exception or do nothing if delete = true
        RAISE EXCEPTION 'Cannot insert edge because key % is marked as deleted in ff_task_metadata', generate_resource_name(from_type, from_name, from_variant);
END IF;
END;
$$;

-- Function: process_feature_variant
create
function process_feature_variant(feature_variant_key text, feature_variant_value text) returns void
    language plpgsql
as
$$
DECLARE
    feature_type      INT := 4; -- Resource type for feature_variant
provider_type     INT := 8; -- Resource type for provider
training_set_type INT := 6; -- Resource type for training_set_variant
source_type       INT := 7; -- Resource type for source_variant
message           JSONB; -- Parsed Message JSON
item              JSONB; -- Individual item in the array
BEGIN
    -- Extract the Message field as JSON
    message := (feature_variant_value::jsonb ->> 'Message')::jsonb;

-- Add an edge from FEATURE_VARIANT to its provider
IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM insert_edge(
                provider_type,
                message ->> 'provider',
                '',
                feature_type,
                extract_resource_details(feature_variant_key, 2),
                extract_resource_details(feature_variant_key, 3)
                );
END IF;

-- Add edges from FEATURE_VARIANT to trainingsets
IF message -> 'trainingsets' IS NOT NULL THEN
        FOR item IN
SELECT jsonb_array_elements(message -> 'trainingsets')
           LOOP
    PERFORM insert_edge(
            feature_type,
            extract_resource_details(feature_variant_key, 2),
            extract_resource_details(feature_variant_key, 3),
            training_set_type,
            item ->> 'name',
            item ->> 'variant'
            );
END LOOP;
END IF;

-- Add an edge from FEATURE_VARIANT to its source
IF message -> 'source' IS NOT NULL THEN
        PERFORM insert_edge(
                source_type,
                ((message -> 'source')::jsonb) ->> 'name',
                ((message -> 'source')::jsonb) ->> 'variant',
                feature_type,
                extract_resource_details(feature_variant_key, 2),
                extract_resource_details(feature_variant_key, 3)
                );
END IF;

END;
$$;

-- Function: process_label_variant
create
function process_label_variant(label_variant_key text, label_variant_value text) returns void
    language plpgsql
as
$$
DECLARE
    label_type        INT := 5; -- Resource type for label_variant
source_type       INT := 7; -- Resource type for source_variant
training_set_type INT := 6; -- Resource type for training_set_variant
provider_type     INT := 8; -- Resource type for provider
message           JSONB; -- Parsed Message JSON
item              JSONB; -- Individual item in JSON arrays
BEGIN
    -- Extract the Message field as JSON
    message := (label_variant_value::jsonb ->> 'Message')::jsonb;

-- Add edge to provider
IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM insert_edge(
                provider_type,
                message ->> 'provider',
                '',
                label_type,
                extract_resource_details(label_variant_key, 2),
                extract_resource_details(label_variant_key, 3)
                );
END IF;

-- Process trainingsets array
IF message -> 'trainingsets' IS NOT NULL THEN
        FOR item IN
SELECT jsonb_array_elements(message -> 'trainingsets')
           LOOP
    PERFORM insert_edge(
            label_variant_key,
            label_type,
            extract_resource_details(label_variant_key, 2),
            extract_resource_details(label_variant_key, 3),
            training_set_type,
            item ->> 'name',
            item ->> 'variant'
            );
END LOOP;
END IF;

-- Add edge to source
IF message -> 'source' IS NOT NULL THEN
        PERFORM insert_edge(
                source_type,
                ((message -> 'source')::jsonb) ->> 'name',
                ((message -> 'source')::jsonb) ->> 'variant',
                label_type,
                extract_resource_details(label_variant_key, 2),
                extract_resource_details(label_variant_key, 3)
                );
END IF;
END;
$$;

-- Function: process_source_variant
create
function process_source_variant(
    source_variant_key text,
    source_variant_value text
) returns void
    language plpgsql
as
$$
DECLARE
    source_type       INT := 7; -- Resource type for source_variant
provider_type     INT := 8; -- Resource type for provider
feature_type      INT := 4; -- Resource type for feature_variant
training_set_type INT := 6; -- Resource type for training_set_variant
label_type        INT := 5; -- Resource type for label_variant
message           JSONB; -- Parsed Message JSON
item              JSONB; -- Individual item in lists
BEGIN
    -- Extract the Message field as JSON
    message := (source_variant_value::jsonb ->> 'Message')::jsonb;

-- Add an edge from SOURCE_VARIANT to its provider
IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM insert_edge(
                provider_type,
                message ->> 'provider',
                '',
                source_type,
                extract_resource_details(source_variant_key, 2),
                extract_resource_details(source_variant_key, 3)
                );
END IF;

-- Process the "source" array in SQLTransformation or DFTransformation
IF message -> 'transformation' -> 'SQLTransformation' -> 'source' IS NOT NULL THEN
        FOR item IN
SELECT jsonb_array_elements(message -> 'transformation' -> 'SQLTransformation' -> 'source')
           LOOP
    PERFORM insert_edge(
            source_type,
            item ->> 'name',
            item ->> 'variant',
            source_type,
            extract_resource_details(source_variant_key, 2),
            extract_resource_details(source_variant_key, 3)
            );
END LOOP;
ELSIF message -> 'transformation' -> 'DFTransformation' -> 'inputs' IS NOT NULL THEN
        FOR item IN
SELECT jsonb_array_elements(message -> 'transformation' -> 'DFTransformation' -> 'inputs')
           LOOP
    PERFORM insert_edge(
            source_type,
            item ->> 'name',
            item ->> 'variant',
            source_type,
            extract_resource_details(source_variant_key, 2),
            extract_resource_details(source_variant_key, 3)
            );
END LOOP;
END IF;

-- Process the "features" array
IF message -> 'features' IS NOT NULL THEN
        FOR item IN
SELECT jsonb_array_elements(message -> 'features')
           LOOP
    PERFORM insert_edge(
            source_type,
            extract_resource_details(source_variant_key, 2),
            extract_resource_details(source_variant_key, 3),
            feature_type,
            item ->> 'name',
            item ->> 'variant'
            );
END LOOP;
END IF;

-- Process the "trainingsets" array
IF message -> 'trainingsets' IS NOT NULL THEN
        FOR item IN
SELECT jsonb_array_elements(message -> 'trainingsets')
           LOOP
    PERFORM insert_edge(
            training_set_type,
            extract_resource_details(source_variant_key, 2),
            extract_resource_details(source_variant_key, 3),
            training_set_type,
            item ->> 'name',
            item ->> 'variant'
            );
END LOOP;
END IF;

-- Process the "labels" array
IF message -> 'labels' IS NOT NULL THEN
        FOR item IN
SELECT jsonb_array_elements(message -> 'labels')
           LOOP
    PERFORM insert_edge(
            source_type,
            extract_resource_details(source_variant_key, 2),
            extract_resource_details(source_variant_key, 3),
            label_type,
            item ->> 'name',
            item ->> 'variant'
            );
END LOOP;
END IF;

END;
$$;

-- Function: process_ts_variant
CREATE
FUNCTION process_ts_variant(
    ts_key TEXT,
    ts_value TEXT
) RETURNS VOID
    LANGUAGE plpgsql
AS
$$
DECLARE
    ts_type       INT := 6; -- Resource type for TS_VARIANT
provider_type INT := 8; -- Resource type for provider
label_type    INT := 5; -- Resource type for label_variant
feature_type  INT := 4; -- Resource type for feature_variant
message       JSONB; -- Parsed Message JSON
item          JSONB; -- Individual item in lists
BEGIN
    -- Extract the Message field as JSON
    message := (ts_value::jsonb ->> 'Message')::jsonb;

-- Add an edge from TS_VARIANT to its provider
IF (message ->> 'provider') IS NOT NULL THEN
        PERFORM insert_edge(
                provider_type,
                message ->> 'provider',
                '',
                ts_type,
                extract_resource_details(ts_key, 2),
                extract_resource_details(ts_key, 3)
                );
END IF;

-- Add an edge from TS_VARIANT to its label
IF message ->> 'label' IS NOT NULL THEN
        PERFORM insert_edge(
                label_type,
                ((message ->> 'label')::jsonb) ->> 'name',
                ((message ->> 'label')::jsonb) ->> 'variant',
                ts_type,
                extract_resource_details(ts_key, 2),
                extract_resource_details(ts_key, 3)
                );
END IF;

-- Add edges for features
IF message -> 'features' IS NOT NULL THEN
        FOR item IN
SELECT jsonb_array_elements(message -> 'features')
           LOOP
    PERFORM insert_edge(
            feature_type,
            item ->> 'name',
            item ->> 'variant',
            ts_type,
            extract_resource_details(ts_key, 2),
            extract_resource_details(ts_key, 3)
            );
END LOOP;
END IF;
END;
$$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Drop all functions
DROP
FUNCTION IF EXISTS process_ts_variant(text, text);
DROP
FUNCTION IF EXISTS process_source_variant(text, text);
DROP
FUNCTION IF EXISTS process_label_variant(text, text);
DROP
FUNCTION IF EXISTS process_feature_variant(text, text);
DROP
FUNCTION IF EXISTS generate_resource_name(integer, text, text);
DROP
FUNCTION IF EXISTS extract_resource_details(text, integer);
DROP
FUNCTION IF EXISTS insert_edge(integer, text, text, integer, text, text);

-- +goose StatementEnd
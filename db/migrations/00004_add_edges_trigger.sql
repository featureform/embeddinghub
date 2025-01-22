-- +goose Up
-- +goose StatementBegin

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
   row RECORD;  -- A record to hold each row from ff_task_metadata
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
   -- Call the existing function for the new row
   PERFORM add_edge_from_ff_task_metadata_row(NEW.key, NEW.value);
   RETURN NEW;  -- Allow the INSERT operation to proceed
END;
$$;

-- Trigger: after_insert_ff_task_metadata
CREATE TRIGGER after_insert_ff_task_metadata
   AFTER INSERT ON ff_task_metadata
   FOR EACH ROW
   EXECUTE PROCEDURE trigger_add_edge_from_ff_task_metadata_row();

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Drop the trigger
DROP TRIGGER IF EXISTS after_insert_ff_task_metadata ON ff_task_metadata;

-- Drop the trigger function
DROP FUNCTION IF EXISTS trigger_add_edge_from_ff_task_metadata_row;

-- Drop the functions
DROP FUNCTION IF EXISTS add_edges_from_all_ff_task_metadata;
DROP FUNCTION IF EXISTS add_edge_from_ff_task_metadata_row(TEXT, TEXT);

-- +goose StatementEnd
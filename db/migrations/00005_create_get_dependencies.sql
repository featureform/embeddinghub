-- +goose Up
-- +goose StatementBegin
-- Migration script to create the get_dependencies function

drop function if exists get_dependencies(integer, varchar, varchar);

-- Create the function
create function get_dependencies(
    p_from_resource_type integer,
    p_from_resource_name character varying,
    p_from_resource_variant character varying
)
returns TABLE(
    to_resource_type integer,
    to_resource_name character varying,
    to_resource_variant character varying
)
language plpgsql
as $function$
BEGIN
    RETURN QUERY
    WITH RECURSIVE dependency_chain AS (
        -- Base case: start from the given resource
        SELECT
            e.from_resource_type,
            e.from_resource_name,
            e.from_resource_variant,
            e.to_resource_type,
            e.to_resource_name,
            e.to_resource_variant
        FROM edges e
        WHERE e.from_resource_type = p_from_resource_type
          AND e.from_resource_name = p_from_resource_name
          AND e.from_resource_variant = p_from_resource_variant

        UNION ALL

        -- Recursive step: traverse downstream dependencies
        SELECT
            e.from_resource_type,
            e.from_resource_name,
            e.from_resource_variant,
            e.to_resource_type,
            e.to_resource_name,
            e.to_resource_variant
        FROM edges e
        INNER JOIN dependency_chain dc
            ON e.from_resource_type = dc.to_resource_type
            AND e.from_resource_name = dc.to_resource_name
            AND e.from_resource_variant = dc.to_resource_variant
    )
-- Select only unique downstream dependencies
SELECT DISTINCT
    dc.to_resource_type,
    dc.to_resource_name,
    dc.to_resource_variant
FROM dependency_chain dc;
END;
$function$;

-- Set the owner of the function
alter function get_dependencies(integer, varchar, varchar) owner to postgres;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- Drop the function
drop function if exists get_dependencies(integer, varchar, varchar);
-- +goose StatementEnd

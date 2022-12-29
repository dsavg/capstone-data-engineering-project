--DROP TABLE {{params.schema_name}}.creators_d CASCADE;

CREATE TABLE IF NOT EXISTS {{params.schema_name}}.creators_d
(
    creator_id      VARCHAR      PRIMARY KEY     SORTKEY,
    object_type_id  VARCHAR,
    name            VARCHAR,
    is_blocked      BOOLEAN,
    is_active       BOOLEAN
)
diststyle all;

DELETE FROM {{params.schema_name}}.creators_d;

INSERT INTO {{params.schema_name}}.creators_d
(
    SELECT creator_id, object_type_id, name, is_blocked, is_active
    FROM (
        SELECT
            creator_id,
            object_type_id,
            name,
            is_blocked,
            is_active,
            row_number() over (PARTITION BY creator_id
                               ORDER BY snapshot_date DESC) AS rnk
        FROM {{params.schema_name}}.creators_snapshot
    )
    WHERE rnk = 1
);
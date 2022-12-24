CREATE TABLE IF NOT EXISTS {{params.schema_name}}.creators_d
(
    creator_id VARCHAR(256) NOT NULL,
    name VARCHAR(256),
    is_blocked BOOLEAN
);

DELETE FROM {{params.schema_name}}.creators_d;

INSERT INTO {{params.schema_name}}.creators_d
(
    SELECT creator_id, name, is_blocked
    FROM (
        SELECT
            creator_id,
            name,
            is_blocked,
            row_number() over (partition by name, creator_id order by snapshot_date desc) AS rnk
        FROM {{params.schema_name}}.creators_snapshot
    )
    WHERE rnk = 1
);
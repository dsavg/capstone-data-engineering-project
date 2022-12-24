CREATE TABLE IF NOT EXISTS {{params.schema_name}}.creators_snapshot
(
    creator_id VARCHAR(256),
    name VARCHAR(256),
    is_blocked BOOLEAN,
    snapshot_date varchar(256)
);

DELETE FROM {{params.schema_name}}.creators_snapshot WHERE snapshot_date='{{params.dt}}';

INSERT INTO {{params.schema_name}}.creators_snapshot
(
    SELECT
        author_fullname AS creator_id,
        author AS name,
        author_is_blocked AS is_blocked,
        '{{params.dt}}' AS snapshot_date
    FROM {{params.schema_name}}.reddit_logs
    where snapshot_date = '{{params.dt}}'
    GROUP BY 1, 2, 3
);
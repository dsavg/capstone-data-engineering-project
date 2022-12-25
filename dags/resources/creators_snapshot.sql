CREATE TABLE IF NOT EXISTS {{params.schema_name}}.creators_snapshot
(
    creator_id VARCHAR(256) NOT NULL,
    name VARCHAR(256),
    is_blocked BOOLEAN,
    snapshot_date varchar(256)
);

DELETE FROM {{params.schema_name}}.creators_snapshot WHERE snapshot_date='{{ ds }}';

INSERT INTO {{params.schema_name}}.creators_snapshot
(
    SELECT
        author_fullname AS creator_id,
        author AS name,
        author_is_blocked AS is_blocked,
        '{{ ds }}' AS snapshot_date
    FROM {{params.schema_name}}.reddit_logs
    WHERE snapshot_date = '{{ ds }}'
    GROUP BY 1, 2, 3
);
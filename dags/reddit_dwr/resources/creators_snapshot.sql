--DROP TABLE IF EXISTS {{params.schema_name}}.creators_snapshot CASCADE;

CREATE TABLE IF NOT EXISTS {{params.schema_name}}.creators_snapshot
(
    creator_id      VARCHAR      NOT NULL,
    object_type_id  VARCHAR      NOT NULL,
    name            VARCHAR      NOT NULL,
    is_blocked      BOOLEAN      NOT NULL,
    is_active       BOOLEAN      NOT NULL,
    snapshot_date   VARCHAR      DISTKEY
);

DELETE FROM {{params.schema_name}}.creators_snapshot WHERE snapshot_date='{{ ds }}';

INSERT INTO {{params.schema_name}}.creators_snapshot
(
    SELECT
        author_fullname creator_id,
        split_part(author_fullname,'_',1) AS object_type_id,
        author AS name,
        author_is_blocked AS is_blocked,
        CASE
            WHEN author_fullname = 't2_unknown' THEN False
            ELSE True
        END is_active,
        '{{ ds }}' AS snapshot_date
    FROM {{params.schema_name}}.reddit_logs
    WHERE snapshot_date = '{{ ds }}'
    GROUP BY 1, 2, 3, 4, 5
);
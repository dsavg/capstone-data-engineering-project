--DROP TABLE IF EXISTS {{params.schema_name}}.subreddits_snapshot CASCADE;

CREATE TABLE IF NOT EXISTS {{params.schema_name}}.subreddits_snapshot
(
    subreddit_id      VARCHAR     NOT NULL,
    name              VARCHAR,
    object_type_id    VARCHAR,
    prefixed_name     VARCHAR,
    type              VARCHAR,
    num_subscribers   BIGINT,
    snapshot_date     VARCHAR     DISTKEY
);

DELETE FROM {{params.schema_name}}.subreddits_snapshot WHERE snapshot_date='{{ ds }}';

INSERT INTO {{params.schema_name}}.subreddits_snapshot
(
    SELECT
        subreddit_id AS subreddit_id,
        subreddit AS name,
        split_part(subreddit_id,'_',1) AS object_type_id,
        subreddit_name_prefixed AS prefixed_name,
        subreddit_type AS type,
        subreddit_subscribers AS num_subscribers,
        '{{ ds }}' AS snapshot_date
    FROM {{params.schema_name}}.reddit_logs
    WHERE snapshot_date = '{{ ds }}'
    GROUP BY 1, 2, 3, 4, 5, 6
);
CREATE TABLE IF NOT EXISTS {{params.schema_name}}.subreddit_snapshot
(
    subreddit_id varchar(256) NOT NULL,
    name varchar(256),
    prefixed_name varchar(256),
    type varchar(256),
    num_subscribers int8,
    snapshot_date varchar(256)
);

DELETE FROM {{params.schema_name}}.subreddit_snapshot WHERE snapshot_date='{{ ds }}';

INSERT INTO {{params.schema_name}}.subreddit_snapshot
(
    SELECT
        subreddit_id AS subreddit_id,
        subreddit AS name,
        subreddit_name_prefixed AS prefixed_name,
        subreddit_type AS type,
        subreddit_subscribers AS num_subscribers,
        '{{ ds }}' AS snapshot_date
    FROM {{params.schema_name}}.reddit_logs
    WHERE snapshot_date = '{{ ds }}'
    GROUP BY 1, 2, 3, 4, 5
);
CREATE TABLE IF NOT EXISTS {{params.schema_name}}.subreddits_d
(
    subreddit_id      VARCHAR     PRIMARY KEY    SORTKEY,
    name              VARCHAR,
    object_type_id    VARCHAR,
    prefixed_name     VARCHAR,
    type              VARCHAR,
    num_subscribers   BIGINT
)
diststyle all;

DELETE FROM {{params.schema_name}}.subreddits_d;

INSERT INTO {{params.schema_name}}.subreddits_d
(
    SELECT
        subreddit_id, name, object_type_id, prefixed_name,
        type, num_subscribers
    FROM (
        SELECT
            subreddit_id,
            name,
            object_type_id,
            prefixed_name,
            type,
            num_subscribers,
            row_number() over (partition by subreddit_id order by snapshot_date desc) AS rnk
        FROM {{params.schema_name}}.subreddits_snapshot
    )
    WHERE rnk = 1
);
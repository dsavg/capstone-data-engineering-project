CREATE TABLE IF NOT EXISTS {{params.schema_name}}.subreddit_d
(
    subreddit_id varchar(256) NOT NULL,
    name varchar(256),
    prefixed_name varchar(256),
    type varchar(256),
    num_subscribers int8,
	CONSTRAINT subreddit_d_pkey PRIMARY KEY (subreddit_id)
);

DELETE FROM {{params.schema_name}}.subreddit_d;

INSERT INTO {{params.schema_name}}.subreddit_d
(
    SELECT subreddit_id, name, prefixed_name, type, num_subscribers
    FROM (
        SELECT
            subreddit_id,
            name,
            prefixed_name,
            type,
            num_subscribers,
            row_number() over (partition by subreddit_id order by snapshot_date desc) AS rnk
        FROM {{params.schema_name}}.subreddit_snapshot
    )
    WHERE rnk = 1
);
CREATE TABLE IF NOT EXISTS {{params.schema_name}}.reddit_fact (
    creator_id varchar(256) NOT NULL     REFERENCES {{params.schema_name}}.creators_d(creator_id),
    subreddit_id varchar(256) NOT NULL   REFERENCES {{params.schema_name}}.subreddit_d(subreddit_id),
    post_id varchar(256) NOT NULL        REFERENCES {{params.schema_name}}.post_d(post_id),
    num_comments int8,
    num_crossposts int8,
    score bigint,
    total_awards_received int8,
    ups int8,
    upvote_ratio FLOAT8,
    dt varchar(256),
	CONSTRAINT reddit_fact_pkey PRIMARY KEY (creator_id, subreddit_id, post_id)
);

DELETE FROM {{params.schema_name}}.reddit_fact WHERE dt='{{ ds }}';

INSERT INTO {{params.schema_name}}.reddit_fact
(
    SELECT
        author_fullname AS creator_id,
        subreddit_id,
        id AS post_id,
        num_comments,
        num_crossposts,
        score,
        total_awards_received,
        ups,
        upvote_ratio,
        '{{ ds }}' AS dt
    FROM {{params.schema_name}}.reddit_logs
    WHERE snapshot_date = '{{ ds }}'
);
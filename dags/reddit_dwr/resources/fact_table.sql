--DROP TABLE {{params.schema_name}}.reddit_fact CASCADE;

CREATE TABLE IF NOT EXISTS {{params.schema_name}}.reddit_fact (
    event_id                BIGINT      IDENTITY(0,1)   PRIMARY KEY,
    creator_id              VARCHAR     NOT NULL        REFERENCES {{params.schema_name}}.creators_d(creator_id),
    subreddit_id            VARCHAR     NOT NULL        REFERENCES {{params.schema_name}}.subreddits_d(subreddit_id),
    post_id                 VARCHAR     NOT NULL        REFERENCES {{params.schema_name}}.posts_d(post_id),
    created_at              DATE        NOT NULL        REFERENCES {{params.schema_name}}.dwr_dates("date"),
    created_at_timestamp    TIMESTAMP,
    num_comments            BIGINT,
    num_crossposts          BIGINT,
    score                   BIGINT,
    total_awards_received   BIGINT,
    ups                     BIGINT,
    upvote_ratio            FLOAT,
    dt                      VARCHAR     NOT NULL     DISTKEY
);

DELETE FROM {{params.schema_name}}.reddit_fact WHERE dt='{{ ds }}';

INSERT INTO {{params.schema_name}}.reddit_fact (creator_id, subreddit_id, post_id,
created_at, created_at_timestamp, num_comments, num_crossposts, score, total_awards_received,
ups, upvote_ratio, dt)
(
    SELECT
        author_fullname AS creator_id,
        subreddit_id,
        name AS post_id,
        trunc(TIMESTAMP WITHOUT TIME ZONE 'epoch' + (created_utc::bigint::float) * INTERVAL '1 second') AS created_at,
        TIMESTAMP WITHOUT TIME ZONE 'epoch' + (created_utc::bigint::float) * INTERVAL '1 second' AS created_at_timestamp,
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
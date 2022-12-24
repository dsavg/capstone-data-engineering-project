CREATE TABLE IF NOT EXISTS {{params.schema_name}}.staging_reddit_logs (

    author varchar(256),
    author_fullname varchar(256),
    author_is_blocked boolean,

    subreddit varchar(256),
    subreddit_id varchar(256),
    subreddit_name_prefixed varchar(256),
    subreddit_subscribers int8,
    subreddit_type varchar(256),

    created_utc FLOAT8,
    domain varchar(256),
    gilded bigint,
    hidden boolean,
    hide_score boolean,
    id varchar(256),
    is_created_from_ads_ui boolean,
    is_crosspostable boolean,
    is_video boolean,
    likes VARBYTE,
    type varchar(256),
    event_id varchar(256),
    name varchar(256),
    no_follow boolean,
    num_comments int8,
    num_crossposts int8,
    num_reports VARBYTE,
    over_18 boolean,
    pinned boolean,
    saved boolean,
    score bigint,
    send_replies boolean,

    title varchar(256),
    total_awards_received int8,
    ups int8,
    upvote_ratio FLOAT8,
    url varchar(256)
);
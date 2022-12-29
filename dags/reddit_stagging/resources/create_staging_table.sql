CREATE TABLE IF NOT EXISTS {{params.schema_name}}.staging_reddit_logs (

    author                      VARCHAR,
    author_fullname             VARCHAR,
    author_is_blocked           BOOLEAN,

    subreddit                   VARCHAR,
    subreddit_id                VARCHAR,
    subreddit_name_prefixed     VARCHAR,
    subreddit_subscribers       BIGINT,
    subreddit_type              VARCHAR,

    created_utc                 FLOAT8,
    domain                      VARCHAR,
    gilded                      BIGINT,
    hidden                      BOOLEAN,
    hide_score                  BOOLEAN,
    id                          VARCHAR,
    is_created_from_ads_ui      BOOLEAN,
    is_crosspostable            BOOLEAN,
    is_video                    BOOLEAN,
    likes                       VARBYTE,
    type                        VARCHAR,
    event_id                    VARCHAR,
    name                        VARCHAR,
    no_follow                   BOOLEAN,
    num_comments                BIGINT,
    num_crossposts              BIGINT,
    num_reports                 VARBYTE,
    over_18                     BOOLEAN,
    pinned                      BOOLEAN,
    saved                       BOOLEAN,
    score                       BIGINT,
    send_replies                BOOLEAN,

    title                       VARCHAR,
    total_awards_received       BIGINT,
    ups                         BIGINT,
    upvote_ratio                FLOAT,
    url                         VARCHAR
);
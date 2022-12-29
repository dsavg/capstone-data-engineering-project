CREATE TABLE IF NOT EXISTS {{params.schema_name}}.reddit_logs (

    author                  VARCHAR,
    author_fullname         VARCHAR,
    author_is_blocked       BOOLEAN,

    subreddit               VARCHAR,
    subreddit_id            VARCHAR,
    subreddit_name_prefixed VARCHAR,
    subreddit_subscribers   BIGINT,
    subreddit_type          VARCHAR,

    created_utc             FLOAT,
    domain                  VARCHAR,
    gilded                  BIGINT,
    hidden                  BOOLEAN,
    hide_score              BOOLEAN,
    id                      VARCHAR,
    is_created_from_ads_ui  BOOLEAN,
    is_crosspostable        BOOLEAN,
    is_video                BOOLEAN,
    likes                   VARBYTE,
    type                    VARCHAR,
    event_id                VARCHAR,
    name                    VARCHAR,
    no_follow               BOOLEAN,
    num_comments            BIGINT,
    num_crossposts          BIGINT,
    num_reports             VARBYTE,
    over_18                 BOOLEAN,
    pinned                  BOOLEAN,
    saved                   BOOLEAN,
    score                   BIGINT,
    send_replies            BOOLEAN,

    title                   VARCHAR(500),
    total_awards_received   BIGINT,
    ups                     BIGINT,
    upvote_ratio            FLOAT,
    url                     VARCHAR(2500),
    snapshot_date           VARCHAR             DISTKEY
);

DELETE FROM {{params.schema_name}}.reddit_logs WHERE snapshot_date='{{ ds }}';

INSERT INTO {{params.schema_name}}.reddit_logs
(
    SELECT
        author,
        case
            when author_fullname is not null then author_fullname
            else 't2_unknown'
        end AS author_fullname,
        author_is_blocked,
        subreddit,
        subreddit_id,
        subreddit_name_prefixed,
        subreddit_subscribers,
        subreddit_type,
        created_utc,
        domain,
        gilded,
        hidden,
        hide_score,
        id,
        is_created_from_ads_ui,
        is_crosspostable,
        is_video,
        likes,
        type,
        event_id,
        name,
        no_follow,
        num_comments,
        num_crossposts,
        num_reports,
        over_18,
        pinned,
        saved,
        score,
        send_replies,
        title,
        total_awards_received,
        ups,
        upvote_ratio,
        url,
        '{{ ds }}' AS snapshot_date
    FROM {{params.schema_name}}.staging_reddit_logs
);
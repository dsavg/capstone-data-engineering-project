CREATE TABLE IF NOT EXISTS {{params.schema_name}}.post_snapshot
(
    post_id varchar(256) NOT NULL,
    created_date timestamp,
    title varchar(256),
    name varchar(256),
    type varchar(256),
    domain varchar(256),
    url_link varchar(256),
    hidden boolean,
    hide_score boolean,
    no_follow boolean,
    is_created_from_ads_ui boolean,
    is_crosspostable boolean,
    is_video boolean,
    gilded bigint,
    over_18 boolean,
    pinned boolean,
    saved boolean,
    send_replies boolean,
    snapshot_date varchar(256)
);

DELETE FROM {{params.schema_name}}.post_snapshot WHERE snapshot_date='{{ ds }}';

INSERT INTO {{params.schema_name}}.post_snapshot
(
    SELECT
        id as post_id,
        TIMESTAMP WITHOUT TIME ZONE 'epoch' + (created_utc::bigint::float) * INTERVAL '1 second' AS created_date,
        title,
        name,
        type,
        domain,
        url AS url_link,
        hidden,
        hide_score,
        no_follow,
        is_created_from_ads_ui,
        is_crosspostable,
        is_video,
        gilded,
        over_18,
        pinned,
        saved,
        send_replies,
        '{{ ds }}' AS snapshot_date
    FROM {{params.schema_name}}.reddit_logs
    WHERE snapshot_date = '{{ ds }}'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
);
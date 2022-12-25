CREATE TABLE IF NOT EXISTS {{params.schema_name}}.post_d
(
    post_id varchar(256)      PRIMARY KEY      SORTKEY,
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
    send_replies boolean
)
diststyle all;

DELETE FROM {{params.schema_name}}.post_d;

INSERT INTO {{params.schema_name}}.post_d
(
    SELECT
        post_id, created_date, title, name, type, domain, url_link,
        hidden, hide_score, no_follow, is_created_from_ads_ui, is_crosspostable,
        is_video, gilded, over_18, pinned, saved, send_replies
    FROM (
        SELECT
            post_id,
            created_date,
            title,
            name,
            type,
            domain,
            url_link,
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
            row_number() over (partition by post_id order by snapshot_date desc) AS rnk
        FROM {{params.schema_name}}.post_snapshot
        WHERE snapshot_date = '{{ ds }}'
    )
    WHERE rnk = 1
);
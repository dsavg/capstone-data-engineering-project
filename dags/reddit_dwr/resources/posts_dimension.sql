CREATE TABLE IF NOT EXISTS {{params.schema_name}}.posts_d
(
    post_id                     VARCHAR             PRIMARY KEY     SORTKEY,
    title                       VARCHAR(500),
    object_type_id              VARCHAR,
    type                        VARCHAR,
    domain                      VARCHAR,
    url_link                    VARCHAR(2500),
    hide_score                  BOOLEAN,
    no_follow                   BOOLEAN,
    is_created_from_ads_ui      BOOLEAN,
    is_crosspostable            BOOLEAN,
    is_video                    BOOLEAN,
    gilded                      BIGINT,
    over_18                     BOOLEAN,
    pinned                      BOOLEAN
)
diststyle all;

DELETE FROM {{params.schema_name}}.posts_d;

INSERT INTO {{params.schema_name}}.posts_d
(
    SELECT
        post_id, title, object_type_id, type, domain, url_link,
        hide_score, no_follow, is_created_from_ads_ui,
        is_crosspostable, is_video, gilded, over_18,
        pinned
    FROM (
        SELECT
            post_id,
            title,
            object_type_id,
            type,
            domain,
            url_link,
            hide_score,
            no_follow,
            is_created_from_ads_ui,
            is_crosspostable,
            is_video,
            gilded,
            over_18,
            pinned,
            row_number() over (PARTITION BY post_id
                               ORDER BY snapshot_date desc) AS rnk
        FROM {{params.schema_name}}.posts_snapshot
        WHERE snapshot_date = '{{ ds }}'
    )
    WHERE rnk = 1
);
CREATE TABLE IF NOT EXISTS {{params.schema_name}}.posts_snapshot
(
    post_id                 VARCHAR          NOT NULL,
    title                   VARCHAR(500),
    object_type_id          VARCHAR,
    type                    VARCHAR,
    domain                  VARCHAR,
    url_link                VARCHAR(2500),
    hide_score              BOOLEAN,
    no_follow               BOOLEAN,
    is_created_from_ads_ui  BOOLEAN,
    is_crosspostable        BOOLEAN,
    is_video                BOOLEAN,
    gilded                  BIGINT,
    over_18                 BOOLEAN,
    pinned                  BOOLEAN,
    snapshot_date           VARCHAR          DISTKEY
);

DELETE FROM {{params.schema_name}}.posts_snapshot WHERE snapshot_date='{{ ds }}';

INSERT INTO {{params.schema_name}}.posts_snapshot
(
    SELECT
        name as post_id,
        title,
        split_part(name,'_',1)  object_type_id,
        type,
        domain,
        url AS url_link,
        hide_score,
        no_follow,
        is_created_from_ads_ui,
        is_crosspostable,
        is_video,
        gilded,
        over_18,
        pinned,
        '{{ ds }}' AS snapshot_date
    FROM {{params.schema_name}}.reddit_logs
    WHERE snapshot_date = '{{ ds }}'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
);
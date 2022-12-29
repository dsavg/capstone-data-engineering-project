CREATE TABLE IF NOT EXISTS {{params.schema_name}}.dwr_dates (
    "date"                          DATE        PRIMARY KEY      SORTKEY,
    "day"                           BIGINT,
    day_name                        VARCHAR,
    week                            BIGINT,
    weekday                         BIGINT,
    "month"                         BIGINT,
    month_name                      VARCHAR,
    "year"                          BIGINT,
    united_state_holidays           VARCHAR,
    is_united_state_holiday         BOOLEAN,
    united_kingdom_holidays         VARCHAR,
    is_united_kingdom_holiday       BOOLEAN,
    canada_holidays                 VARCHAR,
    is_canada_holiday               BOOLEAN,
    australia_holidays              VARCHAR,
    is_australia_holiday            BOOLEAN,
    germany_holidays                VARCHAR,
    is_germany_holiday              BOOLEAN
)
diststyle all;

DELETE FROM {{params.schema_name}}.dwr_dates;

INSERT INTO {{params.schema_name}}.dwr_dates
(
    SELECT
        TIMESTAMP WITHOUT TIME ZONE 'epoch' + (date_utc::bigint::float) * INTERVAL '1 second' AS "date",
        "day",
        day_name,
        week,
        weekday,
        "month",
        month_name,
        "year",
        united_state_holidays,
        is_united_state_holiday,
        united_kingdom_holidays,
        is_united_kingdom_holiday,
        canada_holidays,
        is_canada_holiday,
        australia_holidays,
        is_australia_holiday,
        germany_holidays,
        is_germany_holiday
    FROM {{params.schema_name}}.staging_dwr_dates
);
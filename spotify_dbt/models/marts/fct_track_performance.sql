-- fct_track_performance.sql
-- Fact table: one row per track per chart market
-- Joins track info with artist genre data

with tracks as (
    select * from {{ ref('stg_tracks') }}
),

artists as (
    select * from {{ ref('stg_artists') }}
),

joined as (
    select
        -- Track identifiers
        t.track_id,
        t.track_name,
        t.artist_name,
        t.album_name,

        -- Chart context
        t.chart_market,
        t.genre                         as track_genre,
        a.genre                         as artist_genre,

        -- Pricing
        t.price,
        t.price_tier,

        -- Dates
        t.release_date,
        t.extracted_at                  as chart_date,

        -- Derived metrics
        datediff(
            'day',
            try_to_date(t.release_date),
            t.extracted_at::date
        )                               as days_since_release,

        case
            when datediff(
                'day',
                try_to_date(t.release_date),
                t.extracted_at::date
            ) <= 30  then 'new_release'
            when datediff(
                'day',
                try_to_date(t.release_date),
                t.extracted_at::date
            ) <= 365 then 'recent'
            else          'catalogue'
        end                             as release_category,

        t.loaded_at

    from tracks t
    left join artists a
        on  t.artist_name  = a.artist_name
        and t.chart_market = a.chart_market
)

select * from joined
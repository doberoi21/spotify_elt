-- dim_artists.sql
-- Dimension table: one row per artist per market
-- Aggregates chart appearances and genre info

with track_facts as (
    select * from {{ ref('fct_track_performance') }}
),

artist_summary as (
    select
        artist_name,
        chart_market,
        artist_genre                            as genre,

        -- Chart metrics
        count(distinct track_id)                as total_tracks_charted,
        count(distinct track_genre)             as genre_diversity,
        avg(price)                              as avg_track_price,
        min(release_date)                       as earliest_release,
        max(release_date)                       as latest_release,

        -- How many tracks are new releases vs catalogue
        sum(case when release_category = 'new_release' then 1 else 0 end)
                                                as new_release_count,
        sum(case when release_category = 'catalogue'   then 1 else 0 end)
                                                as catalogue_count,

        max(loaded_at)                          as last_seen_at

    from track_facts
    group by artist_name, chart_market, artist_genre
)

select * from artist_summary
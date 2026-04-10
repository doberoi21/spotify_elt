-- stg_artists.sql
-- Cleans and deduplicates artist data

with source as (
    select * from SPOTIFY_DB.RAW.RAW_ARTISTS
),

cleaned as (
    select
        trim(artist_name)   as artist_name,
        trim(genre)         as genre,
        chart_market,
        extracted_at::timestamp as extracted_at,
        loaded_at,
        -- Row number to deduplicate — keep latest record per artist per market
        row_number() over (
            partition by trim(artist_name), chart_market
            order by loaded_at desc
        )                   as row_num
    from source
    where artist_name is not null
),

deduped as (
    select * from cleaned
    where row_num = 1
)

select
    artist_name,
    genre,
    chart_market,
    extracted_at,
    loaded_at
from deduped
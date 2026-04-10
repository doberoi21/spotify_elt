-- stg_tracks.sql
-- Cleans and standardises raw track data from iTunes charts

with source as (
    select * from SPOTIFY_DB.RAW.RAW_TRACKS
),

cleaned as (
    select
        track_id,
        trim(track_name)                        as track_name,
        trim(artist_name)                       as artist_name,
        trim(album_name)                        as album_name,
        trim(genre)                             as genre,
        chart_market,
        price,
        -- Clean release date — some entries have full timestamps
        left(release_date, 10)                  as release_date,
        -- Flag explicit price tier
        case
            when price = 0    then 'free'
            when price < 1.29 then 'standard'
            else 'premium'
        end                                     as price_tier,
        extracted_at::timestamp                 as extracted_at,
        loaded_at
    from source
    where track_name is not null
      and artist_name is not null
)

select * from cleaned
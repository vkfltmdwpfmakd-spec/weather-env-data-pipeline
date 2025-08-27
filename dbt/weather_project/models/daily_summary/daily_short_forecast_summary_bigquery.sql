-- BigQuery용 일별 단기예보 요약 모델
{{ config(materialized='table') }}

with source_data as (

    select
        baseDate as base_date,
        baseTime as base_time,
        fcstDate as fcst_date,
        fcstTime as fcst_time,
        category,
        case
            when fcstValue = '강수없음' then 0
            else CAST(fcstValue AS NUMERIC)
        end as fcst_value,
        nx,
        ny
    from {{ source('weather_staging', 'kma_short_forecast_staging') }}

)

-- BigQuery 구문으로 수정
select 
    PARSE_DATE('%Y%m%d', CAST(base_date AS STRING)) as forecast_date,
    base_time,
    
    -- BigQuery용 datetime 조합
    DATETIME(
        PARSE_DATE('%Y%m%d', CAST(fcst_date AS STRING)),
        PARSE_TIME('%H%M', LPAD(CAST(fcst_time AS STRING), 4, '0'))
    ) as forecast_timestamp,
    
    category,
    fcst_value,
    nx,
    ny
from source_data
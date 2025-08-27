-- BigQuery용 일별 대기질 예보 모델
{{ config(materialized='table') }}

with source_data as (

    select
        dataTime as datatime_str,
        informCode as inform_code,
        informOverall as inform_overall,
        informCause as inform_cause,
        informGrade as inform_grade,
        imageUrl1 as image_url_1,
        imageUrl2 as image_url_2,
        imageUrl3 as image_url_3,
        informData as inform_data
    from {{ source('weather_staging', 'airkorea_air_quality_staging') }}

)

-- BigQuery 구문으로 수정
select
    PARSE_DATE('%Y-%m-%d', CAST(inform_data AS STRING)) as forecast_date,

    -- BigQuery용 timestamp 변환
    PARSE_TIMESTAMP(
        '%Y-%m-%d %H',
        REPLACE(CAST(datatime_str AS STRING), '시 발표', '')
    ) as announce_timestamp,
    
    inform_code,
    inform_overall,
    inform_cause,
    inform_grade,
    image_url_1,
    image_url_2,
    image_url_3
from source_data
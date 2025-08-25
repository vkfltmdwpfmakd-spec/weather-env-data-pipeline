-- dbt가 이 모델을 구체화(materialize)하는 방식을 테이블로 지정
{{ config(materialized='table') }}

-- with 절을 사용하여 중간 단계의 임시 테이블 생성
with source_data as (

    select
        "baseDate"::varchar as base_date,
        "baseTime"::varchar as base_time,
        "fcstDate"::varchar as fcst_date,
        "fcstTime"::varchar as fcst_time,
        category,
        case
            when "fcstValue" = '강수없음' then 0
            else "fcstValue"::numeric
        end as fcst_value,
        nx,
        ny
    from {{ source('public', 'kma_short_forecast_staging') }}

)


-- 선택
select 
    to_date(base_date, 'YYYYMMDD') as forecast_date,
    base_time,

    -- 1. to_date로 날짜 타입 변환
    -- 2. lpad로 '700' -> '0700' 으로 변환 후 time 타입으로 변환
    -- 3. 두 타입을 합쳐서 timestamp로 최종 변환
    to_date(fcst_date, 'YYYYMMDD') + lpad(fcst_time, 4, '0')::time as forecast_timestamp,
    category,
    fcst_value,
    nx,
    ny
from source_data
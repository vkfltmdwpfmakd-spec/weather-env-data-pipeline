-- dbt가 이 모델을 구체화(materialize)하는 방식을 테이블로 지정
{{ config(materialized='table') }}

-- with 절을 사용하여 중간 단계의 임시 테이블 생성
with source_data as (

    select
        "dataTime" as datatime_str,
        "informCode" as inform_code,
        "informOverall" as inform_overall,
        "informCause" as inform_cause,
        "informGrade" as inform_grade,
        "imageUrl1" as image_url_1,
        "imageUrl2" as image_url_2,
        "imageUrl3" as image_url_3,
        "informData" as inform_data
    from {{ source('public', 'airkorea_air_quality_staging') }} -- 'public' 스키마의 'airkorea_air_quality_staging' 테이블

)

-- 선택
select
    to_date(inform_data, 'YYYY-MM-DD') as forecast_date,

    -- 1. replace 함수로 ' 시 발표' 부분을 공백으로 치환 -> 'YYYY-MM-DD HH'
    -- 2. to_timestamp 함수로 해당 문자열을 timestamp 타입으로 변환
    to_timestamp(replace(datatime_str, '시 발표', ''), 'YYYY-MM-DD HH24') as announce_timestamp,
    inform_code,
    inform_overall,
    inform_cause,
    inform_grade,
    image_url_1,
    image_url_2,
    image_url_3
from source_data
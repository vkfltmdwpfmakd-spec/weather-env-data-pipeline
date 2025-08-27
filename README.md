# 날씨 데이터 파이프라인

기상청과 한국환경공단의 공개 API를 활용하여 날씨 예보 및 대기질 정보를 수집, 변환, 분석하는 ELT 데이터 파이프라인입니다.

## 프로젝트 소개

실시간으로 업데이트되는 날씨와 대기질 데이터를 자동으로 수집하고, 데이터 품질을 검증한 후 분석 가능한 형태로 변환하여 저장하는 데이터 처리 시스템입니다.

### 주요 기능
- 기상청 단기예보 API, 한국환경공단 대기질 API 연동
- MinIO(S3 호환) 및 PostgreSQL, Google Cloud Storage, BigQuery 지원
- Great Expectations을 활용한 자동 데이터 검증
- dbt를 사용한 분석용 테이블 생성
- Metabase를 통한 대시보드 제공
- Apache Airflow를 통한 자동화된 데이터 파이프라인

## 시스템 아키텍처

### 로컬 환경
```
데이터 수집 → MinIO 저장 → PostgreSQL 적재 → dbt 변환 → Metabase 시각화
                    ↓
            Great Expectations 품질 검증
```

### 클라우드 환경
```
데이터 수집 → GCS 저장 → BigQuery 적재 → dbt 변환 → Metabase 시각화
                    ↓
            Great Expectations 품질 검증
```

## 기술 스택

**워크플로 오케스트레이션**
- Apache Airflow

**데이터 처리**
- dbt (데이터 변환)
- Great Expectations (데이터 품질 검증)
- Python (데이터 수집 및 처리)

**저장소**
- PostgreSQL (온프레미스 DB)
- MinIO (온프레미스 객체 저장소)
- Google Cloud Storage (클라우드 객체 저장소)
- BigQuery (클라우드 데이터 웨어하우스)

**시각화**
- Metabase
- Jupyter Lab

**인프라**
- Docker & Docker Compose

## 설치 및 실행

### 사전 요구사항
- Docker 및 Docker Compose
- Python 3.8 이상
- GCP 서비스 계정 키 (클라우드 기능 사용시)

### 실행 방법

1. 프로젝트 클론
```bash
git clone <repository-url>
cd weather-pipeline
```

2. GCP 서비스 계정 키 설정 (클라우드 사용시)
```bash
# gcp-keys/ 폴더에 서비스 계정 키 파일 배치
```

3. Docker 컨테이너 실행
```bash
docker-compose up -d
```

4. 서비스 접속
- Airflow: http://localhost:8080
- Jupyter Lab: http://localhost:8888
- MinIO Console: http://localhost:9001
- Metabase: http://localhost:3000

## 프로젝트 구조

```
weather-pipeline/
├── dags/                          # Airflow DAG 파일들
│   ├── airkorea_air_quality_*.py  # 대기질 데이터 수집
│   ├── kma_short_forecast_*.py    # 기상예보 데이터 수집
│   ├── load_*.py                  # 데이터 적재
│   ├── data_quality_check_*.py    # 데이터 품질 검증
│   └── transform_*.py             # 데이터 변환
├── dbt/weather_project/           # dbt 프로젝트
│   ├── models/daily_summary/      # 일별 요약 모델
│   └── dbt_project.yml           # dbt 설정 파일
├── notebooks/gx/                  # Great Expectations 설정
│   ├── expectations/              # 데이터 품질 규칙
│   └── checkpoints/              # 검증 체크포인트
├── data/                          # 로컬 데이터 파일
├── logs/                          # Airflow 로그
├── docker-compose.yml            # Docker 컨테이너 설정
└── requirements.txt              # Python 패키지 목록
```

## 데이터 파이프라인 상세

### 1. 데이터 수집 (Extract)
- 기상청 단기예보 API에서 3일간의 날씨 예보 정보 수집
- 한국환경공단 대기질 API에서 실시간 미세먼지, 초미세먼지, 오존 정보 수집
- 수집된 데이터는 CSV 형태로 MinIO 또는 GCS에 저장

### 2. 데이터 적재 (Load)
- 원본 데이터를 PostgreSQL 또는 BigQuery의 Staging 테이블에 적재
- 데이터 타입 변환 및 기본 전처리 수행

### 3. 데이터 품질 검증
- Great Expectations를 통한 자동 품질 검증
- 필수 필드 존재 여부, 데이터 타입, 값의 범위 등을 검증
- 검증 실패시 파이프라인 중단 및 알림

### 4. 데이터 변환 (Transform)
- dbt를 사용하여 Staging 테이블에서 Analytics 테이블로 변환
- 비즈니스 로직 적용 및 일별 집계 테이블 생성

## 주요 데이터 모델

### daily_air_quality_forecast
일별 대기질 예보 정보 집계
- 예보 날짜, 발표 시간
- PM10, PM2.5, 오존 등급 및 농도 수치
- 지역별 대기질 상태

### daily_short_forecast_summary
일별 기상예보 요약
- 예보 날짜, 시간대별 예보
- 기온 (최고/최저), 강수확률
- 하늘 상태, 풍향, 풍속

## 데이터 품질 관리

Great Expectations를 통해 다음 항목들을 검증합니다:

**완성도 검사**
- 필수 필드의 NULL 값 존재 여부
- 데이터 건수가 예상 범위 내에 있는지

**유효성 검사**
- 날짜 형식의 유효성
- 수치 데이터의 범위 (예: 미세먼지 농도 0~999)
- 카테고리 값의 유효성 (예: 대기질 등급 1~4)

**일관성 검사**
- 중복 데이터 존재 여부
- 논리적 일관성 (예: 최고기온 >= 최저기온)

## 주요 특징

### 이중 환경 지원
프로젝트는 온프레미스와 클라우드 환경을 모두 지원합니다. 각 환경별로 독립적인 DAG와 dbt 모델을 제공하여, 필요에 따라 선택적으로 사용할 수 있습니다.

### 모듈화된 설계
각 데이터 소스별로 독립적인 DAG를 구성하여 유지보수가 용이하고, 새로운 데이터 소스를 쉽게 추가할 수 있도록 설계했습니다.

### 자동화 및 모니터링
Airflow를 통해 전체 파이프라인을 자동화하고, 각 단계별 실행 상태를 모니터링할 수 있습니다. 실패 시 자동 재시도 및 알림 기능을 제공합니다.

## 개발 및 운영

### 개발 환경 설정
1. Jupyter Lab을 통한 데이터 탐색 및 분석
2. dbt를 사용한 로컬 개발 및 테스트
3. Great Expectations를 통한 데이터 품질 규칙 정의

### 운영 모니터링
- Airflow 웹 UI를 통한 파이프라인 상태 확인
- 각 DAG별 실행 로그 및 성능 지표 모니터링
- Great Expectations 검증 결과 리포트 확인


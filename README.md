# Apache Airflow 3.2.2 Local Development Sandbox

이 저장소는 **Apache Airflow 3.2.2**를 Docker Compose 기반으로 로컬에서 실행하고 테스트할 수 있도록 구축된 최신형 개발 샌드박스입니다. 

Airflow 3.0+부터 도입된 **Task SDK** 및 새롭게 개선된 **Asset(구 Datasets) 기반 스케줄링** 등의 핵심 아키텍처 변화를 실험하고 실무에 적용해볼 수 있는 엔터프라이즈급 템플릿을 제공합니다.

---

## 📂 프로젝트 구조

```
.
├── .env                  # 로컬 환경 변수 설정 (비밀키 및 권한 매핑, Git 제외)
├── .gitignore            # 불필요한 로그, 캐시, 비밀 파일 커밋 방지
├── Dockerfile            # Postgres 및 OpenAI 프로바이더가 추가된 확장 이미지 빌드용
├── docker-compose.yaml   # Airflow 3.2.2 멀티 컨테이너 아키텍처 명세서
└── dags/                 # 커스텀 파이프라인(DAG) 폴더
    ├── task_sdk_pipeline.py         # Airflow 3 Task SDK 데코레이터 예제
    ├── taskflow_mixed_pipeline.py   # Task SDK + 전통적인 BashOperator 결합 예제
    ├── asset_producer_sales.py      # 매출 데이터 수집 및 Asset 발행 예제
    ├── asset_producer_inventory.py  # 재고 데이터 수집 및 Asset 발행 예제
    └── asset_consumer_analysis.py   # 매출+재고 Asset이 모두 갱신되면 자동 트리거되는 컨슈머 예제
```

---

## ⚙️ 로컬 개발 환경 실행 방법

### 1. 사전 요구사항 (Prerequisites)
* Docker Desktop 및 Docker Compose v2.14.0 이상
* Docker 할당 메모리 최소 4GB 이상 권장 (8GB 권장)

### 2. 환경 설정 파일 (`.env`) 작성
프로젝트 루트 폴더에 `.env` 파일을 생성하고 다음 내용을 채워 넣습니다.
(Windows 이외의 Linux 환경에서는 `AIRFLOW_UID` 값을 호스트 사용자의 UID(`id -u`)에 맞춰 조정해야 합니다.)

```env
# Airflow 실행 권한
AIRFLOW_UID=50000

# 데이터베이스 암호화 대칭키 (Fernet Key)
# 임의 생성 시: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
FERNET_KEY=NK6U/fViYtbM3v0+8Ou7jmzRfhGSIPLDbri+AoOIri0=

# 기본 예제 DAG 로드 비활성화
AIRFLOW__CORE__LOAD_EXAMPLES=false

# Web UI 내 환경설정 메뉴 노출 여부
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true

# 초기 관리자 계정 생성 옵션
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

### 3. 컨테이너 이미지 빌드
확장 패키지(OpenAI, Postgres 등)가 포함된 로컬 이미지를 빌드합니다.
```powershell
docker compose build
```

### 4. 메타데이터 데이터베이스 초기화
데이터베이스 스키마 생성 및 초기 관리자 사용자 계정을 생성합니다.
```powershell
docker compose up airflow-init
```
*`airflow-init` 컨테이너가 정상적으로 완료되면 (Exited 0) 다음 단계로 진행합니다.*

### 5. 서비스 기동
Airflow의 전반적인 컴포넌트들을 백그라운드 데몬으로 실행합니다.
```powershell
docker compose up -d
```

### 6. Web UI 접속
브라우저를 열고 `http://localhost:8080`에 접속합니다.
* **ID**: `airflow`
* **Password**: `airflow`

---

## 💡 수록된 핵심 파이프라인 (DAG) 설명

### 1. Task SDK 파이프라인 ([task_sdk_pipeline.py](dags/task_sdk_pipeline.py))
Airflow 3의 새로운 문법 표준인 `from airflow.sdk import dag, task` 네임스페이스를 온전히 활용한 순수 파이썬 TaskFlow 데이터 수집 및 집계 파이프라인입니다.

### 2. 하이브리드 파이프라인 ([taskflow_mixed_pipeline.py](dags/taskflow_mixed_pipeline.py))
현대적인 `@task` 파이썬 데코레이터와 기존 Airflow 2.x 형식의 정통 오퍼레이터인 `BashOperator`를 조합하여 동적으로 명령어를 전달하고 선후행 관계를 엮는 실무 믹스 예제입니다.

### 3. 이벤트 기반 스케줄링 파이프라인 (Asset-driven Trigger)
데이터 저장소의 상태 변경을 감지해 하위 워크플로우를 실행시키는 Airflow 3.x의 **Asset 스케줄러** 검증 세트입니다.
* **[asset_producer_sales.py](dags/asset_producer_sales.py)**: 매출 정보를 처리한 후 `s3://data-lake/sales_data.csv` 자산 갱신 이벤트를 발행합니다.
* **[asset_producer_inventory.py](dags/asset_producer_inventory.py)**: 재고 상태를 처리한 후 `s3://data-lake/inventory_data.csv` 자산 갱신 이벤트를 발행합니다.
* **[asset_consumer_analysis.py](dags/asset_consumer_analysis.py)**: 위 두 자산이 **모두 완료(AND 조건: `sales_asset & inventory_asset`)**되면 스케줄러에 의해 자동으로 연쇄 트리거되어 종합 ML 예측 모델 분석을 실행합니다.

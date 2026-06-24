from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task, Asset
from cosmos import DbtTaskGroup

from plugins.dbt_common_config import (
    get_profile_config,
    get_project_config,
    get_execution_config,
    get_render_config,
    get_operator_args
)

# 파일명에서 dag_id 동적 파싱
DAG_ID = Path(__file__).stem

# 완료 시 발행할 Bronze 데이터 자산(Asset) 등록
DBT_BRONZE_ASSET = Asset(uri="clickhouse://default/dbt_bronze_transform_completed")

@dag(
    dag_id=DAG_ID,
    doc_md="""
    ### dbt Bronze 계층 변환 파이프라인 (dbt Bronze Transform)
    원천 landing 데이터 테이블에서 KST 시간대 통일 및 가명화 처리를 거쳐 Bronze 뷰/테이블을 생성합니다.
    
    * **실행 스케줄**: 15분 주기 Cron (`*/15 * * * *`)
    * **주요 입력**: ClickHouse landing 스키마 (`default.stg_olist_*`)
    * **주요 출력 및 자산(Asset)**: `clickhouse://default/dbt_bronze_transform_completed`
    * **dbt 실행 범위**: `models/01_bronze` 디렉토리 하위의 모델 전체
    """,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "dbt", "bronze"]
)
def run_dbt_bronze_transform_dag():
    
    @task(task_id="start")
    def start_task():
        print("Bronze transform started.")
        
    @task(task_id="end", outlets=[DBT_BRONZE_ASSET])
    def end_task():
        print("Bronze transform finished. Bronze Asset emitted.")
    
    # Cosmos DbtTaskGroup 정의 - 01_bronze 폴더 모델만 선택하여 빌드
    dbt_tasks = DbtTaskGroup(
        group_id="dbt_tasks",
        project_config=get_project_config(),
        profile_config=get_profile_config(),
        execution_config=get_execution_config(),
        render_config=get_render_config(select_tag=["path:models/01_bronze"]),
        operator_args=get_operator_args()
    )
    
    start_task() >> dbt_tasks >> end_task()

run_dbt_bronze_transform_dag()

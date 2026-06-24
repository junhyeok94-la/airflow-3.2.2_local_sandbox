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

# 입출력 데이터 자산(Asset) 등록
DBT_BRONZE_ASSET = Asset(uri="clickhouse://default/dbt_bronze_transform_completed")
DBT_SILVER_ASSET = Asset(uri="clickhouse://default/dbt_silver_transform_completed")

@dag(
    dag_id=DAG_ID,
    doc_md="""
    ### dbt Silver 계층 변환 파이프라인 (dbt Silver Transform)
    Bronze 뷰 데이터를 바탕으로 비즈니스 관점의 정규화 팩트/차원 테이블(fact_orders, dim_customers 등)을 구축합니다.
    
    * **실행 스케줄**: Bronze 자산(`dbt_bronze_transform_completed`) 갱신 시 자동 반응 (Data-Aware)
    * **주요 입력**: ClickHouse Bronze 뷰 (`analytics_bronze.*`)
    * **주요 출력 및 자산(Asset)**: `clickhouse://default/dbt_silver_transform_completed`
    * **dbt 실행 범위**: `models/02_silver` 디렉토리 하위의 모델 전체
    """,
    schedule=[DBT_BRONZE_ASSET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "dbt", "silver"]
)
def run_dbt_silver_transform_dag():
    
    @task(task_id="start")
    def start_task():
        print("Silver transform started.")
        
    @task(task_id="end", outlets=[DBT_SILVER_ASSET])
    def end_task():
        print("Silver transform finished. Silver Asset emitted.")
    
    # Cosmos DbtTaskGroup 정의 - 02_silver 폴더 모델만 선택하여 빌드
    dbt_tasks = DbtTaskGroup(
        group_id="dbt_tasks",
        project_config=get_project_config(),
        profile_config=get_profile_config(),
        execution_config=get_execution_config(),
        render_config=get_render_config(select_tag=["path:models/02_silver"]),
        operator_args=get_operator_args()
    )
    
    start_task() >> dbt_tasks >> end_task()

run_dbt_silver_transform_dag()

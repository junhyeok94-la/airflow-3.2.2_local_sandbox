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
DBT_SILVER_ASSET = Asset(uri="clickhouse://default/dbt_silver_transform_completed")
CLICKHOUSE_ORDER_GOLD_ASSET = Asset(uri="clickhouse://default/mart_daily_sales_wide")

@dag(
    dag_id=DAG_ID,
    doc_md="""
    ### dbt Gold 계층 변환 파이프라인 (dbt Gold Transform)
    Silver 팩트/차원 테이블 데이터를 가공하여 일별 매출 통계 등 최종 BI 및 요약 데이터 마트를 구축합니다.
    
    * **실행 스케줄**: Silver 자산(`dbt_silver_transform_completed`) 갱신 시 자동 반응 (Data-Aware)
    * **주요 입력**: ClickHouse Silver 차원/팩트 테이블 (`analytics_silver.*`)
    * **주요 출력 및 자산(Asset)**: `clickhouse://default/mart_daily_sales_wide`
    * **dbt 실행 범위**: `models/03_gold` 디렉토리 하위의 모델 전체
    """,
    schedule=[DBT_SILVER_ASSET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "dbt", "gold"]
)
def run_dbt_gold_transform_dag():
    
    @task(task_id="start")
    def start_task():
        print("Gold transform started.")
        
    @task(task_id="end", outlets=[CLICKHOUSE_ORDER_GOLD_ASSET])
    def end_task():
        print("Gold transform finished. Gold Asset emitted.")
    
    # Cosmos DbtTaskGroup 정의 - 03_gold 폴더 모델만 선택하여 빌드
    dbt_tasks = DbtTaskGroup(
        group_id="dbt_tasks",
        project_config=get_project_config(),
        profile_config=get_profile_config(),
        execution_config=get_execution_config(),
        render_config=get_render_config(select_tag=["path:models/03_gold"]),
        operator_args=get_operator_args()
    )
    
    start_task() >> dbt_tasks >> end_task()

run_dbt_gold_transform_dag()

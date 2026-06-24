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

# 1. dbt 변환이 완료될 최종 ClickHouse 매출 마트 테이블을 상위 핵심 자산(Asset)으로 등록
CLICKHOUSE_ORDER_GOLD_ASSET = Asset(uri="clickhouse://default/mart_daily_sales_wide")

# [DAG 1] 15분 주기 주기적 배치 파이프라인 (인프라 안정성 확보)
@dag(
    dag_id=DAG_ID,
    doc_md="""
    ### 15분 마이크로 배치 DW 변환 파이프라인 (dbt Transform)
    ClickHouse 데이터 웨어하우스 상에 적재된 원천 landing 데이터를 dbt 모델(Bronze -> Silver -> Gold)로 실시간 가공하고 변환을 오케스트레이션합니다.
    
    * **실행 스케줄**: 15분 마이크로 배치 (`*/15 * * * *`)
    * **주요 입력**: ClickHouse landing 스키마 (`default.stg_olist_*`)
    * **주요 출력 및 자산(Asset)**: `clickhouse://default/mart_daily_sales_wide`
    * **동작 상세**: Cosmos DbtTaskGroup을 사용하여 dbt 프로젝트를 동적으로 Airflow Task로 매핑해 순차적으로 모델을 빌드합니다.
    """,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "dbt", "dw"]
)
def batch_dw_transform():
    
    @task(task_id="start")
    def start_task():
        print("ClickHouse dbt transform pipeline started.")
        
    @task(task_id="end", outlets=[CLICKHOUSE_ORDER_GOLD_ASSET])
    def end_task():
        print("ClickHouse dbt transform pipeline finished. Gold Asset emitted.")
    
    # Cosmos DbtTaskGroup 정의
    dbt_tasks = DbtTaskGroup(
        group_id="dbt_tasks",
        project_config=get_project_config(),
        profile_config=get_profile_config(),
        execution_config=get_execution_config(),
        render_config=get_render_config(),
        operator_args=get_operator_args()
    )
    
    start_task() >> dbt_tasks >> end_task()

batch_dw_transform()

from datetime import datetime
import sys
import os
from airflow.sdk import dag, task, Asset
from airflow.providers.common.sql.hooks.sql import DbApiHook
from cosmos import DbtTaskGroup

from plugins.dbt_common_config import (
    get_profile_config,
    get_project_config,
    get_execution_config,
    get_render_config,
    get_operator_args
)

# 1. dbt 변환이 완료될 최종 ClickHouse 매출 마트 테이블을 상위 핵심 자산(Asset)으로 등록
CLICKHOUSE_ORDER_GOLD_ASSET = Asset(uri="clickhouse://default/mart_daily_sales_wide")

# [DAG 1] 15분 주기 주기적 배치 파이프라인 (인프라 안정성 확보)
@dag(
    dag_id="batch_15min_dw_transform",
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


# [DAG 2] 데이터가 갱신되었을 때만 유기적으로 반응하는 이벤트 기반 다운스트림 (Data-Aware)
@dag(
    dag_id="reactive_stock_alert_pipeline",
    schedule=[CLICKHOUSE_ORDER_GOLD_ASSET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "alert"]
)
def reactive_stock_alert():

    @task(task_id="detect_low_stock_and_notify")
    def check_stock_leak():
        hook = DbApiHook.get_hook_by_conn_id("clickhouse_desktop")
        sql = """
            -- 품목별 최근 누적 주문량을 집계하여 재고 경보 대상 탐색 (Olist 스펙에 맞춰 수정)
            SELECT product_id, count(order_item_id) AS total_ordered 
            FROM default.fact_orders FINAL
            GROUP BY product_id
            ORDER BY total_ordered DESC
            LIMIT 10;
        """
        records = hook.get_records(sql)
        for row in records:
            print(f"Product ID: {row[0]}, Cumulative Quantity Ordered: {row[1]}")
            
    check_stock_leak()

reactive_stock_alert()

from datetime import datetime
from airflow.sdk import dag, task, Asset
from airflow.providers.common.sql.hooks.sql import DbApiHook

# 1. dbt 변환이 완료될 최종 ClickHouse 매출 마트 테이블을 상위 핵심 자산(Asset)으로 등록
CLICKHOUSE_ORDER_GOLD_ASSET = Asset(uri="clickhouse://default/fact_orders_hourly")

# [DAG 1] 15분 주기 주기적 배치 파이프라인 (인프라 안정성 확보)
@dag(
    dag_id="batch_15min_dw_transform",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "dbt", "dw"]
)
def batch_dw_transform():
    
    @task(task_id="execute_dbt_gold", outlets=[CLICKHOUSE_ORDER_GOLD_ASSET])
    def run_dbt_transform():
        # clickhouse_desktop 커넥션을 이용해 SQL 실행
        # (실제 dbt run 연동 쿼리를 모방한 SELECT 1 실행)
        hook = DbApiHook.get_hook_by_conn_id("clickhouse_desktop")
        sql = "SELECT 1;"
        hook.run(sql)
        print("ClickHouse dbt transform simulator executed successfully.")
        
    run_dbt_transform()

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
            -- 품목별 최근 누적 주문량을 집계하여 재고 경보 대상 탐색
            SELECT product_id, sum(quantity) AS total_ordered 
            FROM default.fact_orders_hourly 
            GROUP BY product_id;
        """
        records = hook.get_records(sql)
        for row in records:
            print(f"Product ID: {row[0]}, Cumulative Quantity Ordered: {row[1]}")
            
    check_stock_leak()

reactive_stock_alert()

from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task, Asset

# 파일명에서 dag_id 동적 파싱
DAG_ID = Path(__file__).stem

# 1. dbt 변환이 완료될 최종 ClickHouse 매출 마트 테이블을 상위 핵심 자산(Asset)으로 등록하여 이벤트 구독용으로 사용
CLICKHOUSE_ORDER_GOLD_ASSET = Asset(uri="clickhouse://default/mart_daily_sales_wide")

# [DAG 2] 데이터가 갱신되었을 때만 유기적으로 반응하는 이벤트 기반 다운스트림 (Data-Aware)
@dag(
    dag_id=DAG_ID,
    doc_md="""
    ### 이벤트 기반 실시간 재고 경보 파이프라인 (Reactive Stock Alert)
    상류의 dbt 마트 생성 작업이 완료되어 특정 데이터 자산(Asset)이 갱신되는 순간, 유기적으로 반응하여 품절 임박 또는 누적 주문 급증 품목을 탐색해 알림을 트리거합니다.
    
    * **실행 스케줄**: 데이터 변경 감지 기반 (Data-Aware Scheduling)
    * **구독하는 상류 자산(Asset)**: `clickhouse://default/mart_daily_sales_wide`
    * **동작 상세**: ClickHouse 연결을 통해 최신 주문 팩트(`default.fact_orders`)를 읽어 재고 경보 대상 TOP 10 품목을 추출 및 로깅합니다.
    """,
    schedule=[CLICKHOUSE_ORDER_GOLD_ASSET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "alert"]
)
def reactive_stock_alert():

    @task(task_id="detect_low_stock_and_notify")
    def check_stock_leak():
        from airflow.hooks.base import BaseHook
        hook = BaseHook.get_hook("clickhouse_desktop")
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

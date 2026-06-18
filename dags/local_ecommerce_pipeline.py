from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.sdk import Asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# 1. dbt 변환이 완료될 최종 ClickHouse 매출 마트 테이블을 상위 핵심 자산(Asset)으로 등록
CLICKHOUSE_ORDER_MART_ASSET = Asset(uri="clickhouse://default/fact_orders_hourly")

# [DAG 1] 15분 주기 주기적 배치 파이프라인 (인프라 안정성 확보)
with DAG(
    dag_id="batch_15min_dw_transform",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    }
) as dag_1:
    
    # dbt run을 ClickHouse 상에서 호출하거나 dbt-core 연동 CLI 실행을 래핑하는 SQL 오퍼레이터
    run_dbt_transform = SQLExecuteQueryOperator(
        task_id="execute_dbt_marts",
        conn_id="clickhouse_desktop",
        sql="-- TODO: 실 환경 구성 시 dbt run을 호출하기 위한 SQL 호출 정의\nSELECT 1;",
        outlets=[CLICKHOUSE_ORDER_MART_ASSET]  # 태스크 성공 시 자산 이벤트 방출
    )

# [DAG 2] 데이터가 갱신되었을 때만 유기적으로 반응하는 이벤트 기반 다운스트림 (Data-Aware)
with DAG(
    dag_id="reactive_stock_alert_pipeline",
    schedule=[CLICKHOUSE_ORDER_MART_ASSET],
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag_2:
    
    check_stock_leak = SQLExecuteQueryOperator(
        task_id="detect_low_stock_and_notify",
        conn_id="clickhouse_desktop",
        sql="""
            -- 품목별 최근 누적 주문량을 집계하여 재고 경보 대상 탐색
            SELECT product_id, sum(quantity) AS total_ordered 
            FROM default.fact_orders_hourly 
            GROUP BY product_id;
        """
    )

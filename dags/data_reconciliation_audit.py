from datetime import datetime, timedelta
import logging
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook

logger = logging.getLogger(__name__)

def reconcile_postgres_vs_clickhouse(**context):
    """
    원천 Postgres와 ClickHouse DW staging 데이터의 최근 24시간 동안의
    주문 금액 합계(total_price)를 비교하여 정합성을 교차 검증합니다.
    """
    # DAG 실행일 기준으로 최근 24시간 대역 계산
    execution_date = context["data_interval_end"]
    start_time = execution_date - timedelta(days=1)
    end_time = execution_date

    logger.info(f"정합성 대조 검증 수행 대역: {start_time} ~ {end_time}")

    # 1. Postgres Hook을 이용한 데이터 조회 (외부 포트 5433 혹은 Tailscale 바인딩)
    pg_hook = DbApiHook.get_hook_by_conn_id("postgres_desktop")
    pg_query = """
        SELECT COALESCE(SUM(total_price), 0) 
        FROM orders 
        WHERE created_at >= %s AND created_at < %s;
    """
    pg_result = pg_hook.get_first(pg_query, parameters=(start_time, end_time))
    pg_sum = float(pg_result[0]) if pg_result else 0.0

    # 2. ClickHouse Hook을 이용한 데이터 조회 (FINAL 제어어를 사용하여 백그라운드 중복 제거 반영)
    ch_hook = DbApiHook.get_hook_by_conn_id("clickhouse_desktop")
    ch_query = """
        SELECT COALESCE(SUM(total_price), 0) 
        FROM default.stg_orders FINAL 
        WHERE toDateTime64(ts_ms/1000, 3) >= %s AND toDateTime64(ts_ms/1000, 3) < %s;
    """
    # ClickHouse 쿼리 시 날짜 포맷 전달을 위해 문자열 포맷팅 처리
    ch_result = ch_hook.get_first(
        ch_query, 
        parameters=(start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S"))
    )
    ch_sum = float(ch_result[0]) if ch_result else 0.0

    logger.info(f"[Audit 결과] PostgreSQL SUM: {pg_sum} USD | ClickHouse SUM: {ch_sum} USD")

    # 3. 오차율 검증 (허용 오차 범위 0.01%)
    tolerance = 0.01
    difference = abs(pg_sum - ch_sum)
    
    if pg_sum > 0:
        error_rate = (difference / pg_sum) * 100
    else:
        error_rate = 0.0 if difference == 0 else 100.0

    if difference > tolerance:
        error_message = (
            f"[Data Reconciliation Alarm] 데이터 불일치 감지!\n"
            f"대역: {start_time} ~ {end_time}\n"
            f"PostgreSQL SUM: {pg_sum} USD\n"
            f"ClickHouse SUM: {ch_sum} USD\n"
            f"차액: {difference} USD (오차율: {error_rate:.4f}%)"
        )
        logger.error(error_message)
        # 예외를 발생시켜 DAG 실패 상태로 만들고 Slack Alert 등의 알림 연계를 유도
        raise ValueError(error_message)
    
    logger.info("정합성 검증 성공: 원천 DB와 DW의 데이터가 일치합니다.")

with DAG(
    dag_id="data_reconciliation_audit",
    schedule="0 2 * * *",  # 매일 새벽 2시 실행 (일별 배치 감사)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
) as dag:
    
    run_audit = PythonOperator(
        task_id="run_postgres_clickhouse_audit",
        python_callable=reconcile_postgres_vs_clickhouse,
    )

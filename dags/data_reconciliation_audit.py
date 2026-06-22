from datetime import datetime, timedelta
import logging
from airflow.sdk import dag, task, get_current_context
from airflow.providers.common.sql.hooks.sql import DbApiHook

logger = logging.getLogger(__name__)

@dag(
    dag_id="data_reconciliation_audit",
    schedule="0 2 * * *",  # 매일 새벽 2시 실행 (일별 배치 감사)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "audit"]
)
def data_reconciliation_audit_dag():

    @task(task_id="run_postgres_clickhouse_audit")
    def reconcile_postgres_vs_clickhouse():
        # 1. get_current_context()를 통한 안전한 콘텍스트 접근
        context = get_current_context()
        execution_date = context["data_interval_end"]
        start_time = execution_date - timedelta(days=1)
        end_time = execution_date

        logger.info(f"정합성 대조 검증 수행 대역: {start_time} ~ {end_time}")

        # 2. Postgres Hook을 이용한 데이터 조회
        pg_hook = DbApiHook.get_hook_by_conn_id("postgres_desktop")
        pg_query = """
            SELECT COALESCE(SUM(price), 0) 
            FROM olist_order_items 
            WHERE shipping_limit_date >= %s AND shipping_limit_date < %s;
        """
        pg_result = pg_hook.get_first(pg_query, parameters=(start_time, end_time))
        pg_sum = float(pg_result[0]) if pg_result else 0.0

        # 3. ClickHouse Hook을 이용한 데이터 조회 (FINAL 제어어 사용)
        ch_hook = DbApiHook.get_hook_by_conn_id("clickhouse_desktop")
        ch_query = """
            SELECT COALESCE(SUM(price), 0) 
            FROM default.stg_olist_order_items FINAL 
            WHERE shipping_limit_date >= %s AND shipping_limit_date < %s;
        """
        ch_result = ch_hook.get_first(
            ch_query, 
            parameters=(start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S"))
        )
        ch_sum = float(ch_result[0]) if ch_result else 0.0

        logger.info(f"[Audit 결과] PostgreSQL SUM: {pg_sum} USD | ClickHouse SUM: {ch_sum} USD")

        # 4. 오차율 검증 (허용 오차 범위 0.01%)
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
            raise ValueError(error_message)
        
        logger.info("정합성 검증 성공: 원천 DB와 DW의 데이터가 일치합니다.")

    reconcile_postgres_vs_clickhouse()

data_reconciliation_audit_dag()

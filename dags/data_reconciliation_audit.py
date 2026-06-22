from datetime import datetime, timedelta
import logging
from airflow.sdk import dag, task, get_current_context
from airflow.hooks.base import BaseHook

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
        context = get_current_context()
        # 3.x 호환 및 수동 트리거 에러 방지를 위해 방어적 context 획득
        execution_date = context.get("logical_date") or context.get("execution_date") or datetime.utcnow()
        
        # Olist 데이터셋 대역(2017~2018년)을 포함하여 전체 복사 무결성을 점검하도록 조회 범위 설정
        start_time = datetime(2016, 1, 1)
        end_time = datetime(2027, 1, 1)

        logger.info(f"정합성 대조 검증 수행 대역: {start_time} ~ {end_time}")

        # 2. Postgres Hook을 이용한 데이터 조회 (BaseHook.get_hook 활용)
        pg_hook = BaseHook.get_hook("postgres_desktop")
        pg_query = """
            SELECT COALESCE(SUM(price), 0) 
            FROM olist_order_items 
            WHERE shipping_limit_date >= %s AND shipping_limit_date < %s;
        """
        pg_result = pg_hook.get_first(pg_query, parameters=(start_time, end_time))
        pg_sum = float(pg_result[0]) if pg_result else 0.0

        # 3. ClickHouse 연결 (generic hook 대신 clickhouse_connect 직접 사용)
        import clickhouse_connect
        ch_conn = BaseHook.get_connection("clickhouse_desktop")
        ch_client = clickhouse_connect.get_client(
            host=ch_conn.host or 'ecommerce-clickhouse',
            port=int(ch_conn.port) if ch_conn.port else 8123,
            username=ch_conn.login or 'default',
            password=ch_conn.password or '',
            database=ch_conn.schema or 'default'
        )
        ch_query = f"""
            SELECT COALESCE(SUM(price), 0) 
            FROM analytics_silver.fact_orders FINAL 
            WHERE shipping_limit_date >= '{start_time.strftime("%Y-%m-%d %H:%M:%S")}' 
              AND shipping_limit_date < '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
        """


        ch_result = ch_client.query(ch_query)
        ch_sum = float(ch_result.result_rows[0][0]) if ch_result.result_rows else 0.0


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

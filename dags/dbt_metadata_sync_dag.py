from datetime import datetime
from pathlib import Path
import os
import subprocess
from airflow.sdk import dag, task, Asset

# 파일명에서 dag_id 동적 파싱
DAG_ID = Path(__file__).stem

# 최종 Gold 마트 Asset 정의 (이벤트 구독 대상)
CLICKHOUSE_ORDER_GOLD_ASSET = Asset(uri="clickhouse://default/mart_daily_sales_wide")

@dag(
    dag_id=DAG_ID,
    doc_md="""
    ### dbt 메타데이터 Neo4j 동기화 파이프라인 (dbt to Neo4j Sync)
    최종 dbt Gold 데이터 마트 갱신 완료 이벤트(`mart_daily_sales_wide` Asset)를 감지하여 자동으로 구동됩니다.
    최신 dbt `manifest.json` 컴파일 정보 및 컬럼 설명, Lineage(의존 관계)를 Neo4j 그래프 데이터베이스에 적재합니다.
    
    * **실행 스케줄**: Gold 마트 자산 갱신 시 자동 반응 (Data-Aware)
    * **구독하는 상류 자산(Asset)**: `clickhouse://default/mart_daily_sales_wide`
    * **동작 상세**: `dbt compile`을 실행하여 manifest.json을 최신화한 후 `dbt_to_neo4j.py` 스크립트를 로드하여 실행합니다.
    """,
    schedule=[CLICKHOUSE_ORDER_GOLD_ASSET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "dbt", "neo4j", "metadata"]
)
def run_dbt_metadata_sync_dag():

    @task(task_id="sync_dbt_to_neo4j")
    def sync_to_neo4j():
        # 컨테이너 내 dbt 프로젝트 경로 정의
        dbt_project_dir = "/opt/airflow/dbt_clickhouse_dw"
        script_path = f"{dbt_project_dir}/dbt_to_neo4j.py"
        
        # dbt executable path
        dbt_path = "/home/airflow/.local/bin/dbt"
        
        # 1. dbt docs generate 실행 (manifest.json 및 catalog.json 최신화)
        print("1. dbt docs generate 실행 중...")
        env = os.environ.copy()
        if "CLICKHOUSE_HOST" not in env:
            env["CLICKHOUSE_HOST"] = "ecommerce-clickhouse"
            
        docs_res = subprocess.run(
            [dbt_path, "docs", "generate"],
            cwd=dbt_project_dir,
            env=env,
            capture_output=True,
            text=True
        )
        print(docs_res.stdout)
        if docs_res.returncode != 0:
            print(docs_res.stderr)
            raise RuntimeError("dbt docs generate 실패!")
            
        # 2. dbt_to_neo4j.py 실행
        print("2. dbt_to_neo4j.py 실행 중...")
        # Neo4j 호스트 기본 바인딩 (컨테이너 간 통신 시 docker-compose에 neo4j가 별도로 정의 안되어있으면 host.docker.internal 또는 Tailscale IP)
        if "NEO4J_URI" not in env:
            env["NEO4J_URI"] = "bolt://neo4j-graph-rag:7687"
            
        sync_res = subprocess.run(
            ["python", script_path],
            cwd=dbt_project_dir,
            env=env,
            capture_output=True,
            text=True
        )
        print(sync_res.stdout)
        if sync_res.returncode != 0:
            print(sync_res.stderr)
            raise RuntimeError("dbt_to_neo4j.py 실행 실패!")
            
    sync_to_neo4j()

run_dbt_metadata_sync_dag()

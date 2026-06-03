from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

# Define the identical assets (to match the ones produced by upstream DAGs)
sales_asset = Asset("s3://data-lake/sales_data.csv")
inventory_asset = Asset("s3://data-lake/inventory_data.csv")


@dag(
    dag_id="asset_consumer_analysis",
    # sales_asset과 inventory_asset이 모두 업데이트되었을 때만 자동으로 트리거됩니다 (AND 조건)
    schedule=sales_asset & inventory_asset,
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["asset", "consumer", "analysis"],
)
def analysis_consumer():
    """
    [Consumer DAG]
    매출 데이터와 재고 데이터가 모두 최신 상태로 갱신 완료되면
    이를 감지하여 데이터 마트 분석 및 대시보드 리프레시 모델을 수행하는 DAG입니다.
    """

    @task
    def load_and_combine_data() -> dict:
        """S3에서 두 CSV 데이터를 로드하여 결합하는 시뮬레이션"""
        print("Trigger event detected: both Sales and Inventory assets are updated!")
        print("Reading s3://data-lake/sales_data.csv...")
        print("Reading s3://data-lake/inventory_data.csv...")
        print("Merging datasets based on Product/Category mappings...")
        
        combined_meta = {
            "sales_source": "s3://data-lake/sales_data.csv",
            "inventory_source": "s3://data-lake/inventory_data.csv",
            "combined_records": 1650,
            "status": "ready_for_ml"
        }
        return combined_meta

    @task
    def run_ml_forecast_model(combined_meta: dict) -> None:
        """데이터를 바탕으로 예측 모델 실행"""
        print(f"Loading merged metadata from: {combined_meta['sales_source']} and {combined_meta['inventory_source']}")
        print("Running Product Sales & Stockout risk prediction model...")
        print("Model execution complete.")
        print("Updating enterprise Tableau/Metabase analytical dashboards...")
        print("Process finalized successfully.")

    # 실행 흐름
    meta = load_and_combine_data()
    run_ml_forecast_model(meta)


# DAG 인스턴스화
analysis_consumer()

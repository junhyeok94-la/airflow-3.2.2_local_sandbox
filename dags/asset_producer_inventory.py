from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

# Define the inventory data Asset
inventory_asset = Asset("s3://data-lake/inventory_data.csv")


@dag(
    dag_id="asset_producer_inventory",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["asset", "producer", "inventory"],
)
def inventory_producer():
    """
    [Producer DAG]
    일별 제품 재고 변동 내역을 집계하여 S3 가상 스토리지에 적재하고,
    's3://data-lake/inventory_data.csv' Asset을 업데이트하는 비즈니스 로직입니다.
    """

    @task
    def calculate_stock_levels() -> list[dict]:
        """물류창고 시스템으로부터 재고 상태 집계"""
        print("Fetching real-time stock levels from warehouse system...")
        mock_stock = [
            {"product_id": "P101", "stock": 450, "location": "Warehouse-East"},
            {"product_id": "P102", "stock": 12, "location": "Warehouse-West"},  # Low stock
            {"product_id": "P103", "stock": 1200, "location": "Warehouse-East"},
        ]
        print(f"Inventory calculated for {len(mock_stock)} products.")
        return mock_stock

    @task(outlets=[inventory_asset])
    def save_inventory_to_s3(stock_data: list[dict]) -> str:
        """S3 버킷의 CSV 파일로 저장 및 Outlet Asset 업데이트 트리거"""
        print("Converting inventory data to CSV...")
        csv_content = "product_id,stock,location\n"
        for prod in stock_data:
            csv_content += f"{prod['product_id']},{prod['stock']},{prod['location']}\n"

        print("Writing content to s3://data-lake/inventory_data.csv ...")
        # 실제 환경에서는 boto3 등으로 업로드하게 됩니다.
        print("Upload completed successfully.")
        
        # outlets에 지정된 Asset이 업데이트 이벤트를 발행하게 됩니다.
        return "s3://data-lake/inventory_data.csv"

    # 실행 흐름 연결
    stock_levels = calculate_stock_levels()
    save_inventory_to_s3(stock_levels)


# DAG 인스턴스화
inventory_producer()

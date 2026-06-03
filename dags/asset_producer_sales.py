from __future__ import annotations

import pendulum
from airflow.sdk import Asset, dag, task

# Define the sales data Asset
sales_asset = Asset("s3://data-lake/sales_data.csv")


@dag(
    dag_id="asset_producer_sales",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["asset", "producer", "sales"],
)
def sales_producer():
    """
    [Producer DAG]
    일별 매출 정보를 추출하여 S3 가상 스토리지에 적재하고,
    's3://data-lake/sales_data.csv' Asset을 업데이트하는 비즈니스 로직입니다.
    """

    @task
    def query_sales_database() -> list[dict]:
        """데이터베이스에서 매출 거래 내역 조회"""
        print("Connecting to sales database...")
        mock_sales = [
            {"txn_id": 501, "amount": 120.0, "category": "electronics"},
            {"txn_id": 502, "amount": 45.5, "category": "books"},
            {"txn_id": 503, "amount": 350.0, "category": "furniture"},
        ]
        print(f"Queried {len(mock_sales)} transactions.")
        return mock_sales

    @task(outlets=[sales_asset])
    def save_sales_to_s3(sales_data: list[dict]) -> str:
        """S3 버킷의 CSV 파일로 변환하여 저장 및 Outlet Asset 업데이트 트리거"""
        print("Converting sales data to CSV format...")
        csv_content = "txn_id,amount,category\n"
        for txn in sales_data:
            csv_content += f"{txn['txn_id']},{txn['amount']},{txn['category']}\n"

        print("Writing content to s3://data-lake/sales_data.csv ...")
        # 실제 환경에서는 boto3 등을 활용하여 업로드하게 됩니다.
        print("Upload completed successfully.")
        
        # outlets에 지정된 Asset이 업데이트 이벤트를 발행하게 됩니다.
        return "s3://data-lake/sales_data.csv"

    # 실행 흐름 연결
    sales_transactions = query_sales_database()
    save_sales_to_s3(sales_transactions)


# DAG 인스턴스화
sales_producer()

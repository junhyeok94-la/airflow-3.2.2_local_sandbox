from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="task_sdk_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["task_sdk", "example"],
)
def task_sdk_pipeline():
    """
    Airflow 3.2 Task SDK를 사용하는 현대적인 DAG 예제입니다.
    기존의 'airflow.models.DAG' 대신 'airflow.sdk'의 '@dag'와 '@task' 데코레이터를 사용합니다.
    """

    @task
    def extract_orders() -> list[dict]:
        """주문 데이터를 추출하는 모의 태스크"""
        orders = [
            {"order_id": 1001, "item": "Premium Wireless Keyboard", "price": 89.99, "quantity": 2},
            {"order_id": 1002, "item": "Ergonomic Office Chair", "price": 249.50, "quantity": 1},
            {"order_id": 1003, "item": "USB-C Hub Multiport Adapter", "price": 35.00, "quantity": 5},
        ]
        print(f"Extracted {len(orders)} orders successfully.")
        return orders

    @task
    def transform_orders(orders: list[dict]) -> dict:
        """데이터를 가공하고 매출 합계를 계산하는 태스크"""
        total_revenue = 0.0
        processed_items = []

        for order in orders:
            item_total = order["price"] * order["quantity"]
            total_revenue += item_total
            processed_items.append({
                "order_id": order["order_id"],
                "item": order["item"],
                "item_total": item_total
            })

        result = {
            "processed_items": processed_items,
            "total_revenue": total_revenue,
            "processed_at": pendulum.now().to_iso8601_string()
        }
        print(f"Transformation complete. Total revenue calculated: ${total_revenue:.2f}")
        return result

    @task
    def load_summary(summary: dict) -> None:
        """가공된 분석 결과를 최종 로드(출력)하는 태스크"""
        print("====== Daily Order Summary ======")
        print(f"Processed At: {summary['processed_at']}")
        print(f"Total Revenue: ${summary['total_revenue']:.2f}")
        print("Processed Items:")
        for item in summary["processed_items"]:
            print(f"  - Order #{item['order_id']}: {item['item']} (Subtotal: ${item['item_total']:.2f})")
        print("================================")

    # 태스크 흐름 정의 (TaskFlow API)
    raw_orders = extract_orders()
    transformed_summary = transform_orders(raw_orders)
    load_summary(transformed_summary)


# DAG 인스턴스 생성
task_sdk_pipeline()

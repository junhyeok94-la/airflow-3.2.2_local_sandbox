from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    dag_id="taskflow_mixed_pipeline",
    schedule=None,  # 수동 실행 전용
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    tags=["mixed", "example"],
)
def taskflow_mixed_pipeline():
    """
    Airflow 3.2 Task SDK 데코레이터와 전통적인 Operator(BashOperator)를 
    혼합하여 사용하는 고급 DAG 예제입니다.
    """

    # 1. 전통적인 Operator를 사용한 태스크 정의
    start_task = BashOperator(
        task_id="start_pipeline",
        bash_command="echo 'Starting pipeline execution...'",
    )

    # 2. Task SDK 데코레이터를 사용한 파이썬 태스크 정의
    @task
    def prepare_greeting(username: str) -> str:
        message = f"Hello, {username}! Welcome to Apache Airflow 3.2.2!"
        print(message)
        return message

    @task
    def generate_bash_command(greeting_msg: str) -> str:
        # BashOperator에서 사용할 동적 셸 명령을 구성하여 반환합니다.
        cmd = f"echo 'Received Message: {greeting_msg}'"
        print(f"Generated cmd: {cmd}")
        return cmd

    # 3. 데이터 흐름 및 태스크 흐름 정의
    greeting = prepare_greeting("Developer")
    bash_cmd = generate_bash_command(greeting)

    # 4. 생성된 커맨드를전달받아 셸 스크립트를 수행하는 BashOperator
    run_final_echo = BashOperator(
        task_id="final_echo",
        bash_command=bash_cmd,
    )

    # 전체 종속성 정의
    # (start_task 완료 후 greeting 태스크가 실행되도록 설정)
    start_task >> greeting >> run_final_echo


# DAG 인스턴스 생성
taskflow_mixed_pipeline()

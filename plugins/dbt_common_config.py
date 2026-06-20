import os
import sys
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, InvocationMode
from cosmos.constants import TestBehavior
from airflow.sdk import Variable

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
DBT_HOME = f"{AIRFLOW_HOME}/dbt_clickhouse_dw"

def get_airflow_info():
    """Airflow 시간 정보 및 실행 타입을 반환하는 템플릿"""
    return {
        'data_interval_start': "{{ dag_run.conf.get('data_interval_start', dag.params.get('data_interval_start')) }}",
        'data_interval_end': "{{ dag_run.conf.get('data_interval_end', dag.params.get('data_interval_end')) }}",
        'exec_type': "{{ dag_run.conf.get('exec_type', 'manual') }}",
    }

def get_profile_config(override_target=None):
    """ProfileConfig를 반환하는 함수 (ClickHouse profile 연결)"""
    return ProfileConfig(
        profile_name="clickhouse_dw",
        target_name=override_target or "dev",  # 기본 dev 타겟
        profiles_yml_filepath=f"{DBT_HOME}/profiles.yml",
    )

def get_project_config(dynamic_param=None):
    """ProjectConfig를 반환하는 함수"""
    airflow_info = get_airflow_info()
    
    # 기본 dbt_vars 설정
    dbt_vars = {
        "data_interval_start": airflow_info['data_interval_start'],
        "data_interval_end": airflow_info['data_interval_end'],
        "exec_type": airflow_info['exec_type']
    }
    
    # dynamic_param이 전달된 경우 dbt_vars에 추가
    if dynamic_param:
        dbt_vars.update(dynamic_param)
        
    return ProjectConfig(
        dbt_project_path=DBT_HOME,
        install_dbt_deps=False,
        # Scheduler 부하 방지를 위해 manifest_path를 명시적으로 세팅
        manifest_path=f"{DBT_HOME}/target/manifest.json",
        dbt_vars=dbt_vars,
    )

def get_execution_config():
    """ExecutionConfig를 반환하는 함수"""
    # Airflow 3.2.2 컨테이너 내 airflow 사용자의 dbt 경로 지정
    return ExecutionConfig(
        dbt_executable_path="/home/airflow/.local/bin/dbt",
        invocation_mode=InvocationMode.SUBPROCESS
    )

def get_render_config(select_tag=None, exclude_tag=None, test_behavior=None):
    """RenderConfig를 반환하는 함수"""
    config = {
        'emit_datasets': False
    }
    
    if select_tag:
        config['select'] = select_tag
    if exclude_tag:
        config['exclude'] = exclude_tag

    # test_behavior 설정
    if test_behavior == 'AFTER_EACH':
        config['test_behavior'] = TestBehavior.AFTER_EACH
    elif test_behavior == 'AFTER_ALL':
        config['test_behavior'] = TestBehavior.AFTER_ALL
    elif test_behavior == 'BUILD':
        config['test_behavior'] = TestBehavior.BUILD
    else:
        config['test_behavior'] = TestBehavior.NONE

    return RenderConfig(**config)

def get_operator_args(dynamic_dbt_cmd_flags=[]):
    """dbt 실행을 위한 공통 operator args"""
    return {
        "dbt_cmd_flags": ["--no-write-json", "--no-send-anonymous-usage-stats", "--threads", "1"] + dynamic_dbt_cmd_flags,
        "fail_fast": True,
        "no_version_check": True,
    }

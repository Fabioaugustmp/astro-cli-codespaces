from __future__ import annotations

import os
from datetime import datetime

import pendulum

# Importação do operador moderno do provider Snowflake
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator # 👈 NEW IMPORT: BashOperator
from airflow.models.dag import DAG

from cosmos import DbtDag, ProjectConfig, ProfileConfig # Cosmos is kept for parsing/rendering
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# --- Parâmetros de Configuração ---
# ... (Parâmetros de S3, Snowflake, e dbt são mantidos)
AWS_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
S3_BUCKET = "ml-politicas-energeticas" 
S3_PREFIX = "inmet/"
ANOS_A_PROCESSAR = [str(y) for y in range(2021, 2026)]

SNOWFLAKE_DATABASE = "LAB_PIPELINE"
SNOWFLAKE_STAGE_SCHEMA = "RAW_STAGE"
SNOWFLAKE_WAREHOUSE = "LAB_WH_AIRFLOW"
SNOWFLAKE_STAGE_TABLE = "INMET_STAGE_RAW" 
SNOWFLAKE_EXTERNAL_STAGE_NAME = "INMET_S3_STAGE" 

DBT_PROFILE_NAME = "dbt_inmet_s3_ingestion"
DBT_VENV_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_PATH = "/usr/local/airflow/include/laboratorio_dbt" 
DBT_FINAL_SCHEMA = "CORE"


# Function is omitted for brevity, it remains the same
def get_s3_keys_to_process(s3_conn_id: str, bucket_name: str, years: list[str], prefix: str) -> list[str]:
    # ... (function body remains the same)
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    all_keys = []
    
    for year in years:
        search_prefix = f"{prefix}{year}/"
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=search_prefix)
        
        if keys:
            csv_keys = [key for key in keys if key.lower().endswith('.csv')]
            all_keys.extend(csv_keys)
    
    if not all_keys:
        print(f"Nenhum arquivo CSV encontrado no bucket {bucket_name} com o prefixo base {prefix} para os anos {years}.")

    return all_keys


with DAG(
    dag_id="s3_snowflake_inmet_elt_pipeline_mapped",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["elt", "snowflake", "dbt", "s3", "mapped"],
) as dag:
    
    # TAREFA 1: LISTAR TODOS OS ARQUIVOS (CHAVES S3)
    list_s3_files = PythonOperator(
        task_id="list_s3_files_to_process",
        python_callable=get_s3_keys_to_process,
        op_kwargs={
            "s3_conn_id": AWS_CONN_ID,
            "bucket_name": S3_BUCKET,
            "years": ANOS_A_PROCESSAR,
            "prefix": S3_PREFIX,
        },
    )

    # TAREFA 2: CARREGAMENTO S3 -> SNOWFLAKE (TAREFA MAPPEADA)
    load_s3_to_snowflake = CopyFromExternalStageToSnowflakeOperator.partial(
        task_id="copy_data_from_stage",
        
        # ✅ FIX: RESTORE ALL REQUIRED FIXED PARAMETERS HERE!
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        table=SNOWFLAKE_STAGE_TABLE,
        stage=SNOWFLAKE_EXTERNAL_STAGE_NAME, 
        schema=SNOWFLAKE_STAGE_SCHEMA,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        
        # ✅ FIX: CORRIGIDO FILE_FORMAT (Mantido da última correção)
        file_format=(
             " (TYPE = CSV, "
             "FIELD_DELIMITER = '\t', "
             "SKIP_HEADER = 8, " 
             "FIELD_OPTIONALLY_ENCLOSED_BY = '\"') "
        ),
        copy_options="ON_ERROR = 'CONTINUE'",
    ).expand(files=list_s3_files.output)

    # TAREFA PONTE (BRIDGE)
    dbt_start_bridge = EmptyOperator(
        task_id="dbt_start_execution_signal",
    )

    # TAREFA 3: TRANSFORMAÇÃO DBT (COSMOS - DEFINIÇÃO)
    # 🚨 NOTA: A DbtDag AINDA É DEFINIDA, MAS NÃO USADA NA ORQUESTRAÇÃO. 
    # Ela será renderizada no UI, mas o BashOperator fará a execução.
    profile_config = ProfileConfig(
        profile_name=DBT_PROFILE_NAME,
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id=SNOWFLAKE_CONN_ID,
            profile_args={
                "database": SNOWFLAKE_DATABASE,
                "schema": DBT_FINAL_SCHEMA,
                "warehouse": SNOWFLAKE_WAREHOUSE,
            },
        ),
    )

    dbt_transform_dag_model = DbtDag(
        dag_id="dbt_transform_layer",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        schedule=None,
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        operator_args={
            "dbt_executable_path": DBT_VENV_PATH,
            "install_deps": True,
            "full_refresh": False, 
        },
    )

    # ✅ NOVO OPERADOR DE EXECUÇÃO: BASH
    dbt_run_bash_task = BashOperator(
        task_id="dbt_run_transformations",
        # Navega para o diretório, ativa o venv, e executa dbt run
        bash_command=f"source {DBT_VENV_PATH} && "
                     f"cd {DBT_PROJECT_PATH} && "
                     f"dbt run --profile {DBT_PROFILE_NAME} --target dev",
        # Passar o conn_id do Snowflake como variável de ambiente
        env={"AIRFLOW_CONN_SNOWFLAKE_DEFAULT": SNOWFLAKE_CONN_ID}, 
        # O dbt_snowflake adapter consegue ler a conexão do Airflow se o conn_id 
        # for passado como variável de ambiente, mas isso pode ser complexo.
        # A maneira mais simples é deixar o Cosmos renderizar o profiles.yml,
        # mas como estamos evitando o link direto, o Bash é o caminho mais seguro.
    )

    # --- ORQUESTRAÇÃO CORRIGIDA FINALMENTE ---
    
    # 1. Carrega os arquivos e mapeia o carregamento.
    list_s3_files >> load_s3_to_snowflake
    
    # 2. Todas as cargas mapeadas se ligam à tarefa ponte.
    load_s3_to_snowflake >> dbt_start_bridge 
    
    # 3. A tarefa ponte (singular) dispara o BashOperator (singular), resolvendo o erro.
    dbt_start_bridge >> dbt_run_bash_task
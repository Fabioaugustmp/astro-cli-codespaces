from __future__ import annotations

import os
from datetime import datetime
import io
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook 

import pendulum

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG

from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# --- Parâmetros de Configuração ---
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
DBT_PROJECT_PATH = "/usr/local/airflow/include/dbt_inmet_s3_ingestion"
DBT_FINAL_SCHEMA = "CORE"


# --- Funções Python ---

def get_s3_keys_to_process(s3_conn_id: str, bucket_name: str, years: list[str], prefix: str) -> list[str]:
    """Lista todos os arquivos CSV no S3 para os anos especificados."""
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

def load_s3_to_snowflake_with_pandas(
    s3_key: str, 
    s3_conn_id: str, 
    bucket_name: str, 
    snowflake_conn_id: str, 
    target_table: str,
    database: str,
    schema: str,
    warehouse: str,
):
    """Lê um arquivo CSV do S3 para um DataFrame e o insere no Snowflake usando write_pandas."""
    
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    sf_hook = SnowflakeHook(
        snowflake_conn_id=snowflake_conn_id, 
        database=database, 
        schema=schema, 
        warehouse=warehouse
    )

    print(f"Iniciando processamento para a chave S3: {s3_key}")

    # 1. Obter o conteúdo do arquivo S3
    file_content = s3_hook.get_key(key=s3_key, bucket_name=bucket_name).get()['Body'].read()
    
    # 2. Ler o CSV em memória com pandas
    df = pd.read_csv(
        io.BytesIO(file_content), 
        sep='\t', 
        skiprows=8, 
        encoding='latin-1',
        low_memory=False
    )
    
    # 3. Limpar nomes de colunas
    df.columns = [col.strip().replace(' ', '_').upper() for col in df.columns]
    
    # Adicionar o nome do arquivo S3 como uma coluna de origem
    df['SOURCE_FILENAME'] = s3_key

    # 4. Inserir os dados no Snowflake
    try:
        from snowflake.connector.pandas_tools import write_pandas
        
        with sf_hook.get_conn() as conn:
            success, n_chunks, n_rows = write_pandas(
                conn, 
                df, 
                table_name=target_table, 
                database=database, 
                schema=schema, 
                warehouse=warehouse,
                overwrite=False,
                chunk_size=500000,
            )
        
        if success:
             print(f"Sucesso na inserção de {n_rows} linhas de {s3_key} na tabela {target_table}.")
        else:
            raise Exception(f"Falha ao inserir dados de {s3_key}. Chunks: {n_chunks}, Linhas: {n_rows}")

    except Exception as e:
        print(f"Erro ao inserir dados no Snowflake para {s3_key}: {e}")
        raise

# --- DEFINIÇÃO DO DAG ---

with DAG(
    dag_id="s3_snowflake_inmet_elt_pipeline_pandas_insert",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["elt", "snowflake", "dbt", "s3", "pandas", "insert", "mapped"],
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

    # TAREFA 2: CARREGAMENTO S3 (PANDAS) -> SNOWFLAKE (INSERT)
    load_s3_to_snowflake = PythonOperator.partial(
        task_id="load_data_with_pandas_to_snowflake",
        python_callable=load_s3_to_snowflake_with_pandas,
        # ✅ FIX: Pass fixed arguments directly to .partial()
        s3_conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        target_table=SNOWFLAKE_STAGE_TABLE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_STAGE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        # 'op_kwargs' is omitted here to allow it in .expand()
    ).expand(
        # ✅ FIX: Pass the dynamic argument using op_kwargs in .expand()
        op_kwargs={"s3_key": list_s3_files.output}
    )

    # TAREFA PONTE (BRIDGE)
    dbt_start_bridge = EmptyOperator(
        task_id="dbt_start_execution_signal",
    )

    # TAREFA 3: TRANSFORMAÇÃO DBT (COSMOS - DEFINIÇÃO)
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

    # OPERADOR DE EXECUÇÃO DBT: BASH
    dbt_run_bash_task = BashOperator(
        task_id="dbt_run_transformations",
        bash_command=f"source {DBT_VENV_PATH} && "
                      f"cd {DBT_PROJECT_PATH} && "
                      f"dbt run --profile {DBT_PROFILE_NAME} --target dev",
        env={"AIRFLOW_CONN_SNOWFLAKE_DEFAULT": SNOWFLAKE_CONN_ID},
    )

    # --- ORQUESTRAÇÃO ---
    
    list_s3_files >> load_s3_to_snowflake
    load_s3_to_snowflake >> dbt_start_bridge
    dbt_start_bridge >> dbt_run_bash_task
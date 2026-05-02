import os
import json
import pandas as pd
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

AIRFLOW_HOME = '/opt/airflow'
DATA_DIR = f"{AIRFLOW_HOME}/data"
SQL_DIR = f"{AIRFLOW_HOME}/dags/sql"

# Garante que a pasta SQL exista
os.makedirs(SQL_DIR, exist_ok=True)

KAGGLE_DATASET = "patelris/formula-1-complete-dataset-1950-2026"

def extract_data(**context):
    """
    Extrai os dados do Kaggle usando a API Python nativa (atuando como Hook),
    além de preparar o arquivo JSON necessário.
    """
    base_path = f"{DATA_DIR}/f1_data"
    os.makedirs(base_path, exist_ok=True)
    
    # Resgata as credenciais de forma segura do Airflow Connections
    kaggle_conn = BaseHook.get_connection('kaggle_api')
    os.environ['KAGGLE_USERNAME'] = kaggle_conn.login
    os.environ['KAGGLE_KEY'] = kaggle_conn.password
    
    from kaggle.api.kaggle_api_extended import KaggleApi
    
    # Usa a API do Kaggle (Python) para autenticar, baixar e descompactar os arquivos
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(KAGGLE_DATASET, path=base_path, unzip=True)
    
    # Mock do arquivo JSON na mesma pasta, como exigido na atividade
    json_data = {"1": "Finished", "11": "Disqualified", "3": "Accident", "4": "Collision", "5": "Engine"}
    with open(f"{base_path}/status.json", 'w') as f:
        json.dump(json_data, f)
    print(f"Arquivos CSV e JSON extraídos com sucesso em {base_path}")
    
    # Loga a estrutura de arquivos baixados para fins de debug
    for root, _, files in os.walk(base_path):
        for file in files:
            print(f"Arquivo extraído: {os.path.join(root, file)}")

def transform_data(**context):
    """
    Transforma e enriquece os dados mesclando múltiplos CSVs e um JSON,
    aplicando filtros de data definidos nos parâmetros da DAG.
    """
    base_path = f"{DATA_DIR}/f1_data"
    output_file = f"{DATA_DIR}/f1_final_transformed.csv"
    
    # Parâmetros de data fornecidos pela DAG
    start_date = context['params']['start_date']
    end_date = context['params']['end_date']
    
    # Função auxiliar para buscar os arquivos independentemente de subpastas ou case sensitivity
    def find_file(filename, search_dir):
        for root, _, files in os.walk(search_dir):
            for file in files:
                if file.lower() == filename.lower():
                    return os.path.join(root, file)
        raise FileNotFoundError(f"Arquivo '{filename}' não encontrado em {search_dir}")
    
    # 1. Busca dinâmica e Extração dos CSVs para DataFrames Pandas
    races_df = pd.read_csv(find_file("f1_races.csv", base_path))
    results_df = pd.read_csv(find_file("f1_results.csv", base_path))
    drivers_df = pd.read_csv(find_file("f1_drivers.csv", base_path))
    
    # Padronização de colunas devido a possíveis inconsistências ou typos no dataset do Kaggle
    # (ex: a coluna estar nomeada como 'racerId' ou 'race_id' em vez de 'raceId')
    col_mapping = {
        'racerId': 'raceId',
        'race_id': 'raceId',
        'driver_id': 'driverId',
        'status_id': 'statusId',
        'result_id': 'resultId'
    }
    races_df.rename(columns=col_mapping, inplace=True)
    results_df.rename(columns=col_mapping, inplace=True)
    drivers_df.rename(columns=col_mapping, inplace=True)

    # 2. Filtragem por intervalo de datas (Aplicando na tabela de corridas)
    races_df['date'] = pd.to_datetime(races_df['date'])
    mask = (races_df['date'] >= start_date) & (races_df['date'] <= end_date)
    filtered_races = races_df.loc[mask]
    
    # 3. Transformação: Merge/Join entre os DataFrames de CSV
    # Unindo Resultados com as Corridas filtradas
    merged_df = pd.merge(results_df, filtered_races, on=['season', 'round', 'race_name'], how='inner')
    # Unindo com os dados de Pilotos
    merged_df = pd.merge(merged_df, drivers_df, on='driverId', how='inner')
    
    # 4. Transformação: Leitura e Join com o arquivo JSON consolidando as informações
    with open(f"{base_path}/status.json", 'r') as f:
        status_dict = json.load(f)
    
    # Convertendo o dicionário JSON para um DataFrame para o join
    status_df = pd.DataFrame(list(status_dict.items()), columns=['statusId', 'status'])
    status_df['statusId'] = status_df['statusId'].astype(int)
    
    final_df = pd.merge(merged_df, status_df, on='status', how='left')
    
    # 5. Renomear, selecionar e limpar dados indesejados
    final_df.rename(columns={'name': 'race_name', 'date': 'race_date'}, inplace=True)
    cols_to_keep = ['season', 'round', 'race_name', 'race_date', 'driverId', 'driver_name', 'statusId', 'status']
    final_df = final_df[cols_to_keep].dropna(subset=['driverId'])
    
    # 6. Salvar em CSV limpo para a Carga (sem o cabeçalho, pois o COPY FROM com HEADER espera o cabeçalho no arquivo)
    final_df.to_csv(output_file, index=False)
    print(f"Dados transformados salvos em {output_file}")

def load_to_postgres():
    """
    Lê o template SQL para preparar o DB e insere os dados usando copy_expert.
    """
    hook = PostgresHook(postgres_conn_id='PG_HL_')
    file_path = f"{DATA_DIR}/f1_final_transformed.csv"
    sql_template_path = f"{SQL_DIR}/f1_insert_template.sql"
    
    # Lê o template SQL para criar a tabela e preparar o schema
    with open(sql_template_path, 'r') as file:
        setup_queries = file.read()
        
    hook.run(setup_queries)
    
    # Carrega os dados processados via COPY FROM (Mais eficiente que INSERTS iterativos)
    hook.copy_expert(
        sql="COPY f1_schema.results FROM stdin WITH CSV HEADER",
        filename=file_path # type: ignore
    )
    print("Dados carregados com sucesso no PostgreSQL!")

# Configurações padrão e política de Retries
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='Atividade_ETL_F1_Kaggle',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False, # Previne execuções retroativas indesejadas
    max_active_runs=1, # Previne execuções paralelas concorrentes indesejadas
    params={
        'start_date': '2010-01-01',
        'end_date': '2023-12-31'
    }
) as dag:

    # Task 1: Extração usando a API (Hook) do Kaggle
    extract_task = PythonOperator(
        task_id='extract_from_kaggle',
        python_callable=extract_data
    )

    # Task 2: Transformação
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    # Task 3: Carregamento
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )
    
    # Task 4: Limpeza (Cleanup) - Remove o diretório com os dados brutos e o final
    cleanup_task = BashOperator(
        task_id='cleanup_task',
        bash_command=f'rm -rf {DATA_DIR}/f1_data && rm -f {DATA_DIR}/f1_final_transformed.csv'
    )

    # Definição das dependências
    extract_task >> transform_task >> load_task >> cleanup_task

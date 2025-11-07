# main_transient_to_bronze.py

from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import apache_beam as beam
import logging
import sys
import os

# Importando Classes e Funções dos Helpers
from classes.get_tables_names import GetTablesName
from classes.write_to_bq import WriteToBQ 
from functions.bq_gcs_helpers import PROJETO_DESTINO, REGION_ID 
from google.cloud import bigquery

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

from functions.get_secret import get_secret, save_secret_to_temp_file

# Recupera a chave e salva temporariamente
key_data = get_secret()
key_path = save_secret_to_temp_file(key_data)

# Configura as credenciais do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

def main_transient_to_bronze():
    data_now = datetime.now().strftime("%y%m%d")
    
    # --- Configurações Dataflow (Adaptado do seu código) ---
    options = PipelineOptions(
        project='prosite-455612', # Projecto de execução (onde os workers rodam)
        runner='DataflowRunner',
        streaming=False,
        job_name=f"etl-hoteis-bronze-{data_now}",
        temp_location='gs://site_prosite/temp',
        staging_location='gs://site_prosite/staging',
        region=REGION_ID, # Usa a REGION_ID definida no helper (southamerica-east1)
        # ... (Outras configurações de worker) ...
    )
    
    # 🚨 PREVENÇÃO: Garante que o dataset de destino existe (Opção 2) 🚨
    # Usei o dataset DATASET_DESTINO (bronze_hotelaria) nos helpers.
    # É fundamental que o dataset destino já exista, pois o MERGE não o cria.
    
    # Não vamos adicionar a função de criação de dataset aqui, para manter o foco
    # no pipeline. Assumimos que o dataset bronze_hotelaria existe.
    
    with beam.Pipeline() as p:
            # 1. Listar Nomes das Tabelas
            tables = (
                p
                | "Start PCollection" >> beam.Create([None])
                | "Get Tables Names" >> beam.ParDo(GetTablesName()) # PCollection de Nomes
            )

            # 2. Processar UPSERT e Carregar no BigQuery
            (
                tables
                | "Processar e Carregar BQ" >> beam.ParDo(WriteToBQ()) # Executa LoadJob + MERGE + Limpeza
                | "Exibir Resultados" >> beam.Map(logger.info) # Imprime o status UPSERT OK ou ERRO
            )
        
if __name__ == '__main__':
    main_transient_to_bronze()
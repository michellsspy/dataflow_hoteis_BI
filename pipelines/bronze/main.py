from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import secretmanager
from google.cloud import storage
from datetime import datetime, timedelta
import apache_beam as beam
import tempfile
import logging
import sys
import os

# Importando as Funções
from functions.get_secret import get_secret, save_secret_to_temp_file

# Recupera a chave e salva temporariamente
key_data = get_secret()
key_path = save_secret_to_temp_file(key_data)

# Configura as credenciais do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

def main_transient_to_raw():
    data_now = datetime.now().strftime("%Y%m%d")
    argv = sys.argv
    options = PipelineOptions(
        project = 'etl-hoteis',
        runner = 'DataflowRunner',
        streaming = False,
        job_name = f"etl-hotelaria-{data_now}",
        temp_location = 'gs://bk-etl-hotelaria/temp',
        staging_location = 'gs://bk-etl-hotelaria/staging',
        #template_location = f'gs://bk-etl-hotelaria/templates/etl-hotelaria-{data_now}',
        autoscaling_algorithm = 'THROUGHPUT_BASED',
        worker_machine_type = 'n1-standard-4',
        num_workers = 1,
        max_num_workers = 3,
        disk_size_gb = 25,
        region = 'us-central1',
        #zone = 'us-central1-c',
        #worker_zone = 'us-central1-a',
        project_id = 'etl-hoteis',
        staging_bucket = 'bk-etl-hotelaria',
        save_main_session = False,
        #experiments = 'use_runner_v2',
        prebuild_sdk_container_engine = 'cloud_build',
        docker_registry_push_url = 'us-central1-docker.pkg.dev/etl-hoteis/etl-hotelaria/hotelaria-dev',
        sdk_container_image = 'us-central1-docker.pkg.dev/etl-hoteis/etl-hotelaria/hotelaria-dev:latest',
        sdk_location = 'container',
        requirements_file = './tx/requirements.txt',
        metabase_file = './metadata.json',
        setup_file = './setup.py',
        service_account_email = 'etl-743@etl-hoteis.iam.gserviceaccount.com'
    )
    
    with beam.Pipeline(options=options) as p:
            tables = (
                p
                | "Start PCollection" >> beam.Create([None])
                | "Get Tables Names" >> beam.ParDo(GetTablesName())
            )

            # Processo para copiar e salvar como Parquet
            (
                tables
                | "Processar Tabelas do BigQuery" >> beam.ParDo(BQToParquet())
                | "Exibir Resultados" >> beam.Map(print)
            )

        
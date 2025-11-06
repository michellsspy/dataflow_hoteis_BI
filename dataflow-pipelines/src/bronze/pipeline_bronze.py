import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
# Mude a importação para a classe PipelineOptions, não a função de dicionário
from classes.bronze_options import BronzePipelineOptions 
from classes.transforms_bq import GetFolderNames, GcsCsvToBqUpsert
from functions.utils import setup_logging
from datetime import datetime
import sys
from datetime import datetime

logger = setup_logging()

data_now = datetime.now().strftime("%Y%m%d%H%M")

def run_pipeline():
    data_now = datetime.now().strftime("%Y%m%d%H%M")
    argv = sys.argv
    options = PipelineOptions(
        project='etl-hoteis',
        runner='DataflowRunner',
        streaming=False,
        job_name=f"etl-transient-to-bronze-{data_now}",
        temp_location='gs://bk-etl-hotelaria/temp',
        staging_location='gs://bk-etl-hotelaria/staging',
        #template_location=f'gs://bk-etl-hotelaria/templates/etl-transient-to-bronze-{data_now}',
        autoscaling_algorithm='THROUGHPUT_BASED',
        worker_machine_type='n1-standard-4',
        num_workers=1,
        max_num_workers=3,
        disk_size_gb=25,
        region='us-central1',
        #zone='us-central1-c',
        #worker_zone='us-central1-a',
        project_id='etl-hoteis',
        staging_bucket='bk-etl-hotelaria',
        save_main_session=False,
        #experiments='use_runner_v2',
        prebuild_sdk_container_engine='cloud_build',
        docker_registry_push_url='us-central1-docker.pkg.dev/etl-hoteis/dataflow/bronze-hotelaria',
        sdk_container_image='us-central1-docker.pkg.dev/etl-hoteis/dataflow/bronze-hotelaria:latest',
        sdk_location='container',
        requirements_file='./requirements.txt',
        metabase_file='./metadata.json',
        setup_file='./setup.py',
        service_account_email='etl-hoteis@etl-hoteis.iam.gserviceaccount.com'
    )

    """Ponto de entrada principal para o pipeline da camada Bronze (bronze)."""
    
    # Configurações dinâmicas (que não podem ser setadas via argparse simples)
    
    bronze_options = options.view_as(BronzePipelineOptions)
    
    logger.info("Iniciando pipeline Bronze (GCS Transient -> BQ bronze Upsert)")

    # 2. Inicia a Pipeline
    with beam.Pipeline(options=options) as p:
        # 3. Lista as pastas no GCS
        folder_names = (
            p
            | "Start PCollection" >> beam.Create([None])
            | "Listar Pastas no GCS" >> beam.ParDo(
                GetFolderNames(
                    # Acessando as propriedades diretamente do objeto bronze_options
                    bucket_name=bronze_options.gcs_bucket_name,
                    prefix_to_search=bronze_options.gcs_transient_prefix
                )
            )
        )

        # 4. Executa o LOAD/MERGE para CADA NOME DE PASTA (Tabela)
        results = (
            folder_names
            | "Executar GCS-BQ Upsert" >> beam.ParDo(
                GcsCsvToBqUpsert(
                    # Acessando as propriedades diretamente do objeto bronze_options
                    projeto=bronze_options.project, # 'project' veio da classe BaseOptions
                    dataset_id=bronze_options.dataset_id,
                    bucket_name=bronze_options.gcs_bucket_name,
                    gcs_prefix=bronze_options.gcs_transient_prefix
                )
            ).with_outputs(
                GcsCsvToBqUpsert.TAG_FAILED,
                main=GcsCsvToBqUpsert.TAG_SUCCESS
            )
        )

        # 5. Log dos resultados
        (
            results[GcsCsvToBqUpsert.TAG_SUCCESS]
            | "Log Sucessos" >> beam.Map(lambda msg: logger.info(f"SUCESSO UPSERT: {msg}"))
        )

        (
            results[GcsCsvToBqUpsert.TAG_FAILED]
            | "Log Falhas" >> beam.Map(lambda msg: logger.error(f"FALHA UPSERT: {msg}"))
        )

if __name__ == '__main__':
    run_pipeline()
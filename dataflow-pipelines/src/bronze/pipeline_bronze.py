import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
# Mude a importação para a classe PipelineOptions, não a função de dicionário
from .classes.bronze_options import BronzePipelineOptions 
from .classes.transforms_bq import GetFolderNames, GcsCsvToBqUpsert
from .functions.utils import setup_logging
from .schemas.bronze_schemas import SCHEMA_MAP, PK_COLS_bronze
from datetime import datetime
import sys

logger = setup_logging()

def run_pipeline(argv=None):
    """Ponto de entrada principal para o pipeline da camada Bronze (bronze)."""
    
    # 1. Configuração de Opções
    # O PipelineOptions usa sys.argv por padrão. Ao passar argv=None, ele
    # lê automaticamente os argumentos da linha de comando (CLI ou Flex Template).
    # Passamos `options=BronzePipelineOptions(argv)` para que o BEAM
    # processe TODOS os argumentos da CLI E aplique os defaults da classe.
    options = BronzePipelineOptions(argv)
    
    # Configurações dinâmicas (que não podem ser setadas via argparse simples)
    data_now = datetime.now().strftime("%Y%m%d%H%M")
    options.view_as(PipelineOptions).job_name = f"etl-bronze-bronze-hotelaria-{data_now}"
    
    # Configurações Fixas de Infraestrutura/Runner (se não quiser que venham da CLI)
    options.view_as(PipelineOptions).runner = 'DataflowRunner'
    options.view_as(PipelineOptions).streaming = False
    options.view_as(PipelineOptions).temp_location = 'gs://bk-etl-hotelaria/temp'
    options.view_as(PipelineOptions).staging_location = 'gs://bk-etl-hotelaria/staging'
    options.view_as(PipelineOptions).template_location = 'gs://bk-etl-hotelaria/templates/template-etl-bronze.json'

    # Se a Service Account for sempre a mesma:
    options.view_as(PipelineOptions).service_account_email = 'etl-hoteis@etl-hoteis.iam.gserviceaccount.com'

    # Acessa as opções customizadas de forma CORRETA
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
                    gcs_prefix=bronze_options.gcs_transient_prefix,
                    schema_map=SCHEMA_MAP,
                    pk_map=PK_COLS_bronze
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
# src/bronze/pipeline_bronze.py

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from .classes.bronze_options import get_default_options
from .classes.transforms_bq import GetFolderNames, GcsCsvToBqUpsert
from .functions.utils import setup_logging
from .schemas.bronze_schemas import SCHEMA_MAP, PK_COLS_bronze
from datetime import datetime
import sys

logger = setup_logging()

def run_pipeline(argv=None):
    """Ponto de entrada principal para o pipeline da camada Bronze (bronze)."""
    
    # 1. Configuração de Opções
    base_options_dict = get_default_options("hotelaria")
    
    # Adicionar as opções específicas do projeto/dataset
    custom_options = {
        'dataset_id': 'bronze_hotelaria',
        'gcs_bucket_name': 'bk-etl-hotelaria',
        'gcs_transient_prefix': 'transient/'
    }

    # Combina opções e cria o objeto PipelineOptions
    options_flags = sys.argv[1:] if sys.argv[1:] else []
    all_options = {**base_options_dict, **custom_options}
    options = PipelineOptions(flags=options_flags, **all_options)
    
    logger.info("Iniciando pipeline Bronze (GCS Transient -> BQ bronze Upsert)")

    # 2. Inicia a Pipeline
    with beam.Pipeline(options=options) as p:
        # Acessa as opções customizadas
        bronze_options = options.view_as(type('CustomOptions', (object,), custom_options))

        # 3. Lista as pastas no GCS (Um PCollection de nomes de tabelas: 'source_consumos')
        folder_names = (
            p
            | "Start PCollection" >> beam.Create([None])
            | "Listar Pastas no GCS" >> beam.ParDo(
                GetFolderNames(
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
                    projeto=bronze_options.project,
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
    # O Dataflow Flex Template chama esta função
    run_pipeline()
# src/bronze/classes/bronze_options.py

from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime

class BronzePipelineOptions(PipelineOptions):
    """Opções personalizadas para o pipeline da Camada Bronze (RAW)."""
    @classmethod
    def _add_argparse_args(cls, parser):
        # Parâmetros específicos da pipeline
        parser.add_argument(
            '--dataset_id',
            required=True,
            help='BigQuery dataset de destino (ex: raw_hotelaria).'
        )
        parser.add_argument(
            '--gcs_bucket_name',
            required=True,
            help='Nome do bucket GCS (ex: bk-etl-hotelaria).'
        )
        parser.add_argument(
            '--gcs_transient_prefix',
            required=False,
            default='transient/',
            help='Prefixo para busca de arquivos transient (ex: transient/).'
        )

# Para usar as opções padrão que você listou:
def get_default_options(job_name_suffix: str) -> dict:
    """Retorna um dicionário com opções de execução padrão."""
    data_now = datetime.now().strftime("%Y%m%d%H%M")
    
    return {
        'project': 'etl-hoteis',
        'runner': 'DataflowRunner',
        'streaming': False,
        'job_name': f"etl-bronze-raw-{job_name_suffix}-{data_now}",
        'temp_location': 'gs://bk-etl-hotelaria/temp',
        'staging_location': 'gs://bk-etl-hotelaria/staging',
        # Opções de Flex Template
        'template_location': f'gs://bk-etl-hotelaria/templates/template-etl-bronze.json',
        'autoscaling_algorithm': 'THROUGHPUT_BASED',
        'worker_machine_type': 'n1-standard-4',
        'max_num_workers': 3,
        'region': 'us-central1', # Mantenha a região que você usa para o GCR/Dataflow
        'save_main_session': False,
        'sdk_location': 'container',
        # Configurações de CI/CD (usadas pelo gcloud flex-template build, mas listadas aqui)
        'service_account_email': 'etl-hoteis@etl-hoteis.iam.gserviceaccount.com' 
    }
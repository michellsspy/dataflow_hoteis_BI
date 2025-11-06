from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import argparse # Importação recomendada para melhor tipagem

class BronzePipelineOptions(PipelineOptions):
    """Opções personalizadas para o pipeline da Camada Bronze (bronze)."""
    
    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser):
        # ----------------------------------------------------
        # 1. Parâmetros Específicos do Pipeline (Business Logic)
        # OBRIGATÓRIOS para a pipeline funcionar e tipicamente passados via Template
        # ----------------------------------------------------
        parser.add_argument(
            '--dataset_id',
            required=True,
            help='BigQuery dataset de destino (ex: bronze_hotelaria).'
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

        # ----------------------------------------------------
        # 2. Defaults de Infraestrutura do Dataflow (Para rodar localmente ou fixar defaults)
        # O Dataflow flex template exige que esses sejam passados na CLI.
        # Aqui definimos defaults para rodar o script diretamente
        # ----------------------------------------------------
        parser.add_argument(
            '--project',
            default='etl-hoteis',
            help='ID do Projeto GCP.'
        )
        parser.add_argument(
            '--region',
            default='us-central1',
            help='Região do Dataflow.'
        )
        parser.add_argument(
            '--worker_machine_type',
            default='n1-standard-4',
            help='Tipo de máquina dos workers.'
        )
        parser.add_argument(
            '--max_num_workers',
            default=3,
            type=int,
            help='Máximo de workers.'
        )
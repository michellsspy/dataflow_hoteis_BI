# src/bronze/classes/pipeline_options.py

from apache_beam.options.pipeline_options import PipelineOptions

class BronzePipelineOptions(PipelineOptions):
    """
    Opções personalizadas para o pipeline da Camada Bronze.
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        """
        Adiciona argumentos específicos do pipeline ao parser.
        """
        parser.add_argument(
            '--dataset',
            required=True,
            help='BigQuery dataset de destino (ex: raw_hotelaria).'
        )
        parser.add_argument(
            '--gcs_input_path',
            required=True,
            help='Caminho de input no GCS com wildcards (ex: gs://bk-etl-hotelaria/transient/source_*/source_*.csv).'
        )

# NOTA: O Dataflow Flex Template já injeta as opções padrão (project, staging_location, etc.)
# Você pode acessá-las diretamente pelo objeto PipelineOptions.
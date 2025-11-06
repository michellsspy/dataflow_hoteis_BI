import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from src.bronze.transforms_bronze import build_bronze_transform
from src.common.logging_utils import get_logger

logger = get_logger("pipeline_bronze")


# -----------------------------------------------------
# CONFIGURAÇÃO DE PARÂMETROS DO PIPELINE
# -----------------------------------------------------
class BronzeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--project", required=True)
        parser.add_argument("--region", required=True)
        parser.add_argument("--temp_location", required=True)
        parser.add_argument("--staging_location", required=True)
        parser.add_argument("--bucket_transient", required=True)
        parser.add_argument("--dataset_raw", required=True)
        parser.add_argument("--tables", required=True)
        parser.add_argument("--delimiter", default=",")
        parser.add_argument("--encoding", default="utf-8")
        parser.add_argument("--skip_header_lines", type=int, default=1)


# -----------------------------------------------------
# FUNÇÃO PRINCIPAL DO PIPELINE BRONZE
# -----------------------------------------------------
def run(argv=None):
    """
    Executa o pipeline Bronze:
    - Lê os CSVs do bucket transient
    - Identifica automaticamente o header de cada arquivo
    - Converte em dicionário
    - Escreve no BigQuery (dataset raw)
    """
    pipeline_options = PipelineOptions(argv)
    opts = pipeline_options.view_as(BronzeOptions)

    # Ajuste: usa ';' como separador, não vírgula
    tables = [t.strip() for t in opts.tables.split(";")]

    logger.info("--------------------------------------------------")
    logger.info(f"Projeto........: {opts.project}")
    logger.info(f"Região.........: {opts.region}")
    logger.info(f"Bucket.........: {opts.bucket_transient}")
    logger.info(f"Dataset destino: {opts.dataset_raw}")
    logger.info(f"Tabelas........: {tables}")
    logger.info("--------------------------------------------------")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        build_bronze_transform(pipeline, tables, opts)

    logger.info("✅ Pipeline Bronze finalizado com sucesso!")


if __name__ == "__main__":
    run()

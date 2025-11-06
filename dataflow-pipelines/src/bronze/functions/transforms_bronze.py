import apache_beam as beam
from google.cloud import storage
from src.common.logging_utils import get_logger

logger = get_logger("transforms_bronze")


# -----------------------------------------------------
# FUNÇÃO AUXILIAR: LER HEADER DE CSV NO GCS
# -----------------------------------------------------
def get_csv_header(bucket_name: str, table_path: str, encoding: str = "utf-8") -> list:
    """
    Lê a primeira linha (header) do primeiro arquivo CSV dentro de gs://bucket/transient/<table>/.
    Retorna uma lista de nomes de colunas.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=f"transient/{table_path}/"))

    if not blobs:
        logger.warning(f"Nenhum arquivo encontrado em gs://{bucket_name}/transient/{table_path}/")
        return []

    blob = blobs[0]
    logger.info(f"Lendo header de: gs://{bucket_name}/{blob.name}")

    content = blob.download_as_text(encoding=encoding)
    header_line = content.splitlines()[0]
    return [col.strip().replace(" ", "_").lower() for col in header_line.split(",")]


# -----------------------------------------------------
# CLASSE DoFn: CONVERTE CSV PARA DICIONÁRIO
# -----------------------------------------------------
class ParseDynamicCSV(beam.DoFn):
    def __init__(self, table_name: str, columns: list, delimiter: str):
        self.table_name = table_name
        self.columns = columns
        self.delimiter = delimiter

    def process(self, element):
        try:
            values = [v.strip() for v in element.split(self.delimiter)]
            record = dict(zip(self.columns, values))
            yield record
        except Exception as e:
            logger.error(f"[{self.table_name}] Linha inválida: {element[:100]} - Erro: {str(e)}")


# -----------------------------------------------------
# TRANSFORMAÇÃO PRINCIPAL
# -----------------------------------------------------
def build_bronze_transform(pipeline, tables, opts):
    """
    Constrói dinamicamente o pipeline Bronze:
    - Lê CSVs de cada tabela no bucket transient
    - Mapeia colunas dinamicamente via header
    - Escreve no BigQuery (dataset raw)
    """
    for table in tables:
        raw_table = table.replace("source_", "raw_")

        # 1️⃣ Lê o cabeçalho real do CSV no GCS
        columns = get_csv_header(
            bucket_name=opts.bucket_transient,
            table_path=table,
            encoding=opts.encoding
        )

        if not columns:
            logger.warning(f"Pulado {table}: não foi possível ler header.")
            continue

        logger.info(f"Tabela: {table} → Colunas: {columns}")

        (
            pipeline
            | f"Read_{table}" >> beam.io.ReadFromText(
                f"gs://{opts.bucket_transient}/transient/{table}/*.csv",
                skip_header_lines=opts.skip_header_lines
            )
            | f"Parse_{table}" >> beam.ParDo(ParseDynamicCSV(table, columns, opts.delimiter))
            | f"Write_{raw_table}" >> beam.io.WriteToBigQuery(
                table=f"{opts.project}:{opts.dataset_raw}.{raw_table}",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

    return pipeline

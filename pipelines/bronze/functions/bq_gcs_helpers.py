# functions/bq_gcs_helpers.py

# --- Constantes de Configuração ---
# Estas constantes devem refletir o ambiente real
PROJETO_DESTINO = "etl-hoteis"
DATASET_DESTINO = "bronze_hotelaria" # Dataset final para as tabelas bronze
BUCKET_ORIGEM = "bk-etl-hotelaria"
PREFIXO_RAIZ_ORIGEM = "transient"
REGION_ID = "us-central1" # A região de execução do Dataflow/BigQuery

# 🚨 Chaves Primárias: Mapeamento necessário para a operação MERGE
# Assumimos que a primeira coluna do CSV é a PK (baseado na sua informação)
# Mas é mais seguro ter um mapeamento explícito (ou usar a detecção da primeira coluna)
# Vamos confiar na detecção da primeira coluna (que foi corrigida na última tentativa).


def montar_path_gcs(table_name: str) -> str:
    """Monta a URI do arquivo CSV no GCS."""
    # gs://<BUCKET>/<PASTA_RAIZ>/<NOME_TABELA>/<NOME_TABELA>.csv
    return f"gs://{BUCKET_ORIGEM}/{PREFIXO_RAIZ_ORIGEM}/{table_name}/{table_name}.csv"

def transformar_nome_tabela(source_table_name: str) -> str:
    """Substitui o prefixo 'source_' por 'bronze_'."""
    if source_table_name.startswith("source_"):
        return source_table_name.replace("source_", "bronze_", 1)
    return source_table_name 

def obter_uri_tabela_bq(project_id: str, dataset_id: str, table_name: str) -> str:
    """Monta a URI do BigQuery no formato API (projeto:dataset.tabela) para client.load_table_from_uri."""
    return f"{project_id}:{dataset_id}.{table_name}"

def obter_uri_tabela_bq_sql(project_id: str, dataset_id: str, table_name: str) -> str:
    """Monta a URI do BigQuery no formato SQL (`projeto.dataset.tabela`) para queries MERGE."""
    return f"`{project_id}.{dataset_id}.{table_name}`"
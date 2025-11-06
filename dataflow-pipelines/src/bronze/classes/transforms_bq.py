# src/bronze/classes/transforms_bq.py

from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound
from apache_beam.pvalue import TaggedOutput
import apache_beam as beam
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

# --- 1. CLASSE PARA LISTAR PASTAS NO GCS ---
class GetFolderNames(beam.DoFn):
    """Lista os nomes das pastas no GCS que contêm os CSVs."""
    def __init__(self, bucket_name: str, prefix_to_search: str):
        self.bucket_name = bucket_name
        self.prefix_to_search = prefix_to_search
        self.storage_client = None

    def setup(self):
        self.storage_client = storage.Client()
        logger.info("Cliente GCS (GetFolderNames) inicializado.")

    def process(self, element, *args, **kwargs):
        # Assumindo que a estrutura é gs://BUCKET/transient/source_*/source_*.csv
        # Queremos extrair 'source_consumos', 'source_faturas', etc.
        full_prefix = self.prefix_to_search.strip('/') + '/'
        
        try:
            iterator = self.storage_client.list_blobs(
                self.bucket_name, 
                prefix=full_prefix, 
                delimiter='/'
            )
            
            # O iterator.prefixes retorna as subpastas (ex: 'transient/source_consumos/')
            pastas_encontradas = [
                p.strip('/').split('/')[-1] # Pega apenas o último nome (ex: 'source_consumos')
                for p in iterator.prefixes
                if p.startswith(full_prefix + 'source_') # Filtra apenas pastas de origem (se houver outras)
            ]
            
            nomes_unicos = list(set(pastas_encontradas))
            logger.info(f"Pastas encontradas no GCS: {nomes_unicos}")
            
            # Emite cada nome de pasta (tabela) como um elemento da PCollection
            for nome_pasta in nomes_unicos:
                yield nome_pasta
        except Exception as e: 
            logger.error(f"Erro ao listar pastas no GCS: {e}"); 
            raise e


# --- 2. CLASSE PARA LOAD E MERGE NO BIGQUERY ---
class GcsCsvToBqUpsert(beam.DoFn):
    """
    Executa o LOAD do CSV (GCS) para uma tabela TEMP (Staging) e o MERGE 
    condicional subsequente para a tabela final (bronze) no BigQuery.
    """
    TAG_FAILED = 'failed'
    TAG_SUCCESS = 'success'

    def __init__(self, projeto: str, dataset_id: str, bucket_name: str, 
                 gcs_prefix: str, schema_map: dict, pk_map: dict):
        # Mapa de Schemas e PKs é injetado no construtor
        self.projeto = projeto
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.gcs_prefix = gcs_prefix
        self.schema_map = schema_map
        self.pk_map = pk_map
        self.storage_client = None
        self.bq_client = None

    def setup(self):
        # Clientes inicializados no setup para serem serializados uma vez por worker
        self.storage_client = storage.Client(project=self.projeto)
        self.bq_client = bigquery.Client(project=self.projeto)

    def process(self, folder_name: str, *args, **kwargs):
        now = datetime.now()
        timestamp_suffix = now.strftime('%Y%m%d%H%M%S_%f')
        
        try:
            # 1. Nomes e Caminhos
            # Ex: source_consumos/source_consumos.csv
            blob_path = f"{self.gcs_prefix.strip('/')}/{folder_name}/{folder_name}.csv"
            gcs_uri = f"gs://{self.bucket_name}/{blob_path}"
            
            # Nome da Tabela BQ de destino (Ex: bronze_consumos)
            table_name = folder_name.replace('source_', 'bronze_')
            table_id = f"{self.projeto}.{self.dataset_id}.{table_name}"
            
            # Nome da Tabela TEMP (Ex: _staging_consumos_2024...)
            temp_table_name = f"_staging_{table_name.replace('bronze_', '')}_{timestamp_suffix}"
            temp_table_id = f"{self.projeto}.{self.dataset_id}.{temp_table_name}"
            
            logger.info(f"[{table_name}] Iniciando UPSERT para: {table_id} (via staging: {temp_table_id})")

            # 2. Obter Schemas e PKs
            table_schema_final = self.schema_map.get(table_name)
            pk_cols = self.pk_map.get(table_name)
            
            if not table_schema_final or not pk_cols: 
                raise ValueError(f"Schema ou PKs não definidos para a tabela: {table_name}")
            
            # Schema para o LOAD (sem insert_date/update_date, pois o CSV não as tem)
            staging_schema = [f for f in table_schema_final if f.name not in ('insert_date', 'update_date')]

            # 3. LOAD do CSV para Staging (Gerenciamento de Arquivos)
            job_config_load = bigquery.LoadJobConfig(
                schema=staging_schema, 
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1, 
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Limpa a temp a cada execução
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                allow_quoted_newlines=True, 
                max_bad_records=10
            )
            
            job_load = self.bq_client.load_table_from_uri(gcs_uri, temp_table_id, job_config=job_config_load)
            job_load.result()
            logger.info(f"[{table_name}] Carga para staging concluída: {temp_table_id}")

            # 4. Garante que a Tabela Final (bronze) exista
            try:
                self.bq_client.get_table(table_id)
            except NotFound:
                table_ref = bigquery.Table(table_id, schema=table_schema_final)
                self.bq_client.create_table(table_ref)
                logger.info(f"[{table_name}] Tabela final {table_id} criada com schema de auditoria.")

            # 5. Construir MERGE QUERY
            current_timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
            
            # Colunas para o ON clause (PKs)
            on_clause = " AND ".join([f"T.{col} = S.{col}" for col in pk_cols])
            
            # Colunas para o UPDATE (todas, exceto PKs e insert_date/update_date)
            all_cols = [f.name for f in staging_schema]
            update_cols = [col for col in all_cols if col not in pk_cols]
            
            update_set_clause = ",\n".join([f"T.{col} = S.{col}" for col in update_cols])
            
            # Adicionar a lógica de update_date (se houver qualquer alteração)
            match_condition = [f"(T.{col} IS DISTINCT FROM S.{col})" for col in update_cols]
            match_condition_string = f"AND ({' OR '.join(match_condition)})" if update_cols else ""
            
            # Colunas de INSERT (todos os campos de staging + insert_date e NULL para update_date)
            insert_cols = ", ".join(all_cols + ['insert_date', 'update_date'])
            values_cols = ", ".join([f"S.{col}" for col in all_cols] + [f"'{current_timestamp}'", "NULL"])


            merge_query = f"""
            MERGE INTO `{table_id}` T
            USING `{temp_table_id}` S
            ON {on_clause}
            
            WHEN MATCHED {match_condition_string} THEN
                UPDATE SET
                    {update_set_clause},
                    T.update_date = '{current_timestamp}'
                    
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({values_cols})
            """

            # 6. Executar MERGE
            job_merge = self.bq_client.query(merge_query)
            job_merge.result()
            
            affected_rows = job_merge.num_dml_affected_rows if job_merge.num_dml_affected_rows is not None else 0
            logger.info(f"[{table_name}] MERGE concluído. Linhas afetadas: {affected_rows}")

            # 7. Limpar Staging
            self.bq_client.delete_table(temp_table_id, not_found_ok=True)
            
            yield TaggedOutput(self.TAG_SUCCESS, f"Upsert para {table_name} concluído (Linhas: {affected_rows}).")

        except Exception as e:
            logger.error(f"Erro GERAL no processo de UPSERT para {folder_name}: {e}", exc_info=True)
            
            # Tentar limpar staging em caso de falha
            if temp_table_id:
                try:
                    self.bq_client.delete_table(temp_table_id, not_found_ok=True)
                except Exception: pass
            
            yield TaggedOutput(self.TAG_FAILED, f"{folder_name}: Erro: {e}")
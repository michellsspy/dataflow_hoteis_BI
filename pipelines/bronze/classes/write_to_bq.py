# classes/write_to_bq.py

from google.cloud import bigquery
import apache_beam as beam
import logging
import uuid 

# Importa TUDO do helper
from functions.bq_gcs_helpers import (
    montar_path_gcs, 
    transformar_nome_tabela, 
    obter_uri_tabela_bq,
    obter_uri_tabela_bq_sql,
    PROJETO_DESTINO,
    DATASET_DESTINO,
    REGION_ID
)

logger = logging.getLogger(__name__)

class WriteToBQ(beam.DoFn):
    
    def process(self, source_table_name: str, *args, **kwargs):
        
        # 🚨 Inicializa o cliente forçando o projeto para evitar o erro 403 anterior
        client = bigquery.Client(project=PROJETO_DESTINO) 

        # 1. DEFINIÇÃO DAS URIS
        bronze_table_name = transformar_nome_tabela(source_table_name)
        gcs_uri_origem = montar_path_gcs(source_table_name)
        
        unique_suffix = str(uuid.uuid4()).replace('-', '_')
        staging_table_name = f"{bronze_table_name}_temp_{unique_suffix}"
        
        tabela_staging_uri_api = obter_uri_tabela_bq(PROJETO_DESTINO, DATASET_DESTINO, staging_table_name)
        tabela_bronze_uri_sql = obter_uri_tabela_bq_sql(PROJETO_DESTINO, DATASET_DESTINO, bronze_table_name)
        tabela_staging_uri_sql = obter_uri_tabela_bq_sql(PROJETO_DESTINO, DATASET_DESTINO, staging_table_name)
        
        table_ref_staging = client.dataset(DATASET_DESTINO, project=PROJETO_DESTINO).table(staging_table_name)

        logger.info(f"Iniciando UPSERT: {source_table_name}. Tabela TEMP: {staging_table_name}")

        try:
            # --- FASE 1: CARREGAR PARA A TABELA TEMPORÁRIA ---
            
            job_config_staging = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                max_bad_records=500,
                quote_character=None,
            )
            
            load_job_staging = client.load_table_from_uri(
                gcs_uri_origem, table_ref_staging, job_config=job_config_staging
            )
            load_job_staging.result() 
            logger.info(f"Dados carregados para STAGING temporário: {tabela_staging_uri_api}. {load_job_staging.output_rows} linhas.")
            
            
            # --- FASE 2: DETECÇÃO DO ESQUEMA, CRIAÇÃO CONDICIONAL E MONTAGEM DA QUERY ---
            
            # 2.1 Detecta o esquema da tabela temporária
            table_staging = client.get_table(table_ref_staging)
            
            if not table_staging.schema:
                raise ValueError("O esquema não foi detectado na tabela de staging.")
                
            schema_cols = [col.name for col in table_staging.schema]
            pk_column = schema_cols[0] # Assumimos a primeira coluna como PK
            logger.info(f"Chave Primária Detectada (PK): {pk_column}")
            
            
            # 🚨 NOVO BLOCO: GARANTE QUE A TABELA FINAL EXISTE (RESOLVE 404 NO MERGE) 🚨
            table_ref_bronze = client.dataset(DATASET_DESTINO, project=PROJETO_DESTINO).table(bronze_table_name)
            
            try:
                # Tenta obter a tabela. 
                client.get_table(table_ref_bronze)
                logger.info(f"Tabela de destino {bronze_table_name} já existe. Executando MERGE.")
            except Exception:
                # Se a tabela NÃO existe (404), nós a criamos.
                logger.warning(f"Tabela de destino {bronze_table_name} não encontrada. Criando pela primeira vez.")
                
                # Prepara o esquema final
                final_schema = list(table_staging.schema)
                final_schema.append(bigquery.SchemaField("insert_date", "TIMESTAMP", mode="NULLABLE"))
                final_schema.append(bigquery.SchemaField("update_date", "TIMESTAMP", mode="NULLABLE"))
                
                # Cria o objeto Table *sem* tentar definir a location no setter
                bronze_table = bigquery.Table(table_ref_bronze, schema=final_schema)
                
                # ❌ REMOVER QUALQUER LINHA QUE DIGA bronze_table.location = ...
                
                # Criar a tabela no BigQuery. O BigQuery deve herdar a location do Dataset.
                # Se o Dataset foi criado na location correta (us-central1), esta linha funciona.
                client.create_table(bronze_table)
                
                logger.info(f"Tabela de destino {bronze_table_name} criada com sucesso.")
                
            # Lógica SQL (continua, garantindo que a Tabela de destino (T) existe)
            cols_to_update = [col for col in schema_cols if col != pk_column]
            set_clauses = [f"T.{col} = S.{col}" for col in cols_to_update]
            set_clauses.append(f"T.update_date = CURRENT_TIMESTAMP()")

            insert_cols = schema_cols + ['insert_date', 'update_date']
            insert_values = [f'S.{col}' for col in schema_cols] + ['CURRENT_TIMESTAMP()', 'NULL']

            merge_query = f"""
            MERGE INTO {tabela_bronze_uri_sql} AS T
            USING {tabela_staging_uri_sql} AS S
            ON T.{pk_column} = S.{pk_column}
            
            WHEN MATCHED THEN
                UPDATE SET
                    {', '.join(set_clauses)}

            WHEN NOT MATCHED THEN
                INSERT ({', '.join(insert_cols)})
                VALUES ({', '.join(insert_values)})
            """
            
            # Executar o Job MERGE com correção da LOCATION
            query_job = client.query(merge_query, location=REGION_ID) 
            query_job.result()
            logger.info(f"MERGE UPSERT concluído para a tabela {bronze_table_name}.")
            
            
            # --- FASE 3: LIMPEZA E EMISSÃO ---
            
            client.delete_table(table_ref_staging)
            logger.info(f"Tabela temporária {table_ref_staging.table_id} excluída.")

            yield f"UPSERT OK: {bronze_table_name}"
            
        except Exception as e:
            error_message = f"FALHA CRÍTICA no UPSERT da tabela {source_table_name}. Erro: {e}"
            logger.error(error_message)
            
            try:
                # Tenta limpar a tabela temporária em caso de falha crítica
                if 'table_ref_staging' in locals():
                    client.delete_table(table_ref_staging)
                    logger.warning(f"Tabela temporária {table_ref_staging.table_id} excluída após falha.")
            except Exception as clean_error:
                logger.error(f"Falha ao limpar a tabela temporária: {clean_error}")
                
            yield error_message
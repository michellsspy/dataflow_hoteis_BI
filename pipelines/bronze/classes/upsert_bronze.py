from datetime import datetime, date
from google.cloud import bigquery
# CORREÇÃO DEFINITIVA DE IMPORTAÇÃO: Classes do BQ para Schemas e Jobs
from google.cloud.bigquery import Table, SchemaField, LoadJobConfig, WriteDisposition
from google.cloud import storage
from google.api_core import exceptions
import apache_beam as beam
import pyarrow.parquet as pq
import pandas as pd
import logging
import sys
import io

# Monitoramento Logging
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Mapeamento de Tabela para Schema (Todas as colunas de data são DATETIME) ---
SCHEMA_MAP = {
    "consumos": [
        SchemaField("consumo_id", "INTEGER", "NULLABLE"), SchemaField("reserva_id", "INTEGER", "NULLABLE"), 
        SchemaField("hospede_id", "INTEGER", "NULLABLE"), SchemaField("hotel_id", "INTEGER", "NULLABLE"), 
        SchemaField("nome_servico", "STRING", "NULLABLE"), 
        SchemaField("data_consumo", "DATETIME", "NULLABLE"), # CORRIGIDO
        SchemaField("quantidade", "INTEGER", "NULLABLE"), SchemaField("valor_total_consumo", "FLOAT", "NULLABLE"), 
        SchemaField("hora_consumo", "STRING", "NULLABLE"), 
        SchemaField("local_consumo", "STRING", "NULLABLE"), SchemaField("funcionario_responsavel", "STRING", "NULLABLE"), 
        SchemaField("insert_date", "DATETIME", "NULLABLE"), SchemaField("update_date", "DATETIME", "NULLABLE"),
    ],
    "faturas": [
        SchemaField("fatura_id", "INTEGER", "NULLABLE"), SchemaField("reserva_id", "INTEGER", "NULLABLE"), 
        SchemaField("hospede_id", "INTEGER", "NULLABLE"), 
        SchemaField("data_emissao", "DATETIME", "NULLABLE"),    # CORRIGIDO
        SchemaField("data_vencimento", "DATETIME", "NULLABLE"), # CORRIGIDO
        SchemaField("status_pagamento", "STRING", "NULLABLE"), SchemaField("forma_pagamento", "STRING", "NULLABLE"), 
        SchemaField("subtotal_estadia", "FLOAT", "NULLABLE"), SchemaField("subtotal_consumos", "FLOAT", "NULLABLE"), 
        SchemaField("descontos", "FLOAT", "NULLABLE"), SchemaField("impostos", "FLOAT", "NULLABLE"), 
        SchemaField("valor_total", "FLOAT", "NULLABLE"), 
        SchemaField("data_pagamento", "DATETIME", "NULLABLE"), # CORRIGIDO
        SchemaField("taxa_limpeza", "FLOAT", "NULLABLE"), SchemaField("taxa_turismo", "FLOAT", "NULLABLE"), 
        SchemaField("taxa_servico", "FLOAT", "NULLABLE"), SchemaField("numero_transacao", "STRING", "NULLABLE"), 
        SchemaField("insert_date", "DATETIME", "NULLABLE"), SchemaField("update_date", "DATETIME", "NULLABLE"),
    ],
    "hoteis": [
        SchemaField("hotel_id", "INTEGER", "NULLABLE"), SchemaField("nome_hotel", "STRING", "NULLABLE"), 
        SchemaField("endereco", "STRING", "NULLABLE"), SchemaField("cidade", "STRING", "NULLABLE"), 
        SchemaField("estado", "STRING", "NULLABLE"), SchemaField("estrelas", "INTEGER", "NULLABLE"), 
        SchemaField("numero_quartos", "INTEGER", "NULLABLE"), SchemaField("comodidades", "STRING", "NULLABLE"), 
        SchemaField("telefone", "STRING", "NULLABLE"), SchemaField("email_contato", "STRING", "NULLABLE"), 
        SchemaField("data_abertura", "DATETIME", "NULLABLE"), # CORRIGIDO
        SchemaField("horario_checkin", "STRING", "NULLABLE"), 
        SchemaField("horario_checkout", "STRING", "NULLABLE"), 
        SchemaField("categoria_hotel", "STRING", "NULLABLE"), SchemaField("tipo_hotel", "STRING", "NULLABLE"), 
        SchemaField("ano_fundacao", "INTEGER", "NULLABLE"), SchemaField("capacidade_total", "INTEGER", "NULLABLE"), 
        SchemaField("possui_acessibilidade", "BOOLEAN", "NULLABLE"), SchemaField("certificacoes", "STRING", "NULLABLE"), 
        SchemaField("latitude", "FLOAT", "NULLABLE"), SchemaField("longitude", "FLOAT", "NULLABLE"), 
        SchemaField("descricao_hotel", "STRING", "NULLABLE"), SchemaField("numero_funcionarios", "INTEGER", "NULLABLE"), 
        SchemaField("insert_date", "DATETIME", "NULLABLE"), SchemaField("update_date", "DATETIME", "NULLABLE"),
    ],
    "hospedes": [
        SchemaField("hospede_id", "INTEGER", "NULLABLE"), SchemaField("nome_completo", "STRING", "NULLABLE"), 
        SchemaField("cpf", "STRING", "NULLABLE"), 
        SchemaField("data_nascimento", "DATETIME", "NULLABLE"), # CORRIGIDO
        SchemaField("email", "STRING", "NULLABLE"), SchemaField("telefone", "STRING", "NULLABLE"), 
        SchemaField("estado", "STRING", "NULLABLE"), SchemaField("nacionalidade", "STRING", "NULLABLE"), 
        SchemaField("data_cadastro", "DATETIME", "NULLABLE"), # CORRIGIDO
        SchemaField("programa_fidelidade", "STRING", "NULLABLE"), SchemaField("profissao", "STRING", "NULLABLE"), 
        SchemaField("tipo_documento", "STRING", "NULLABLE"), SchemaField("numero_documento", "STRING", "NULLABLE"), 
        SchemaField("empresa", "STRING", "NULLABLE"), SchemaField("eh_viajante_frequente", "BOOLEAN", "NULLABLE"), 
        SchemaField("preferencias_hospede", "STRING", "NULLABLE"), SchemaField("restricoes_alimentares", "STRING", "NULLABLE"), 
        SchemaField("data_ultima_hospedagem", "DATETIME", "NULLABLE"), # CORRIGIDO
        SchemaField("total_hospedagens", "INTEGER", "NULLABLE"), 
        SchemaField("insert_date", "DATETIME", "NULLABLE"), SchemaField("update_date", "DATETIME", "NULLABLE"),
    ],
    "quartos": [
        SchemaField("quarto_id", "INTEGER", "NULLABLE"), SchemaField("hotel_id", "INTEGER", "NULLABLE"), 
        SchemaField("numero_quarto", "STRING", "NULLABLE"), # Mantido STRING (formato "0101")
        SchemaField("tipo_quarto", "STRING", "NULLABLE"), SchemaField("capacidade_maxima", "INTEGER", "NULLABLE"), 
        SchemaField("preco_diaria_base", "FLOAT", "NULLABLE"), SchemaField("andar", "INTEGER", "NULLABLE"), 
        SchemaField("vista", "STRING", "NULLABLE"), SchemaField("comodidades_quarto", "STRING", "NULLABLE"), 
        SchemaField("possui_ar_condicionado", "BOOLEAN", "NULLABLE"), SchemaField("tamanho_quarto", "STRING", "NULLABLE"), 
        SchemaField("status_manutencao", "STRING", "NULLABLE"), 
        SchemaField("ultima_manutencao", "DATETIME", "NULLABLE"), # CORRIGIDO
        SchemaField("eh_smoke_free", "BOOLEAN", "NULLABLE"), SchemaField("possui_kit_boas_vindas", "BOOLEAN", "NULLABLE"), 
        SchemaField("numero_camas", "INTEGER", "NULLABLE"), SchemaField("insert_date", "DATETIME", "NULLABLE"), 
        SchemaField("update_date", "DATETIME", "NULLABLE"),
    ],
    "reservas": [
        SchemaField("reserva_id", "INTEGER", "NULLABLE"), SchemaField("hospede_id", "INTEGER", "NULLABLE"), 
        SchemaField("quarto_id", "INTEGER", "NULLABLE"), SchemaField("hotel_id", "INTEGER", "NULLABLE"), 
        SchemaField("data_reserva", "DATETIME", "NULLABLE"),       # CORRIGIDO
        SchemaField("data_checkin", "DATETIME", "NULLABLE"),       # CORRIGIDO
        SchemaField("data_checkout", "DATETIME", "NULLABLE"),      # CORRIGIDO
        SchemaField("numero_noites", "INTEGER", "NULLABLE"), SchemaField("numero_adultos", "INTEGER", "NULLABLE"), 
        SchemaField("numero_criancas", "INTEGER", "NULLABLE"), SchemaField("canal_reserva", "STRING", "NULLABLE"), 
        SchemaField("status_reserva", "STRING", "NULLABLE"), 
        SchemaField("data_cancelamento", "DATETIME", "NULLABLE"),  # CORRIGIDO
        SchemaField("solicitacoes_especiais", "STRING", "NULLABLE"), SchemaField("valor_total_estadia", "FLOAT", "NULLABLE"), 
        SchemaField("motivo_viagem", "STRING", "NULLABLE"), SchemaField("motivo_cancelamento", "STRING", "NULLABLE"), 
        SchemaField("taxa_limpeza", "FLOAT", "NULLABLE"), SchemaField("taxa_turismo", "FLOAT", "NULLABLE"), 
        SchemaField("avaliacao_hospede", "FLOAT", "NULLABLE"), SchemaField("comentarios_hospede", "STRING", "NULLABLE"), 
        SchemaField("insert_date", "DATETIME", "NULLABLE"), SchemaField("update_date", "DATETIME", "NULLABLE"),
    ],
    "reservas_ota": [
        SchemaField("ota_reserva_id", "INTEGER", "NULLABLE"), SchemaField("reserva_id", "INTEGER", "NULLABLE"), 
        SchemaField("ota_codigo_confirmacao", "STRING", "NULLABLE"), SchemaField("ota_nome_convidado", "STRING", "NULLABLE"), 
        SchemaField("total_pago_ota", "FLOAT", "NULLABLE"), SchemaField("taxa_comissao", "FLOAT", "NULLABLE"), 
        SchemaField("valor_liquido_recebido", "FLOAT", "NULLABLE"), SchemaField("ota_solicitacoes_especificas", "STRING", "NULLABLE"), 
        SchemaField("insert_date", "DATETIME", "NULLABLE"), SchemaField("update_date", "DATETIME", "NULLABLE"),
    ],
}


class ParquetToBQ(beam.DoFn):
    
    def process(self, table_name, *args, **kwargs):
        projeto = "etl-hoteis"
        dataset = "bronze_hotelaria"
        bucket_name = "bk-etl-hotelaria"
        
        keys_id = {
            "hoteis": "hotel_id", "quartos": "quarto_id", "hospedes": "hospede_id", 
            "reservas": "reserva_id", "consumos": "consumo_id", "faturas": "fatura_id", 
            "reservas_ota": "ota_reserva_id"
        }
        
        if table_name not in keys_id or table_name not in SCHEMA_MAP:
            logger.error(f"Configuração ausente para a tabela: '{table_name}'.")
            return

        target_schema = SCHEMA_MAP[table_name]

        # --- 1. Leitura da Tabela Parquet do GCS ---
        df_new = self._read_parquet_from_gcs(bucket_name, table_name)
        if df_new.empty:
            logger.warning(f"DataFrame vazio. Abortando.")
            return

        # --- 2. Adição de Colunas de Controle e Conversão de Tipos ---
        df_new = self._add_control_columns(df_new, target_schema)
        
        # --- 3. Lógica de Upsert (Merge) no BigQuery ---
        self._upsert_to_bigquery(projeto, dataset, table_name, df_new, keys_id[table_name], target_schema)

        yield f"Processo de Upsert concluído para a tabela {dataset}.{table_name}"

    def _read_parquet_from_gcs(self, bucket_name, table_name):
        """Lê o arquivo Parquet do GCS diretamente para um DataFrame Pandas."""
        blob_name = f"transient/source_{table_name}.parquet"
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        try:
            buffer = io.BytesIO()
            blob.download_to_file(buffer)
            buffer.seek(0)
            
            table = pq.read_table(buffer)
            df = table.to_pandas()
            logger.info(f"Arquivo {blob_name} lido com sucesso. Linhas: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"Erro ao ler Parquet do GCS: {e}")
            return pd.DataFrame()


    def _add_control_columns(self, df, schema):
        """
        Cria colunas de controle e força a conversão das colunas de data/hora
        para o tipo datetime do Pandas (datetime64[ns]).
        """
        data_atual = datetime.now()
        DATA_PADRAO_UPDATE = datetime(1991, 1, 1, 0, 0, 0) 
        
        # Popula colunas de auditoria (DATETIME)
        df['insert_date'] = data_atual
        df['update_date'] = DATA_PADRAO_UPDATE
        
        for field in schema:
            col_name = field.name
            col_type = field.field_type
            
            # AÇÃO CRÍTICA: Converter STRINGs de data/hora para datetime64[ns]
            if col_type in ['DATETIME', 'TIMESTAMP']: 
                # Converte strings de data de origem para o tipo datetime do Pandas
                try:
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce') 
                except Exception as e:
                    logger.warning(f"Falha na conversão de {col_name} (para {col_type}): {e}")
            
            # Lidando com INTEGERs que vieram como STRINGs (ex: "0101")
            # Isso é necessário porque o Parquet pode salvar INTs como strings se o dtype for Object
            if col_type == 'INTEGER' and df[col_name].dtype == object:
                 try:
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').astype('Int64')
                 except:
                    logger.warning(f"Não foi possível converter a coluna {col_name} para INTEGER.")

            # Conversão de BOOLEAN (para lidar com NULLs corretamente no BQ)
            if col_type == 'BOOLEAN':
                df[col_name] = df[col_name].astype('boolean') 
        
        logger.info(f"Colunas de controle e tipagem ajustadas.")
        return df

    def _upsert_to_bigquery(self, project, dataset, table_name, df_new, key_column, target_schema):
        client = bigquery.Client(project=project)
        target_table_id = table_name
        target_table_ref = f"{project}.{dataset}.{target_table_id}"
        temp_table_id = f"temp_{target_table_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        temp_table_ref = f"{dataset}.{temp_table_id}"
        
        # --- 1. Upload para Tabela Temporária (SOURCE) ---
        logger.info(f"Iniciando upload para a tabela temporária: {temp_table_ref}")
        
        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            schema=target_schema, # USANDO SCHEMA EXPLÍCITO
        )
        
        try:
            client.load_table_from_dataframe(
                df_new, temp_table_ref, job_config=job_config
            ).result()
            logger.info(f"Upload para a tabela temporária concluído. Linhas: {len(df_new)}")
        except Exception as e:
            try: client.delete_table(temp_table_ref)
            except exceptions.NotFound: pass
            logger.error(f"Falha ao carregar DF para tabela temporária: {e}")
            return

        # --- 2. Garante a Tabela Alvo (TARGET) ---
        try:
            client.get_table(target_table_ref)
            logger.info(f"Tabela alvo {target_table_ref} já existe. Prosseguindo para MERGE.")
            
        except exceptions.NotFound:
            logger.warning(f"Tabela alvo {target_table_ref} não encontrada. Criando.")
            
            table_to_create = Table(target_table_ref, schema=target_schema)
            client.create_table(table_to_create)
            logger.info(f"Tabela {target_table_ref} criada.")

            query_insert = f"""
            INSERT INTO `{target_table_ref}`
            SELECT * FROM `{project}.{temp_table_ref}`
            """
            client.query(query_insert).result()
            logger.info(f"Dados iniciais inseridos em {target_table_ref}.")

            client.delete_table(temp_table_ref)
            logger.info(f"Tabela temporária {temp_table_ref} excluída.")
            return 
            
        # --- 3. Query MERGE (Upsert) ---
        
        update_cols = [field.name for field in target_schema if field.name not in [key_column, 'insert_date', 'update_date']]
        
        update_set_statements = ", ".join([f"T.{col} = S.{col}" for col in update_cols])
        # Usamos CURRENT_DATETIME() para corresponder ao tipo DATETIME do schema
        update_set_statements += f", T.update_date = CURRENT_DATETIME()" 

        insert_cols = ", ".join([field.name for field in target_schema])
        insert_vals = ", ".join([f"S.{col}" for col in [field.name for field in target_schema]])

        merge_query = f"""
        MERGE INTO `{target_table_ref}` T
        USING `{project}.{temp_table_ref}` S
        ON T.{key_column} = S.{key_column}

        WHEN MATCHED AND (
            {' OR '.join([f"T.{col} IS DISTINCT FROM S.{col}" for col in update_cols])}
        ) THEN
            UPDATE SET 
                {update_set_statements}

        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
        """

        # Execução
        logger.info("Executando operação MERGE (Upsert) no BigQuery...")
        query_job = client.query(merge_query)
        query_job.result()
        logger.info(f"MERGE concluído. Rows processadas: {query_job.num_dml_affected_rows}")

        # --- 4. Limpeza da Tabela Temporária ---
        client.delete_table(temp_table_ref)
        logger.info(f"Tabela temporária {temp_table_ref} excluída.")
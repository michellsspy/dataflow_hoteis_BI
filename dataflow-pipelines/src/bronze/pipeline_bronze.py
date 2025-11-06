# src/bronze/pipeline_bronze.py

import apache_beam as beam
from apache_beam.io import ReadFromText, FileIO
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
import logging
from typing import Dict, Any, Tuple
import uuid

# Importa as classes e funções que criamos
from .classes.pipeline_options import BronzePipelineOptions
from .classes.schemas import get_bq_schema, AUDIT_FIELDS
from .functions.transforms_bronze import AddAuditColumns, extract_table_name

# Configuração de Logs
logging.basicConfig(level=logging.INFO)

# --------------------------------------------------------------------------
# CLASSE PARA O UPSERT NO BIGQUERY (MERGE SQL)
# --------------------------------------------------------------------------
class WriteToBigQueryWithUpsert(beam.PTransform):
    """
    PTransform personalizado para escrever dados em uma tabela BigQuery 
    usando o padrão MERGE SQL para Upsert (Insert/Update).
    """
    def __init__(self, project: str, dataset: str, table_name: str, schema: Any):
        super().__init__()
        self.project = project
        self.dataset = dataset
        self.table_name = table_name
        self.schema = schema
        self.full_table_name = f'{project}:{dataset}.{table_name}'

    def expand(self, pcoll):
        """
        Expande a PTransform para realizar o Upsert.
        A PCollection de entrada é escrita em uma tabela temporária.
        Em seguida, é executado um comando MERGE SQL.
        """
        
        # 1. Gera um nome único para a tabela temporária (Staging)
        temp_table_name = f'temp_{self.table_name}_{uuid.uuid4().hex[:8]}'
        full_temp_table_name = f'{self.project}:{self.dataset}.{temp_table_name}'

        logging.info(f"Escrevendo dados de staging para: {full_temp_table_name}")

        # 2. Escreve a PCollection na Tabela Temporária
        _ = (
            pcoll
            | f'Write_{self.table_name}_Temp' >> beam.io.WriteToBigQuery(
                table=full_temp_table_name,
                schema=self.schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE, # Sempre sobrescreve a temporária
                additional_bq_parameters={'timePartitioning': {'type': 'DAY'}} # Particionamento opcional
            )
        )
        
        # 3. Executa a transformação que fará o MERGE (SQL DDL)
        # O Merge será executado após a escrita da tabela temporária.
        return (
            pcoll.pipeline 
            | f'Merge_{self.table_name}' >> beam.ParDo(ExecuteMergeSQL(
                target_table=self.full_table_name,
                source_table=full_temp_table_name,
                project=self.project,
                dataset=self.dataset,
                schema=self.schema 
            ))
        )

class ExecuteMergeSQL(beam.DoFn):
    """
    Um DoFn que executa o comando BigQuery MERGE SQL.
    """
    def __init__(self, target_table: str, source_table: str, project: str, dataset: str, schema: Any):
        self.target_table = target_table
        self.source_table = source_table
        self.project = project
        self.dataset = dataset
        self.schema = schema

    def start_bundle(self):
        # Inicializa o cliente BQ (BigQueryWrapper é uma ferramenta do Beam)
        # OBS: Em produção, usar o cliente BQ padrão do Google é mais comum.
        # Aqui, usaremos a Wrapper para simplicidade no ambiente Dataflow.
        self.bq_wrapper = BigQueryWrapper()

    def process(self, element=None):
        """
        Executa o MERGE. O elemento de entrada é ignorado.
        """
        
        # Cria a lista de colunas que precisam ser atualizadas (todas exceto chaves e insert_date)
        update_fields = [
            f.name for f in self.schema.fields 
            if f.name not in ['primary_key_business', 'insert_date']
        ]
        
        # Constrói a lista de atribuições SET para UPDATE
        set_statements = ", ".join([f'T.{field} = S.{field}' for field in update_fields])
        
        # Constrói a lista de colunas para INSERT
        insert_fields = ", ".join([f.name for f in self.schema.fields])
        insert_values = ", ".join([f'S.{f.name}' for f in self.schema.fields])
        
        # Chave de Negócio para o MERGE
        pk_field = 'primary_key_business' # Assumindo este nome da classe schemas.py
        
        # Construção da QUERY MERGE
        merge_query = f"""
        MERGE INTO {self.target_table} T
        USING {self.source_table} S
        ON T.{pk_field} = S.{pk_field}
        WHEN MATCHED THEN
            UPDATE SET 
                {set_statements}, 
                T.update_date = S.update_date
        WHEN NOT MATCHED THEN
            INSERT ({insert_fields})
            VALUES ({insert_values});
        """

        logging.info(f"Executando MERGE SQL para {self.target_table}: \n{merge_query}")
        
        # Executa o Job no BigQuery
        self.bq_wrapper.execute_query(
            query=merge_query,
            project=self.project,
            use_legacy_sql=False,
            query_job_config={'dryRun': False}
        )
        
        # Limpa a tabela temporária (opcional, mas recomendado)
        self.bq_wrapper.delete_table(self.project, self.dataset, self.source_table.split('.')[-1])

# --------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# --------------------------------------------------------------------------
def run():
    # 1. Leitura das Opções
    options = PipelineOptions()
    bronze_options = options.view_as(BronzePipelineOptions)
    
    # Adiciona as opções de Dataflow padrão
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    # 2. Criação do Pipeline
    with beam.Pipeline(options=options) as p:
        
        # 3. Leitura de Múltiplos Arquivos CSV
        # Usamos FileIO.match_all() para encontrar todos os arquivos
        # E ReadMatches para ler o conteúdo de cada arquivo
        
        # (Tuple[str, str]) -> (Nome da Tabela, Linha do CSV)
        
        # A. Faz o matching dos arquivos e extrai o nome da tabela
        file_matches = (
            p 
            | 'MatchFiles' >> FileIO.match_all(bronze_options.gcs_input_path)
            | 'ReadMatches' >> FileIO.read_matches()
            | 'ParseFiles' >> beam.Map(lambda file_metadata: (
                extract_table_name(file_metadata.metadata.path), 
                file_metadata.open() # Abre o arquivo
            ))
        )
        
        # B. Transforma os arquivos abertos em linhas (para cada tabela)
        # O resultado é uma PCollection de tuplas (table_name, data_dict)
        data_by_table = (
            file_matches
            # ReadFromText dentro de um ParDo para processar o FileHandle aberto
            | 'ReadCSVContents' >> beam.FlatMap(lambda table_file: (
                (table_file[0], line) 
                for line in table_file[1].readlines()
            ))
            # O primeiro elemento de cada arquivo é o cabeçalho, precisamos remover
            # NOTE: Esta é uma implementação simplificada. Em produção, use um 
            # módulo de parse CSV robusto que lide com cabeçalhos automaticamente.
            | 'FilterHeader' >> beam.Filter(lambda table_line: not table_line[1].startswith("id_")) # Assumindo que id_ é o começo do cabeçalho
            # Transforma a linha CSV em um Dicionário (Key: Value)
            # Esta parte é complexa sem saber os cabeçalhos. Vamos usar a TENTATIVA
            # de um parser simples para CSV (você deve ajustar isso!).
            | 'ParseCSVToDict' >> beam.Map(lambda table_line: (
                table_line[0], # Nome da Tabela
                dict(zip(
                    ['col1', 'col2', 'col3', 'col4', 'col5'], # Placeholder de Colunas. PRECISA SER AJUSTADO!
                    table_line[1].decode('utf-8').strip().split(',')
                ))
            ))
            # Adiciona as colunas de auditoria
            | 'AddAuditFields' >> beam.Map(lambda table_data: (
                table_data[0], # Nome da Tabela
                next(iter(p | beam.Create([table_data[1]]) | beam.ParDo(AddAuditColumns())))
            ))
            # Agrupa os dados por nome da tabela
            | 'GroupDataByTable' >> beam.GroupByKey() # Resultado: (table_name, Iterable[data_dict])
        )
        
        # 4. Escreve os dados no BigQuery (para cada tabela)
        # O PCollection é agora um (table_name, [data_dict, ...])
        def write_per_table(table_group: Tuple[str, Any]):
            table_name, records = table_group
            
            # Obtém o schema BQ para esta tabela
            schema = get_bq_schema(table_name)
            
            # Converte o Iterable de volta para PCollection
            records_pcoll = p | f'CreatePCollection_{table_name}' >> beam.Create(list(records))
            
            # Executa a escrita com o Upsert (MERGE)
            records_pcoll | f'Write_{table_name}_Upsert' >> WriteToBigQueryWithUpsert(
                project=bronze_options.project,
                dataset=bronze_options.dataset,
                table_name=table_name,
                schema=schema
            )

        data_by_table | 'ExecuteWrites' >> beam.Map(write_per_table)


if __name__ == '__main__':
    run()
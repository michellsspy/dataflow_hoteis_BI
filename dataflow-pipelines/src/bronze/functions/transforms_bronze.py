# src/bronze/functions/transforms_bronze.py

import apache_beam as beam
from datetime import datetime
import csv
from io import StringIO
import json

class AddAuditColumns(beam.DoFn):
    """
    Uma transformação (DoFn) para adicionar as colunas de auditoria (insert_date e update_date)
    a cada linha de dados.
    """
    def process(self, element: dict):
        """
        Adiciona as colunas de controle.
        A data atual será usada para o 'update_date'.
        O 'insert_date' é definido apenas se o registro for novo (o que será garantido
        pela lógica de upsert que irá rodar em cima disso).
        Aqui, preenchemos as duas com a data/hora da execução do pipeline.
        A lógica de Upsert/Merge no BigQuery garantirá o comportamento desejado.
        
        Args:
            element (dict): Um dicionário representando uma linha de dados.
            
        Yields:
            dict: O dicionário com as colunas de auditoria adicionadas.
        """
        
        # Gera a data/hora atual no formato ISO para BigQuery TIMESTAMP
        current_timestamp = datetime.now().isoformat()
        
        # O 'insert_date' inicialmente recebe a data de processamento.
        # Ele só será alterado no BigQuery se o registro for NOVO.
        element['insert_date'] = current_timestamp 
        
        # O 'update_date' inicialmente recebe a data de processamento.
        # Ele será usado pelo Merge SQL para verificar se houve alteração e atualizar.
        element['update_date'] = current_timestamp
        
        # Gera o 'primary_key_bronze' com o hash de todas as colunas existentes para fácil merge
        # Como não sabemos as chaves primárias dos arquivos CSV, usamos todas as colunas.
        # Se soubéssemos a chave primária (ex: 'id_reserva'), usaríamos apenas ela.
        # IMPORTANTE: Para o upsert, precisaremos de uma chave única!
        # Vamos assumir que o BigQuery fará o merge baseado em alguma coluna de negócio que
        # será mapeada na classe de Schema. POR HORA, vamos usar uma chave de hash de todas 
        # as colunas, mas isso deve ser ajustado.
        
        # Exemplo Simples de Chave Única (NÃO RECOMENDADO para Upsert, mas para demonstrar transformação)
        # Vamos apenas criar um campo de ID que será crucial para o MERGE
        element['primary_key_bronze'] = hash(json.dumps(element, sort_keys=True))
        
        yield element

class CsvStringToDict(beam.DoFn):
    """
    Transforma a linha de CSV (string) em um dicionário (dict), usando os cabeçalhos.
    """
    def process(self, element: str):
        # O Apache Beam geralmente lida com strings lidas do GCS.
        # Assumimos que a primeira linha de cada arquivo é o cabeçalho.
        # A lógica da pipeline_bronze.py precisará extrair e passar o cabeçalho.
        
        # Como não temos o cabeçalho aqui, e o Dataflow FileIO já faz a leitura 
        # com os cabeçalhos se usarmos a função correta de parse, 
        # vamos adaptar esta classe para a estrutura do FileIO.
        
        # Se o FileIO for configurado para retornar dicts (o que é o ideal para CSV),
        # esta classe não será necessária.
        
        # POR ENQUANTO, vamos considerar que a leitura retorna um dicionário (key:valor)
        # onde a chave é o nome da coluna do CSV.
        
        # Vamos assumir que a leitura do pipeline principal (pipeline_bronze.py)
        # entregará os dicionários prontos (key=nome da coluna, value=valor).
        
        # Esta classe fica como um placeholder para demonstração de um DoFn.
        # Se você precisar de lógica específica de parse de CSV aqui, 
        # ela deverá ser implementada.
        
        # Por enquanto, retornamos o elemento.
        yield element

# Função de ajuda para extrair o nome da tabela do caminho GCS
def extract_table_name(file_path: str) -> str:
    """
    Extrai o nome da tabela BigQuery do caminho do arquivo GCS.
    Substitui 'source_' por 'bronze_'.
    
    Args:
        file_path (str): O caminho completo do arquivo no GCS.
        
    Returns:
        str: O nome da tabela BigQuery.
    """
    # Ex: gs://bk-etl-hotelaria/transient/source_consumos/source_consumos.csv
    # Queremos 'bronze_consumos'
    
    # 1. Pega apenas o nome do arquivo
    file_name = file_path.split('/')[-1] # source_consumos.csv
    
    # 2. Remove a extensão
    table_base_name = file_name.replace('.csv', '') # source_consumos
    
    # 3. Substitui 'source_' por 'bronze_'
    table_name = table_base_name.replace('source_', 'bronze_')
    
    return table_name
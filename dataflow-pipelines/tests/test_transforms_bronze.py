# tests/test_transforms_bronze.py

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import re
import sys
import os

# 🎯 CORREÇÃO DE ARQUITETURA E IMPORT:
# 1. Adiciona a pasta 'src' ao sys.path para o Python reconhecer os pacotes.
# O diretório atual é 'dataflow-pipelines/tests', então subimos um nível (..),
# e entramos em 'src'.
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# 2. O import agora reflete a nova arquitetura (assumindo transforms.py em classes/)
# Se o arquivo for transforms_bq.py dentro de classes, ajuste o import:
# NOTE: Você precisa ter certeza de que AddAuditColumns e extract_table_name estão neste novo arquivo!
from bronze.classes.transforms_bq import AddAuditColumns, extract_table_name 

# Expressão regular para verificar o formato do timestamp ISO
TIMESTAMP_REGEX = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}'

# FUNÇÃO DE ASSERÇÃO GLOBAL: (Corrigida para não usar tipagem complexa)
def check_audit_columns(actual):
    """
    Função global de asserção para verificar se os registros contêm as colunas
    de auditoria e se os tipos estão corretos.
    """
    
    # Converte o Iterable para uma lista para contagem e iteração segura.
    actual_list = list(actual) 
    
    # 1. Verifica se o número de registros está correto
    assert len(actual_list) == 2, f"Esperado 2 registros, encontrado {len(actual_list)}"
    
    for record in actual_list:
        # 2. Verifica se as colunas originais estão presentes
        assert 'id_hospede' in record, "Falta a chave 'id_hospede'"
        assert 'cpf' in record, "Falta a chave 'cpf'"
        
        # 3. Verifica a presença das colunas de auditoria
        assert 'insert_date' in record, "Falta a coluna 'insert_date'"
        assert 'update_date' in record, "Falta a coluna 'update_date'"
        assert 'primary_key_bronze' in record, "Falta a coluna 'primary_key_bronze'"
        
        # 4. Verifica o formato do timestamp (essencial)
        assert re.match(TIMESTAMP_REGEX, record['insert_date']), \
            f"Formato de insert_date inválido: {record['insert_date']}"
        assert re.match(TIMESTAMP_REGEX, record['update_date']), \
            f"Formato de update_date inválido: {record['update_date']}"
            
        # 5. Verifica se a chave de hash é um tipo inteiro 
        assert isinstance(record['primary_key_bronze'], int), \
            f"primary_key_bronze não é inteiro, é {type(record['primary_key_bronze'])}"


class TestBronzeTransforms(unittest.TestCase):
    
    # --- Testes da Função extract_table_name ---
    
    def test_extract_table_name_consumos(self):
        """Testa a extração do nome da tabela para consumos."""
        file_path = "gs://bk-etl-hotelaria/transient/source_consumos/source_consumos.csv"
        expected = "bronze_consumos"
        self.assertEqual(extract_table_name(file_path), expected)

    def test_extract_table_name_reservas_ota(self):
        """Testa a extração do nome da tabela para reservas_ota."""
        file_path = "gs://bk-etl-hotelaria/transient/source_reservas_ota/source_reservas_ota.csv"
        expected = "bronze_reservas_ota"
        self.assertEqual(extract_table_name(file_path), expected)
        
    def test_extract_table_name_with_subpath(self):
        """Testa a extração mesmo com uma subpasta extra."""
        file_path = "gs://bk-etl-hotelaria/transient/sub/source_hoteis/source_hoteis.csv"
        expected = "bronze_hoteis"
        self.assertEqual(extract_table_name(file_path), expected)

    # --- Testes da Transformação AddAuditColumns (Apache Beam) ---
    
    def test_add_audit_columns_logic(self):
        """
        Testa a transformação de adição de colunas de auditoria no pipeline.
        """
        test_data = [
            {'id_hospede': 'H123', 'nome': 'Maria', 'cpf': '111.222.333-44'},
            {'id_hospede': 'H456', 'nome': 'João', 'cpf': '555.666.777-88'},
        ]
        
        with TestPipeline() as p:
            output = (
                p 
                | 'CreateData' >> beam.Create(test_data)
                | 'AddAudit' >> beam.ParDo(AddAuditColumns())
            )

            # Executa a verificação usando a função global.
            assert_that(output, check_audit_columns)

# Ponto de entrada para o unittest se for executado diretamente
if __name__ == '__main__':
    unittest.main()
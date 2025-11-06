# tests/test_transforms_bronze.py

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

# Importa as funções e classes do nosso módulo de transformações
# Assumimos que o path do projeto está configurado corretamente para o import relativo
from src.bronze.functions.transforms_bronze import AddAuditColumns, extract_table_name
import datetime
import re

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
        Testa se as colunas insert_date, update_date e primary_key_bronze
        são adicionadas corretamente.
        """
        # Entrada de dados (como se tivesse vindo do parser CSV)
        test_data = [
            {'id_hospede': 'H123', 'nome': 'Maria', 'cpf': '111.222.333-44'},
            {'id_hospede': 'H456', 'nome': 'João', 'cpf': '555.666.777-88'},
        ]
        
        # Expressão regular para verificar o formato do timestamp ISO
        timestamp_regex = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}'
        
        # Cria um pipeline de teste
        with TestPipeline() as p:
            # Aplica a transformação
            output = (
                p 
                | 'CreateData' >> beam.Create(test_data)
                | 'AddAudit' >> beam.ParDo(AddAuditColumns())
            )

            # Assertions: Verifica cada elemento da saída
            def check_results(actual):
                self.assertEqual(len(actual), 2)
                
                for record in actual:
                    # 1. Verifica se as colunas originais estão presentes
                    self.assertIn('id_hospede', record)
                    self.assertIn('cpf', record)
                    
                    # 2. Verifica se as colunas de auditoria foram adicionadas
                    self.assertIn('insert_date', record)
                    self.assertIn('update_date', record)
                    self.assertIn('primary_key_bronze', record)
                    
                    # 3. Verifica o formato do timestamp
                    self.assertTrue(re.match(timestamp_regex, record['insert_date']))
                    self.assertTrue(re.match(timestamp_regex, record['update_date']))
                    
                    # 4. Verifica se a chave de hash foi gerada (deve ser um inteiro)
                    self.assertIsInstance(record['primary_key_bronze'], int)

            # Executa a verificação no final do pipeline de teste
            assert_that(output, check_results)

# Ponto de entrada para o unittest se for executado diretamente
if __name__ == '__main__':
    unittest.main()
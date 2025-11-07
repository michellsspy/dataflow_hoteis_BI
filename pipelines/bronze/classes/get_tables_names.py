# classes/get_tables_names.py

from google.cloud import storage
import apache_beam as beam
import logging

# Importa as constantes do helper para ter acesso aos nomes do bucket
from functions.bq_gcs_helpers import BUCKET_ORIGEM, PREFIXO_RAIZ_ORIGEM

logger = logging.getLogger(__name__)

def listar_pastas_tabelas_gcs(bucket_name: str, prefixo_raiz: str) -> list[str]:
    """Lista todos os objetos no prefixo e extrai o nome do subdiretório (tabela)."""
    
    prefixo_completo = f"{prefixo_raiz}/"
    logger.info(f"Listando objetos no GCS. Bucket: {bucket_name}, Prefixo: {prefixo_completo}")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Lista TODOS os objetos dentro do prefixo (sem delimitador)
    blobs = storage_client.list_blobs(
        bucket, 
        prefix=prefixo_completo
    )

    nomes_das_tabelas = set() 
    
    for blob in blobs:
        caminho_sem_raiz = blob.name.replace(prefixo_completo, '', 1)
        primeira_barra = caminho_sem_raiz.find('/')
        
        if primeira_barra > 0:
            nome_tabela = caminho_sem_raiz[:primeira_barra]
            nomes_das_tabelas.add(nome_tabela)
            
    tabelas = list(nomes_das_tabelas)
    logger.info(f"Diretórios encontrados: {tabelas}")
    return tabelas


class GetTablesName(beam.DoFn):
    def process(self, element, *args, **kwargs):
        
        tabelas = listar_pastas_tabelas_gcs(BUCKET_ORIGEM, PREFIXO_RAIZ_ORIGEM)
        
        # Emite cada nome de tabela (ex: 'source_consumos')
        for table_name in tabelas:
            yield table_name
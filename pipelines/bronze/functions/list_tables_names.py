from google.cloud import storage
import logging
import sys
import os

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - [%(name)s] - (%(filename)s:%(lineno)d) - %(funcName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Função de Listagem do GCS ---
def listar_pastas_tabelas_gcs(bucket_name: str, prefixo_raiz: str) -> list[str]:
    """
    Lista todos os objetos (blobs) no prefixo e extrai o nome do subdiretório (tabela).
    
    Esta versão é mais robusta contra problemas de 'delimiter'.
    """
    logger.info(f"Listando TODOS os objetos no GCS. Bucket: {bucket_name}, Prefixo: {prefixo_raiz}")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # 1. Listar TODOS os blobs que começam com o prefixo
    #    NÃO usamos o delimiter='/'
    blobs = storage_client.list_blobs(
        bucket, 
        prefix=prefixo_raiz
    )

    nomes_das_tabelas = set() # Usamos um SET para evitar nomes duplicados
    
    for blob in blobs:
        # Ex: blob.name será 'transient/source_consumos/source_consumos.csv'
        
        # 2. Ignora o prefixo raiz ('transient/')
        caminho_sem_raiz = blob.name.replace(prefixo_raiz, '', 1)
        
        # 3. Encontra a primeira barra ('/') no caminho (que separa a pasta do arquivo)
        primeira_barra = caminho_sem_raiz.find('/')
        
        if primeira_barra > 0:
            # 4. Extrai o nome do diretório (Ex: 'source_consumos')
            nome_tabela = caminho_sem_raiz[:primeira_barra]
            nomes_das_tabelas.add(nome_tabela)
            
    # Converte o set de volta para uma lista
    tabelas = list(nomes_das_tabelas)
    
    logger.info(f"Diretórios encontrados através da listagem total: {tabelas}")
    return tabelas
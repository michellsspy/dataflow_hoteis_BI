# src/bronze/classes/schemas.py

from apache_beam.io.gcp.bigquery import TableSchema, TableFieldSchema

# OBS: Você precisará definir o campo de negócio que será a chave primária
# para cada tabela. Por enquanto, usaremos um PLACEHOLDER chamado 'id_registro'.

# --------------------------------------------------------------------------
# Definição das colunas de Auditoria (comuns a todas as tabelas Bronze)
# --------------------------------------------------------------------------
AUDIT_FIELDS = [
    TableFieldSchema(name='insert_date', type='TIMESTAMP', description='Data da primeira inserção do registro.'),
    TableFieldSchema(name='update_date', type='TIMESTAMP', description='Data da última atualização do registro.'),
    # Campo crucial para o MERGE: A Chave que será usada para o Upsert
    TableFieldSchema(name='primary_key_business', type='STRING', mode='REQUIRED', description='Chave primária de negócio para o upsert. EX: id_reserva, id_quarto.'),
]

# --------------------------------------------------------------------------
# Mapeamento do Schema por Tabela (Adaptar com as colunas reais do CSV)
# --------------------------------------------------------------------------
SCHEMA_MAP = {
    # IMPORTANTE: Estes campos são placeholders. Você deve SUBSTITUIR 
    # 'id_registro' pelo campo que é a chave primária real do seu CSV!
    
    'bronze_consumos': TableSchema(fields=[
        TableFieldSchema(name='id_consumo', type='STRING', mode='REQUIRED'),
        TableFieldSchema(name='valor', type='FLOAT'),
        TableFieldSchema(name='data_consumo', type='DATE'),
        *AUDIT_FIELDS # Adiciona as colunas de auditoria
    ]),
    
    'bronze_faturas': TableSchema(fields=[
        TableFieldSchema(name='id_fatura', type='STRING', mode='REQUIRED'),
        TableFieldSchema(name='data_vencimento', type='DATE'),
        TableFieldSchema(name='status', type='STRING'),
        *AUDIT_FIELDS
    ]),
    
    'bronze_hospedes': TableSchema(fields=[
        TableFieldSchema(name='id_hospede', type='STRING', mode='REQUIRED'),
        TableFieldSchema(name='nome', type='STRING'),
        TableFieldSchema(name='cpf', type='STRING'),
        *AUDIT_FIELDS
    ]),
    
    # Adicione os demais schemas (hoteis, quartos, reservas, reservas_ota)
    # Exemplo:
    'bronze_hoteis': TableSchema(fields=[
        TableFieldSchema(name='id_hotel', type='STRING', mode='REQUIRED'),
        TableFieldSchema(name='nome_hotel', type='STRING'),
        *AUDIT_FIELDS
    ]),
    
    'bronze_quartos': TableSchema(fields=[
        TableFieldSchema(name='id_quarto', type='STRING', mode='REQUIRED'),
        TableFieldSchema(name='tipo_quarto', type='STRING'),
        *AUDIT_FIELDS
    ]),
    
    'bronze_reservas': TableSchema(fields=[
        TableFieldSchema(name='id_reserva', type='STRING', mode='REQUIRED'),
        TableFieldSchema(name='data_checkin', type='DATE'),
        TableFieldSchema(name='data_checkout', type='DATE'),
        *AUDIT_FIELDS
    ]),
    
    'bronze_reservas_ota': TableSchema(fields=[
        TableFieldSchema(name='id_reserva_ota', type='STRING', mode='REQUIRED'),
        TableFieldSchema(name='nome_ota', type='STRING'),
        *AUDIT_FIELDS
    ]),
}

def get_bq_schema(table_name: str) -> TableSchema:
    """
    Retorna o schema do BigQuery para o nome da tabela fornecido.
    
    Args:
        table_name (str): O nome da tabela (ex: 'bronze_consumos').
        
    Returns:
        TableSchema: O objeto TableSchema do BigQuery.
        
    Raises:
        ValueError: Se o nome da tabela não for encontrado no mapeamento.
    """
    if table_name not in SCHEMA_MAP:
        raise ValueError(f"Schema não encontrado para a tabela: {table_name}")
    return SCHEMA_MAP[table_name]
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import secretmanager
from google.cloud import storage
from datetime import datetime, timedelta
import apache_beam as beam
import tempfile
import logging
import sys
import os

# Importando as Funções
from functions.get_secret import get_secret, save_secret_to_temp_file
from options.options import func_options
from classes.upsert_bronze import ParquetToBQ

# Recupera a chave e salva temporariamente
key_data = get_secret()
key_path = save_secret_to_temp_file(key_data)

# Configura as credenciais do GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Recebe a data atual
data_now = datetime.now().strftime("%Y%m%d%H%M%S")

# Lista das tabelas para carga
tables_names = [
    "hoteis",   
    "consumos",
    "faturas",
    "hospedes",
    "quartos",
    "reservas",
    "reservas_ota"
]

def main_transient_to_raw(data_now):

    options = func_options(data_now)
    
    with beam.Pipeline(options=options) as p:
            tables = (
                p
                | "Create Table List" >> beam.Create(tables_names)
                | 'Process Tables' >> beam.ParDo(ParquetToBQ())
                | 'Print Results' >> beam.Map(print)
            )

if __name__ == "__main__":
      main_transient_to_raw(data_now)

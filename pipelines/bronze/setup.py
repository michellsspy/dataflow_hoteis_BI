import setuptools
import sys

# Garante que o setuptools seja usado para empacotamento
setuptools.setup(
    name='hotelaria-etl',
    version='0.0.1',
    install_requires=[
        # Se você usa um requirements.txt, este campo é opcional,
        # mas inclua dependências críticas aqui, como google-cloud-bigquery
        'google-cloud-bigquery',
        'apache-beam[gcp]', 
        'pandas',
        'pyarrow',
        # ...
    ],
    packages=setuptools.find_packages(where='.'),
    # Define o diretório raiz para a busca de pacotes
    package_dir={'': '.'},
)
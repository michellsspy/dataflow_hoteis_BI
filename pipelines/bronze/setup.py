from setuptools import setup, find_packages

setup(
    name="bronze_pipeline",
    version="0.1",
    packages=find_packages(),  # procura pacotes no diretório atual (bronze/)
    include_package_data=True,
    install_requires=[
        "apache-beam[gcp]",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "pandas",
        "pyarrow"
    ],
)

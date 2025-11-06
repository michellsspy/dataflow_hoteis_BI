from setuptools import setup, find_packages

setup(
    name="dataflow_hoteis_bronze",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "apache-beam[gcp]==2.56.0",
        "google-cloud-bigquery",
        "google-cloud-storage"
    ],
)

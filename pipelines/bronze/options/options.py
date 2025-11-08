from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timedelta
import sys

# Criando Def Options para reaproveitamento do código
def func_options(data_now):
    
    # Variáveis do projeto
    project_id = "etl-hoteis"
    argv = sys.argv

    return PipelineOptions(
        flags=argv,
        project=project_id,
        runner = "DataflowRunner", # Executa no dataflow
        #runner = "DirectRunner", # Exectua localmente
        streaming = False,
        job_name = f"etl-hotelaria-{data_now}",
        temp_location = "gs://bk-etl-hotelaria/temp",
        staging_location = "gs://bk-etl-hotelaria/staging",
        #template_location = f"gs://bk-etl-hotelaria/templates/template-hotelaria-{data_now}",
        autoscaling_algorithm = "THROUGHPUT_BASED",
        worker_machine_type = "n1-standard-4",
        num_workers = 1,
        max_num_workers = 3,
        disk_size_gb = 25,
        region = "us-central1",
        #zone = "us-central1-c",
        #worker_zone = "us-central1-a",
        project_id = "etl-hoteis",
        staging_bucket = "bk-etl-hotelaria",
        save_main_session = False,
        #experiments = "use_runner_v2",
        prebuild_sdk_container_engine = "cloud_build",
        docker_registry_push_url = "us-central1-docker.pkg.dev/etl-hoteis/etl-hotelaria/hotelaria-dev",
        sdk_container_image = "us-central1-docker.pkg.dev/etl-hoteis/etl-hotelaria/hotelaria-dev:latest",
        sdk_location = "container",
        requirements_file = "./requirements.txt",
        metabase_file = "./metadata.json",
        setup_file = "./setup.py",
        service_account_email = "etl-hoteis@etl-hoteis.iam.gserviceaccount.com"
    )
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime, timedelta

# Criando Def Options para reaproveitamento do código
def func_options(data_now):
    
    # Variáveis do projeto
    project_id = "my-project-ga4-441222"
    dataset_id = "analytics_466537818"
    argv = None

    return PipelineOptions(
        flags=argv,
        project=project_id,
        runner="DataflowRunner",
        streaming=False,
        job_name=f"etl-analytics-ga4-{data_now}",
        staging_location="gs://bk-analytics-ga4/staging_location",
        temp_location="gs://bk-analytics-ga4/temp_location",
        autoscaling_algorithm="THROUGHPUT_BASED",
        worker_machine_type="n1-standard-4",
        region="us-central1",  
        zone="us-central1-c",  
        num_workers=1,  
        max_num_workers=5, 
        number_of_worker_harness_threads=2,  
        disk_size_gb=100,  
        save_main_session=True,
        requirements_file="./requirements.txt",
        setup_file="./setup.py",
        service_account_email="my-project-ga4-441222@appspot.gserviceaccount.com"
    )
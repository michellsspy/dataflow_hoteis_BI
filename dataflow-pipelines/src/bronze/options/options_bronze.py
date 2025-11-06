from datetime import datetime
data_now = datetime.now()
data_now = str(data_now).replace(' ', '_').replace(':', '.')

def options_bronze(data_now):
    {
    "dataset": "raw_hotelaria",
    "gcs_input_path": "gs://bk-etl-hotelaria/transient/source_*/source_*.csv",
    "project": "etl-hoteis",
    "staging_location": "gs://bk-etl-hotelaria/staging",
    "temp_location": "gs://bk-etl-hotelaria/temp",
    "runner": "DataflowRunner",
    "streaming": False,
    "job_name": f"etl-transient-to-raw-{data_now}",
    "template_location": f"gs://bk-etl-hotelaria/templates/template-etl-hotel-{data_now}",
    "autoscaling_algorithm": "THROUGHPUT_BASED",
    "worker_machine_type": "n1-standard-4",
    "num_workers": 1,
    "max_num_workers": 3,
    "disk_size_gb": 25,
    "region": "us-central1",
    "save_main_session": False,
    "prebuild_sdk_container_engine": "cloud_build",
    "docker_registry_push_url": "us-central1-docker.pkg.dev/etl-hotel/etl-transient-to-raw/etl-transient-dev",
    "sdk_container_image": "us-central1-docker.pkg.dev/etl-hotel/etl-transient-to-raw/etl-transient-dev:latest",
    "sdk_location": "container",
    "requirements_file": "./requirements.txt",
    "setup_file": "./setup.py",
    "metabase_file": "./metadata_raw.json",
    "service_account_email": "etl-hoteis@etl-hoteis.iam.gserviceaccount.com"
}
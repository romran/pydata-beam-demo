# pydata-beam-demo

#### Setup gcloud cli
https://cloud.google.com/sdk/docs/install#deb

## How to run ETL pipeline?

#### Authorize gcp account
- `gcloud auth login`
- `gcloud config set project project-demo`
- `gcloud auth application-default login`
- `gcloud auth application-default set-quota-project project-demo`

#### Run ETL locally
- `cd single_upload`
- `python upload_data_main.py --n_loops 3 --table_name big_table --n_rows 500`

#### Stage ETL pipeline template
- `python upload_data_main.py --n_loops 2 --table_name big_table  --n_rows 1000 --runner DataflowRunner --project pydata-demo --staging_location gs://staging_location/dataflow/staging --temp_location gs://temp_location/dataflow/temp --region europe-central2 --setup_file ./setup.py --template_location gs://template_location/dataflow/templates/upload_data_main --max_num_workers 32 --machine_type n1-highcpu-8`

#### Run ETL pipeline
- `python upload_data_main.py --n_loops 10 --table_name big_table --n_rows 500000 --runner DataflowRunner --project pydata-demo --staging_location gs://staging_location/dataflow/staging --temp_location gs://temp_location/dataflow/temp --region europe-central2 --setup_file ./setup.py`
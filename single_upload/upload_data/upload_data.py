import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import bigquery
import numpy as np
import pandas as pd

class GetCount(beam.DoFn):
    def __init__(self, table_name):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.table_name = table_name

    def process(self, response):
        client = bigquery.Client()
        sql = f"""
        SELECT COUNT(*) as total_rows
        FROM `{self.project_id}.{self.dataset_id}.{self.table_name}`
        """
        df = client.query(sql).to_dataframe()

        total_rows = df['total_rows']
        logging.info(f'Table {total_rows}')
        yield None
    
class GetData(beam.DoFn):
    def __init__(self, table_name, n_rows):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.table_name = table_name
        self.n_rows = int(str(n_rows))

    def process(self, response):
        np.random.seed(0)
        data = np.random.randn(self.n_rows, 5)
        columns = ['A', 'B', 'C', 'D', 'E']
        df = pd.DataFrame(data, columns=columns)

        client = bigquery.Client()
        schema = []
        for column in columns:
            schema.append(bigquery.SchemaField(column, "FLOAT"))
        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")
        table_location = f'{self.project_id}.{self.dataset_id}.{self.table_name}'

        job = client.load_table_from_dataframe(df, table_location, job_config=job_config)
        job.result()
        logging.info(f'Upload table: {df.shape}')

        yield True

class RunTimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--n_loops', required=True, help='Number of loops')
        parser.add_value_provider_argument('--table_name', required=True, help='Select table name')
        parser.add_value_provider_argument('--n_rows', required=True, help='Select number of rows')

def run(argv=None, save_main_session=False):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    user_options = pipeline_options.view_as(RunTimeOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options = PipelineOptions(temp_location = 'gs://temp_location/dataflow/temp')
    

    with beam.Pipeline(argv=pipeline_args) as p:
        iter_list = list(range(0, int(str(user_options.n_loops))))
        pcollections = []
        for iter in iter_list:
            pcollection = p | f'Iteration {iter} ' >> beam.Create([None])
            pcollection = (
                pcollection
                | f'Create df for iteration {iter}' >> beam.ParDo(GetData(user_options.table_name, user_options.n_rows))) 
                        
            pcollections.append(pcollection)
        final_collection = (
            pcollections | 'Merge PCollections' >> beam.Flatten()
            | 'Get distinct states' >> beam.Distinct()
            | 'Get table count' >> beam.ParDo(GetCount(user_options.table_name))
        )
from airflow.hooks.base import BaseHook
import json
import os
from google.cloud import bigquery

from airflow.exceptions import AirflowException

class CustomBigQueryHook(BaseHook):
    
    def __init__(self,bq_conn):
        
        self.conn = self.get_connection(bq_conn)
        extras = self.conn.get_extra()

        extras = json.loads(extras)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= f"/home/rahul/airflow/data/{extras['BigQuery_credential']}"
        
        print(extras)
        
        
    def check_bq_table_exists(self, dataset_id, table_name):

        table_ref = self.client.dataset(dataset_id).table(table_name)
        try:
            self.client.get_table(table_ref)
            return True
        except:
            
            raise AirflowException('table {} does not exist'.format(table_name))

    
    def dump_into_bigquery(self,dataset,stats,content_details,stats_id,content_details_id):
        
        
        client = bigquery.Client()
        try:
        
            dataset = client.create_dataset(dataset)
        
        except:
            
            print('{} already exists'.format(dataset))
        
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
                
                bigquery.SchemaField("Views", bigquery.enums.SqlTypeNames.INTEGER),
        
                bigquery.SchemaField("Likes", bigquery.enums.SqlTypeNames.INTEGER),
        
                bigquery.SchemaField("Dislikes", bigquery.enums.SqlTypeNames.INTEGER),
        
                bigquery.SchemaField("Favorites", bigquery.enums.SqlTypeNames.INTEGER),
        
                bigquery.SchemaField("Number_of_Comments", bigquery.enums.SqlTypeNames.INTEGER),
            ],
        )



        client.load_table_from_json(stats,f"{dataset}.{stats_id}",job_config=job_config)



        job_config2 = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("videoId", bigquery.enums.SqlTypeNames.STRING),
                                                     
                bigquery.SchemaField("Date", bigquery.enums.SqlTypeNames.DATETIME)
            ])


        client.load_table_from_json(content_details,f"{dataset}.{content_details_id}",job_config=job_config2)
        
        print("Data has been dumped into bigquery")
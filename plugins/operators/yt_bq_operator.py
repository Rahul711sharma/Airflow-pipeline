from hooks.yt_hook import CustomYoutubeHook
from hooks.bq_hook import CustomBigQueryHook

from airflow.models.baseoperator import BaseOperator

class YtToBQOperator(BaseOperator):
    
    def __init__(self,playlist_id,bigquery_dataset,bq_conn,yt_conn,**kwargs) -> None:
            super().__init__(**kwargs)
            
            self.playlist_id= playlist_id
            self.bq_conn = bq_conn
            self.yt_conn = yt_conn
            self.bigquery_dataset = bigquery_dataset
    
    def execute(self, context):
        
        yt_hook = CustomYoutubeHook(yt_conn=self.yt_conn)
        
        Stats,content_details = yt_hook.YT_to_local(playlist_id=self.playlist_id)
        
        stats_path ="/home/rahul/airflow/test/stats.json"
        content_path = "/home/rahul/airflow/test/content_details.json"
            
        content_detail_file = open(content_path,"wt")
        content_detail_file.write(str(content_details))
        content_detail_file.close()

        stat = open(stats_path,"wt")
        stat.write(str(Stats))
        stat.close()
        
        
        bq_hook = CustomBigQueryHook(bq_conn=self.yt_conn)
        
        
        bq_hook.dump_into_bigquery(dataset=self.bigquery_dataset,stats=Stats,content_details=content_details,stats_id="stats_id",content_details_id="content_details_id")
    
    
    
        
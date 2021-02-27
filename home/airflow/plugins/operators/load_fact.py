import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    #fact_table_template = """
    #CREATE TABLE IF NOT EXISTS {table} AS {sql}
    #"""
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))
        self.log.info('Table deleted')
        
        #fact_sql=LoadFactOperator.fact_table_template.format(
        #    table = self.table,
        #    sql = self.sql
        #)
        #redshift.run(fact_sql)
        redshift.run(self.sql)
        self.log.info('LoadFactOperator completed')
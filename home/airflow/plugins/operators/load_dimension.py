from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql='',
                 table='',
                 append_data='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_data=append_data

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
          sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
          redshift.run(sql_statement)
        else:
          sql_statement = 'DELETE FROM %s' % self.table
          redshift.run(sql_statement)
          self.log.info('Table deleted')
        
          sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
          redshift.run(sql_statement)
            
        self.log.info('LoadDimensionOperator completed')
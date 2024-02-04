from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)   
        self.log.info("Inserting data into {} table".format(self.table))
        try:
            self.log.info("Inserting data into {} table".format(self.table))
            redshift.run(self.sql)
            self.log.info("Successfully completed insert into {} table".format(self.table))
            
        except Exception as e:
            self.log.error("An error occurred while loading data into {} table: {}".format(self.table, str(e)))
            raise